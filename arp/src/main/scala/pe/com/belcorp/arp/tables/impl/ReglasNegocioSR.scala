package pe.com.belcorp.arp.tables.impl

import org.apache.spark.sql.expressions.Window
import org.apache.spark.sql.functions.{lit, _}
import org.apache.spark.sql.{DataFrame, SparkSession}
import pe.com.belcorp.arp.tables.Constants
import pe.com.belcorp.arp.tables.impl.blocks.ReglasNegocio
import pe.com.belcorp.arp.utils.Extensions._
import pe.com.belcorp.arp.utils.ProcessParams

class ReglasNegocioSR(params: ProcessParams, arpParametrosEst: DataFrame, arpEspaciosForzadosEst: DataFrame, dwhDproducto: DataFrame, dwhDmatrizcampana: DataFrame, arpListadoProbabilidades: Option[DataFrame], tipoPersonalizacion: String)
  extends ReglasNegocio(params, arpParametrosEst, arpEspaciosForzadosEst, dwhDproducto, dwhDmatrizcampana, tipoPersonalizacion) {

  def get(spark: SparkSession): DataFrame = {
    arpTotalProductosEstimados
  }

  //Se eligen productos sin regalo para los cálculos
  def productosTotales: DataFrame = {
    arpParametrosEst
      .where($"ANIOCAMPANAPROCESO" === params.anioCampanaProceso() &&
        $"ANIOCAMPANAEXPO" === params.anioCampanaExpo() &&
        $"CODPAIS" === params.codPais() &&
        $"TIPOARP" === params.tipoARP() &&
        $"PERFIL" === params.perfil() &&
        $"TIPOPERSONALIZACION" === tipoPersonalizacion)
      .select("TipoTactica", "CodTactica", "CodCUC", "Unidades", "PrecioOferta", "CodVenta", "IndicadorPadre", "FlagTop")
      .distinct()
  }

  //#ListadoConsultora_Total2
  def listadoConsultoraTotalSR: DataFrame = {
    arpListadoProbabilidades
      .get
      .where($"Probabilidad" =!= 0 &&
        $"TipoPersonalizacion" === tipoPersonalizacion &&
        $"CodPais" === params.codPais() &&
        $"AnioCampanaProceso" === params.anioCampanaProceso() &&
        $"AnioCampanaExpo" === params.anioCampanaExpo() &&
        $"TipoARP" === params.tipoARP() &&
        $"FlagMC" === params.flagMC() &&
        $"Perfil" === params.perfil())
      .select("CodEbelista", "CodTactica", "FlagTop", "Probabilidad")
      .distinct()
  }

  if (params.tipoARP() == Constants.ARP_ESTABLECIDAS) {

    //#Temporal_Ranking
    def temporalRanking: DataFrame = {
      listadoConsultoraTotalSR
        .select($"CodEbelista", $"CodTactica", $"FlagTop", $"Probabilidad", row_number().over(Window
          .partitionBy($"CodEbelista")
          .orderBy(desc("Probabilidad"))).alias("Posicion"))
        .orderBy($"CodEbelista", desc("Probabilidad"))
        .distinct()
    }

    //#ListadoOF
    //Se separan las tácticas de Oferta Final (Se toma el 3er producto)
    def listadoOF: DataFrame = {
      temporalRanking
        .where($"Posicion" === 3)
        .select($"CodEbelista", $"CodTactica", $"Probabilidad", $"Posicion".alias("Orden"), lit(1).alias("OrdenInterfaz"))
    }

    //TODO - Rever o delete com Daniel
    def deleteListadoConsultoraTotal(): DataFrame = {
      listadoConsultoraTotalSR.alias("lctSR")
      temporalRanking.alias("tr")

      val conditionsDrop = listadoConsultoraTotalSR
        .join(temporalRanking, $"lctSR.CodTactica" === $"tr.CodTactica" && $"lctSR.CodEbelista" === $"tr.CodEbelista", "Inner")
        .filter($"Posicion" === 3)

      listadoConsultoraTotalSR
        .join(temporalRanking, $"lctSR.CodTactica" === $"tr.CodTactica" && $"lctSR.CodEbelista" === $"tr.CodEbelista", "Inner")
        .except(conditionsDrop)
    }

    //Se separan las tácticas de Oferta Final (Se copia el 1ero y 2do)
    def insertTemporalRanking1(): DataFrame = {
      val insert = temporalRanking
        .where($"Posicion" === 1)
        .select($"CodEbelista", $"CodTactica", $"Probabilidad", $"Posicion".alias("Orden"), lit(2).alias("OrdenInterfaz"))

      //Insert
      listadoOF.union(insert)
    }

    //Se separan las tácticas de Oferta Final (Se copia el 1ero y 2do)
    def insertTemporalRanking2(): DataFrame = {
      val insert = insertTemporalRanking1()
        .where($"Posicion" === 2)
        .select($"CodEbelista", $"CodTactica", $"Probabilidad", $"Posicion".alias("Orden"), lit(3).alias("OrdenInterfaz"))

      //Insert
      listadoOF.union(insert)
    }
  }

  //Tipo: N (No Forzado) F(Forzado) T(Top)
  /** 1.1. TOPS **/

  //#Temporal_Tops
  //Se Ordenan las tácticas según la probabilidad:
  def temporalTops: DataFrame = {
    listadoConsultoraTotalSR
      .where($"Probabilidad" =!= 0 && $"FlagTop" === 1)
      .select($"CodEbelista", $"CodTactica", $"Probabilidad", row_number().over(Window
        .partitionBy($"CodEbelista")
        .orderBy(desc("Probabilidad"))).alias("Posicion"))
      .orderBy($"CodEbelista", desc("Probabilidad"))
      .distinct()
  }

  //#ListaRecomendados
  //Tipo T: TOPS
  def listaRecomendados: DataFrame = {
    temporalTops
      .where($"Posicion" <= numEspaciosTop)
      .select($"CodEbelista", $"CodTactica", $"Probabilidad", lit("T").alias("Tipo"),
        lit(0).alias("Prioridad"), lit(0).alias("Orden"))
      .distinct()
  }

  //#Temporal_0
  //Se guardan las tácticas que no son Top
  def temporal0: DataFrame = {
    listadoConsultoraTotalSR
      .where($"Probabilidad" =!= 0 && $"FlagTop" === 0)
      .select($"CodEbelista", $"CodTactica", $"Probabilidad")
  }

  //#ListadoPropuestos
  //Tipo F y N: Forzados y No Forzados
  def listadoPropuestos: DataFrame = {
    temporal0.alias("tmp0")
      .join(productosCUC.alias("pcuc"), $"tmp0.CodTactica" === $"pcuc.CodTactica", "Inner")
      .select($"tmp0.CodEbelista", $"tmp0.CodTactica", $"pcuc.CodProduto", $"pcuc.DesMarca", $"pcuc.DESCategoria", $"Probabilidad",
        lit(0).alias("FlagQueda"), lit(0).alias("Ranking"), lit("N").alias("Tipo"))
  }

  /** 1.2. Espacios Forzados **/

  //Se obtienen los productos y tácticas de los espacios forzados

  var i = 1

  while (i <= numEspaciosFijos) {

    //#Forzado_Top
    //Se busca el Top de productos disponibles y se guarda en una tabla temporal
    def forzadoTop: DataFrame = {
      listadoPropuestos.alias("lp")
      espaciosFijos.alias("ef")

      listadoPropuestos
        .join(espaciosFijos, $"lp.DesMarca" === $"ef.Marca" && $"lp.DESCategoria" === $"ef.Categoria" && $"ef.VinculoEspacio" === i, "Inner")
        .where($"lp.FlagQueda" === 0)
        .select($"CodEbelista", $"CodTactica", $"CodProducto", $"Probabilidad", row_number().over(Window
          .partitionBy($"CodEbelista")
          .orderBy(desc("Probabilidad"))).alias("Posicion"))
        .orderBy($"CodEbelista", desc("Probabilidad"))
    }

    //#TOP1
    //Extraigo el registro a evaluar (El de mayor probabilidad)
    def top1: DataFrame = {
      forzadoTop
        .where($"Posicion" === 1)
        .select($"CodEbelista", $"CodTactica", $"CodProducto")
    }

    //Se actualiza la tabla inicial ListadoPropuestos
    //TODO - Rever update com Daniel
    def updateListadoPropuestos(): DataFrame = {
      listadoPropuestos.alias("lp")
        .join(top1.alias("top1"), $"lp.CodEbelista" === $"top1.CodEbelista" && $"lp.CodTactica" === $"top1.CodTactica", "Inner")
        .select($"lp.*")
        .withColumn("Ranking", when($"lp.FlagQueda" === 0, i))
        .withColumn("FlagQueda", when($"lp.FlagQueda" === 0, lit(1)))
        .withColumn("Tipo", when($"lp.FlagQueda" === 0, lit("F")))
    }

    i += 1
    //TODO -> conferir com o Daniel esse dropTable
    //DROP TABLE
  }

  //TODO -> conferir com o Daniel esse dropTable
  //Elimino los productos que son de la misma Marca y Categoría de los Espacios No Forzados
  def deleteMarcaCategoriaNoRepetir(): DataFrame = {
    listadoPropuestos.alias("lp")
    marcaCategoriaNoRepetir.alias("mcnr")

    val conditionsDrop = listadoPropuestos
      .join(marcaCategoriaNoRepetir, $"lp.DesMarca" === $"mcnr.Marca" && $"lp.DESCategoria" === $"mcnr.Categoria", "Inner")
      .filter($"FlagQueda" === 0)

    listadoPropuestos
      .join(marcaCategoriaNoRepetir, $"lp.DesMarca" === $"mcnr.Marca" && $"lp.DESCategoria" === $"mcnr.Categoria", "Inner")
      .except(conditionsDrop)
  }

  /** 1.3. Espacios No Forzados **/

  var j = 1

  while (j <= numEspaciosLibres) {

    //#Producto_Top1
    //Se busca el top de productos disponibles y se guarda en una tabla temporal
    def productoTop1: DataFrame = {
      listadoPropuestos
        .where($"FlagQueda" === 0)
        .select($"CodEbelista", $"CodTactica", $"CodProducto", $"Probabilidad", row_number().over(Window
          .partitionBy($"CodEbelista")
          .orderBy(desc("Probabilidad"))).alias("Posicion"))
        .orderBy($"CodEbelista", desc("Probabilidad"))
    }

    //#TOP1_1
    //Extraigo el registro a evaluar (El Top)
    def top1_1: DataFrame = {
      productoTop1
        .where($"Posicion" === 1)
        .select($"CodEbelista", $"CodTactica", $"CodProducto")
    }

    //Se actualiza la tabla inicial ListadoPropuestos
    def updateListadoPropuestos2(): DataFrame = {
      listadoPropuestos.alias("lp")
        .join(top1_1.alias("top1_1"), $"lp.CodEbelista" === $"top1_1.CodEbelista" && $"lp.CodTactica" === $"top1_1.CodTactica" && $"lP.CodProducto" === $"top1_1.CodProducto", "Inner")
        .select($"lp.*")
        .withColumn("Ranking", when($"lp.FlagQueda" === 0, i))
        .withColumn("FlagQueda", when($"lp.FlagQueda" === 0, 1))
    }

    //Se guarda las consultoras y las tácticas donde aparece el producto evaluado
    def aEliminar: DataFrame = {
      listadoPropuestos.alias("lp")
        .join(top1_1.alias("top1_1"), $"lp.CodEbelista" === $"top1_1.CodEbelista" && $"lp.CodProducto" === $"top1_1.CodProducto" && $"lp.FlagQueda" === 0, "Inner")
        .select($"lp.CodEbelista", $"lp.CodTactica")
        .distinct()
    }

    //Se elimina todas las tácticas que contengan el producto evaluado
    def deleteTacticasListadoPropuestos(): DataFrame = {
      listadoPropuestos.alias("lP")
      aEliminar.alias("aEliminar")

      val conditionsDrop = listadoPropuestos
        .join(aEliminar, $"lP.CodEbelista" === $"aEliminar.CodEbelista" && $"lP.CodTactica" === $"aEliminar.CodTactica" && $"lP.FlagQueda" === 0, "Inner")
        .filter($"FlagQueda" === 0)
      listadoPropuestos
        .join(aEliminar, $"lP.CodEbelista" === $"aEliminar.CodEbelista" && $"lP.CodTactica" === $"aEliminar.CodTactica" && $"lP.FlagQueda" === 0, "Inner")
        .filter($"FlagQueda" === 0)
        .except(conditionsDrop)
    }

    j += 1
    //TODO -> conferir com o Daniel esse dropTable
    //DROP TABLE
  }

  /* 1.4. Espacios que sobran */

  var k = 1

  while (k <= numEspaciosFijos) {

    def consultorASCEI: DataFrame = {
      listadoPropuestos
        .where($"FlagQueda" === 1)
        .groupBy("CodEbelista")
        .agg(countDistinct($"CodTactica").alias("NumEspacios"))
        .select("CodEbelista")
      //HAVING
    }

    //Se busca el top de productos disponibles y se guarda en una tabla temporal
    def productoTop1ES: DataFrame = {
      listadoPropuestos.alias("lp")
        .join(consultorASCEI.alias("cASCEII"), $"lp.CodEbelista" === $"cASCEII.CodEbelista", "Inner")
        .where($"FlagQueda" === 0)
        .select($"lp.CodEbelista", $"lp.CodTactica", $"lp.CodProducto", $"lp.Prababilidad", row_number().over(Window
          .partitionBy($"lp.CodEbelista")
          .orderBy(desc("Probabilidad"))).alias("Posicion"))
        .orderBy($"lp.CodEbelista", desc("Probabilidad"))
    }

    //Extraigo el Registro a evaluar (el Top)
    def top1_1ES: DataFrame = {
      productoTop1ES
        .where($"Posicion" === 1)
        .select($"CodEbelista", $"CodTactica", $"CodProducto")
    }

    //Se actualiza la tabla inicial ListadoPropuestos
    def updateListadoPropuestos3(): DataFrame = {
      listadoPropuestos.alias("lp")
        .join(top1_1ES.alias("top1_1ES"),
          $"lp.CodEbelista" === $"top1_1ES.CodEbelista" &&
            $"lp.CodTactica" === $"top1_1ES.CodTactica" &&
            $"lp.CodProducto" === $"top1_1ES.CodProducto", "Inner")
        .select($"lp.*")
        .withColumn("Ranking", when($"lp.FlagQueda" === 0, i))
        .withColumn("FlagQueda", when($"lp.FlagQueda" === 0, 1))
    }

    //Se guarda las consultoras y las tácticas donde aparece el producto evaluado
    def aEliminarES: DataFrame = {
      listadoPropuestos.alias("lp")
        .join(top1_1ES.alias("top1_1ES"), $"lp.CodEbelista" === $"top1_1ES.CodEbelista" && $"lp.CodProducto" === $"top1_1ES.CodProducto" && $"lp.FlagQueda" === 0)
        .select($"lp.CodEbelista", $"lp.CodTactica")
        .distinct()
    }

    //Se elimina todas las tácticas que contengan el producto evaluado
    def deleteEvaluadoListadoPropuestos(): DataFrame = {
      listadoPropuestos.alias("lP")
      aEliminarES.alias("aEES")

      val conditionsDrop = listadoPropuestos
        .join(aEliminarES, $"lP.CodEbelista" === $"aEES.CodEbelista" && $"lP.CodTactica" === $"aEES.CodTactica" && $"lP.FlagQueda" === 0, "Inner")
        .filter($"FlagQueda" === 0)
      listadoPropuestos
        .join(aEliminarES, $"lP.CodEbelista" === $"aEES.CodEbelista" && $"lP.CodTactica" === $"aEES.CodTactica" && $"lP.FlagQueda" === 0, "Inner")
        .filter($"FlagQueda" === 0)
        .except(conditionsDrop)
    }

    k += 1
    //TODO -> conferir com o Daniel esse dropTable
    //DROP TABLE
  }

  //Extraigo las tácticas de acuerdo a la cantidad de espacios
  def insertListaRecomendados(): DataFrame = {
    val insert = listadoPropuestos
      .where($"FlagQueda" === 1)
      .select($"CodEbelista", $"CodTactica", $"Probabilidad", $"Tipo")
      .distinct()

    listaRecomendados.union(insert)
  }

  //Para o Insert, utilizado o dataframe acima (insertListaRecomendados)
  //para o Select e a função .union para juntar com o dataframe original
  //(listaRecomendados)


  //Se actualiza la prioridad de los tipos para hacer el ordenamiento
  def updatePrioridadListaRecomendados1(): DataFrame = {
    listaRecomendados
      .withColumn("Prioridad", when($"Tipo" === 'T', 1))
  }

  def updatePrioridadListaRecomendados2(): DataFrame = {
    listaRecomendados
      .withColumn("Prioridad", when($"Tipo" === 'N', 2))
  }

  def updatePrioridadListaRecomendados3(): DataFrame = {
    listaRecomendados
      .withColumn("Prioridad", when($"Tipo" === 'F', 3))
  }

  //Se ordena los productos: Prioridad Top, No Forzado, Forzado
  def listadoOPT: DataFrame = {
    listaRecomendados
      .select($"CodEbelista", $"CodTactica", $"Probabilidad", $"Tipo", row_number().over(Window
        .partitionBy($"CodEbelista")
        .orderBy($"Prioridad", desc("Probabilidad"))).alias("Orden"))
  }

  /** Estimación **/
  def totalConsultorasTactica: DataFrame = {
    listadoOPT
      .groupBy($"CodTactica")
      .agg(countDistinct("CodEbelista").alias("TotalConsultoras"))
  }

  def productosCUCTotales: DataFrame = {
    productosTotales.alias("pt")
      .join(dwhDproducto.alias("dp"), $"pt.CodCUC" === $"dp.CUC", "Inner")
      .where($"dp.descripcuc".isNotNull)
      .groupBy($"dp.CUC".alias("CodProducto"), $"TipoTactica", $"CodTactica", $"CodVenta", $"Unidades", $"pt.PrecioOferta", $"IndicadorPadre")
      .agg(max($"dp.descripcuc").alias("DesProducto"),
        max($"dp.DesMarca").alias("DesMarca"),
        max($"dp.DesCategoria").alias("DesCategoria"),
        max($"dp.DesTipoSolo").alias("DesTipoSolo"))
  }

  def arpTotalProductosEstimados: DataFrame = {
    totalConsultorasTactica.alias("tct")
      .join(productosCUCTotales.alias("pcuct"), $"tct.CodTactica" === $"pcuct.CodTactica", "Inner")
      .groupBy($"pcuct.TipoTactica", $"tct.CodTactica", $"pcuct.DesMarca", $"pcuct.DesCategoria", $"pcuct.DesTipoSolo", $"pcuct.CodProducto", $"DesProducto", $"pcuct.Unidades")
      .agg(lit("SR").alias("TipoPersonalizacion"),
        sum($"tct.TotalConsultoras").alias("TotalConsultoras"))
  }

}