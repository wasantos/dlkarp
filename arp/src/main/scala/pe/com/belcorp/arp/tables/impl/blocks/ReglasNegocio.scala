package pe.com.belcorp.arp.tables.impl.blocks

import org.apache.spark.sql.DataFrame
import org.apache.spark.sql.functions._
import pe.com.belcorp.arp.utils.Extensions._
import pe.com.belcorp.arp.utils.ProcessParams

class ReglasNegocio(params: ProcessParams, dfArpParametros: DataFrame,
                    dfArpEspaciosForzados: DataFrame, dfDwhDproducto: DataFrame,
                    dfDwhDmatrizcampana: DataFrame, tipoPersonalizacion: String) {

  /** Estimación **/

  //TODO -> o que deve ser feito quanto a isso ?

  //IF OBJECT_ID('ListadoProductos') is not null
  //DROP TABLE ListadoProductos

  //IF OBJECT_ID('ListadoRegalos') is not null
  //DROP TABLE ListadoRegalos

  //IF OBJECT_ID('EspaciosForzados') is not null
  //DROP TABLE EspaciosForzados

  //IF OBJECT_ID('CampaniaExpoEspacios') is not null
  //DROP TABLE CampaniaExpoEspacios


  //Se eligen productos sin regalo para los cálculos
  def listadoProductos: DataFrame = {
    dfArpParametros.where($"ANIOCAMPANAPROCESO" === params.anioCampanaProceso() &&
      $"ANIOCAMPANAEXPO" === params.anioCampanaExpo() &&
      $"CODPAIS" === params.codPais() &&
      $"TIPOARP" === params.tipoARP() &&
      $"PERFIL" === params.perfil() &&
      $"TIPOPERSONALIZACION" === tipoPersonalizacion &&
      $"PrecioOferta" > 0)
      .select("TipoTactica", "CodTactica", "CodCUC", "Unidades", "PrecioOferta", "CodVenta", "IndicadorPadre", "FlagTop", "LimUnidades", "FlagUltMinuto", "CodVinculoOF")
      .distinct()
  }

  //Se eligen los regalos para los cálculos
  def listadoRegalos: DataFrame = {
    dfArpParametros.where($"ANIOCAMPANAPROCESO" === params.anioCampanaProceso() &&
      $"ANIOCAMPANAEXPO" === params.anioCampanaExpo() &&
      $"CODPAIS" === params.codPais() &&
      $"TIPOARP" === params.tipoARP() &&
      $"PERFIL" === params.perfil() &&
      $"TIPOPERSONALIZACION" === tipoPersonalizacion &&
      $"PrecioOferta" === 0)
      .select("TipoTactica", "CodTactica", "CodCUC", "Unidades", "PrecioOferta")
      .distinct()
  }

  //Se Guardan el números de espacios
  def espaciosForzados: DataFrame = {
    dfArpEspaciosForzados.where($"ANIOCAMPANAPROCESO" === params.anioCampanaProceso() &&
      $"ANIOCAMPANAEXPO" === params.anioCampanaExpo() &&
      $"CODPAIS" === params.codPais() &&
      $"TIPOARP" === params.tipoARP() &&
      $"PERFIL" === params.perfil() &&
      $"TIPOPERSONALIZACION" === tipoPersonalizacion)
      .select("Marca", "Categoria", "TipoForzado", "VinculoEspacio")
  }

  //Se guarda el números de espacios total
  def campaniaExpoEspacios: DataFrame = {
    dfArpParametros.where($"ANIOCAMPANAPROCESO" === params.anioCampanaProceso() &&
      $"ANIOCAMPANAEXPO" === params.anioCampanaExpo() &&
      $"CODPAIS" === params.codPais() &&
      $"TIPOARP" === params.tipoARP() &&
      $"PERFIL" === params.perfil() &&
      $"TIPOPERSONALIZACION" === tipoPersonalizacion)
      .select("AnioCampanaExpo", "Espacios", "EspaciosTop")
      .distinct()
  }

  //SELECT * INTO #ListadoProductos FROM ListadoProductos
  //SELECT * INTO #ListadoRegalos FROM ListadoRegalos
  //SELECT * INTO #EspaciosForzados FROM EspaciosForzados
  //SELECT * INTO #CampaniaExpoEspacios FROM CampaniaExpoEspacios

  def valueFromCampaniaExpoEspacios(colName: String): Int = {
    val value = campaniaExpoEspacios.select(colName).head(1)

    if(value.isEmpty || value(0).anyNull) 0
    else value(0).getInt(0)
  }

  println(valueFromCampaniaExpoEspacios("Espacios"))
  println(valueFromCampaniaExpoEspacios("Espacios").getClass)

  val numEspacios: Int = valueFromCampaniaExpoEspacios("Espacios")
  val numEspaciosTop: Int = valueFromCampaniaExpoEspacios("EspaciosTop")

  //Se guardan los espacios Fijos
  def espaciosFijos: DataFrame = {
    espaciosForzados
      .where($"TipoForzado" === 1)
      .select("Marca", "Categoria", "TipoForzado", "VinculoEspacio")
      .distinct()
  }

  //Se guardan las marcas y categorias que no se deben repetir en los espacios no forzados
  def marcaCategoriaNoRepetir: DataFrame = {
    espaciosForzados
      .where($"TipoForzado" === 0)
      .select("Marca", "Categoria", "TipoForzado")
      .distinct()
  }

  def numEspaciosFijos: Int = {
    val value = espaciosFijos
      .agg(max($"VinculoEspacio").as('VinculoEspacio))
      .head(1)

    if(value.isEmpty || value(0).anyNull) 0
    else value(0).getInt(0)
  }

  val numEspaciosLibres: Int = numEspacios - numEspaciosFijos - numEspaciosTop

  //TODO -> Falar com o Daniel/Takeshi sobre essa situação
  ///* Se evalua que el número de espacios fijos sea menor o igual al número de espacios totales */
  //IF  (@NumEspacios<@NumEspaciosFijos+@NumEspaciosTop)
  //BEGIN
  //RETURN
  //END

  //Se obtienen los productos a nivel CUC
  def productosCUC: DataFrame = {
    listadoProductos.alias("lp")
      .join(dfDwhDproducto.alias("dwhp"), $"lp.CodCUC" === $"dwhp.CodCUC" && $"dwhp.CodPais" === params.codPais(), "Inner")
      .where($"dwhp.DesProductoCUC".isNotNull)
      .groupBy("CodCUC", "TipoTactica", "CodTactica", "CodVenta", "Unidades", "PrecioOferta", "IndicadorPadre", "FlagTop")
      .agg(max($"dwhp.DesProductoCUC").alias("DesProducto"),
        max($"dwhp.DesMarca").alias("DesMarca"),
        max($"dwhp.DESCategoria").alias("DESCategoria"),
        max($"lp.LimUnidades").alias("LimUnidades"),
        max($"lp.FlagUltMinuto").alias("FlagUltMinuto"))
      .select("CodCUC", "TipoTactica", "CodTactica", "CodVenta", "Unidades", "PrecioOferta", "IndicadorPadre", "FlagTop", "CodSAP")

  }

  def updateProductosCUC(): DataFrame = {

    //TODO - Rever questão de quantidade de pais -> Daniel
    productosCUC.alias("pcuc")
      .join(dfDwhDmatrizcampana.alias("dwhdm"), $"dwhdm.CodVenta" === $"pcuc.CodVenta" && $"pcuc.AnioCampanaExpo" === params.anioCampanaExpo(), "Inner")
      .join(dfDwhDproducto.alias("dwhp"), $"dwhdm.CodProducto" === $"dwhp.CodProducto" && $"dwhp.CodPais" === params.codPais(), "Inner")
      .withColumn("CodSAP", when($"pcuc.AnioCampana" === params.anioCampanaExpo() && $"dwhdm.CodPais" === params.codPais(), $"dwhdm.CodSAP"))
  }

  //--Se crea la tabla para cargar los resultados finales
  //  CREATE TABLE #ListadoInterfazFinal(
  //  [CodPais] [varchar](5) NOT NULL,
  //[Tipo] [varchar](3) NOT NULL,
  //[AnioCampanaVenta] [varchar](6) NOT NULL,
  //[CodEbelista] [varchar](15) NOT NULL,
  //[CodProducto] [varchar](20) NULL,
  //[CodSAP] [varchar](18) NULL,
  //[CodVenta] [varchar](5) NOT NULL,
  //[Portal] [varchar](3) NULL,
  //[DiaInicio] [int] NULL,
  //[DiaFin] [int] NULL,
  //[Orden] [int] NULL,
  //[FlagManual] [int] NULL,
  //[TipoARP] [varchar](1) NOT NULL,
  //[CodVinculo] [int] NULL,
  //[PPU] [float] NULL,
  //[LimUnidades] [int] NULL,
  //[FlagUltMinuto] [int] NULL,
  //[Perfil] [varchar](1) NULL)

  //Se crea la tabla para cargar los resultados finales
  /*val schema = StructType(StructField("CodPais", StringType, false) :: StructField("Tipo", StringType, false) ::
    StructField ("AnioCampanaVenta", StringType, false) :: StructField ("CodEbelista", StringType, false) ::
    StructField ("CodProducto", StringType, true) :: StructField ("CodSAP", StringType, true) ::
    StructField ("CodVenta", StringType, false) :: StructField ("Portal", StringType, true) ::
    StructField ("DiaInicio", IntegerType, true) :: StructField ("DiaFin", IntegerType, true) ::
    StructField ("Orden", IntegerType, true) :: StructField ("FlagManual", IntegerType, true) ::
    StructField ("TipoARP", StringType, false) :: StructField ("CodVinculo", IntegerType, true) ::
    StructField ("PPU", FloatType, true) :: StructField ("LimUnidades", IntegerType, true) ::
    StructField ("FlagUltMinuto", IntegerType, true) :: StructField ("Perfil", StringType, true) :: Nil)

  def listadoInterfazFinal = spark.createDataFrame(spark.sparkContext.emptyRDD[Row], schema)*/


}
