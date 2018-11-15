package pe.com.belcorp.arp.tables.impl

import org.apache.spark.sql.expressions.Window
import org.apache.spark.sql.functions._
import org.apache.spark.sql.{DataFrame, SparkSession}
import org.apache.spark.storage.StorageLevel
import pe.com.belcorp.arp.utils.AnioCampana.{Spark => acUDF}
import pe.com.belcorp.arp.utils.Extensions._
import pe.com.belcorp.arp.utils.{AnioCampana, ProcessParams}

class GrupoPotencial(params: ProcessParams,
  dwhDGeografiaCampana: DataFrame, dwhDMatrizCampana: DataFrame,
  dwhDProducto: DataFrame, dwhDTipoOferta: DataFrame,
  dwhFStaEbeCam: DataFrame, dwhFVtaProEbeCam: DataFrame,
  listadoProductos: DataFrame, listadoRegalos: DataFrame,
  baseConsultorasReal: DataFrame, mdlPerfilOutput: DataFrame,
  listadoVariablesIndividual: DataFrame, listadoVariablesBundle: DataFrame) {

  val acProceso: AnioCampana = AnioCampana.parse(params.anioCampanaProceso())

  type Results = (DataFrame, DataFrame, DataFrame)

  def get(spark: SparkSession): Results = {
    val (
      grupoPotencialIndividual, grupoPotencialBundle,
      listadoIndividual, listadoBundle
    ) = if(params.tipoGP().toInt == 1) {
      porSegmentoYRegion()
    } else {
      porPerfil()
    }

    val brechaListadoIndividual = updateListadoConsultora(listadoIndividual)
    val listadoVariablesRFM = montarListadoConsolidado(brechaListadoIndividual,
      listadoBundle)

    (grupoPotencialIndividual, grupoPotencialBundle, listadoVariablesRFM)
  }

  def porSegmentoYRegion(): (DataFrame, DataFrame, DataFrame, DataFrame) = {
    /** Base Potencial U6C para GP por Región y Segmento **/

    // Se obtiene la Base Potencial para las últimas 6 campañas
    // anteriores a la campaña de proceso.
    // Para hallar la Venta Potencial Mínima U6C y la Frecuencia U6C GP
    // Se filtran solo las consultoras que hayan realizado como mínimo
    // un pedido Web o Web Mixto y Activas en la campañana de proceso
    val basePotencialAux3 = {
      val vp = ventasProductos6AC.alias("vp")
      val fs = fstaebecam24AC.alias("fs")
      val dg = dwhDGeografiaCampana.alias("dg")

      vp.join(fs,
        $"fs.CODEBELISTA" === $"vp.CODEBELISTA"
          && $"fs.ANIOCAMPANA" === acProcesso.toString,
        "inner"
      ).join(dg,
        $"dg.CODTERRITORIO" === $"vp.CODTERRITORIO"
          && $"dg.ANIOCAMPANA" === acProcesso.toString
          && $"dg.CODPAIS" === params.codPais(),
        "inner"
      ).where(
        $"vp.ANIOCAMPANA".between(acInicio6UC.toString, acProcesso.toString)
          && $"fs.FLAGACTIVA" === 1
      ).groupBy(
        $"vp.ANIOCAMPANA", $"vp.CODEBELISTA", $"vp.CODTACTICA", $"vp.CODPRODUCTO",
        $"dg.CODREGION", $"fs.CODCOMPORTAMIENTOROLLING", $"vp.NROFACTURA"
      ).agg(
        sum($"REALVTAMNNETO").as("VENTAPOTENCIAL")
      ).select("*").where($"VENTAPOTENCIAL" > 0)
    }

    // Se filtran solo las consultoras que hayan realizado
    // como mínimo un pedido Web o Web Mixto
    val basePotencialAux6AC = {
      val bp = basePotencialAux3.alias("bp")
      val fs = fstaebecam24AC.alias("fs")

      bp.join(fs,
        $"bp.CODEBELISTA" === $"fs.CODEBELISTA",
        "inner"
      ).where(
        $"fs.ANIOCAMPANA".between(acInicio6UC.toString, acProcesso.toString)
          && $"fs.CODIGOFACTURAINTERNET".isin("WEB", "WMX")
      ).selectExpr("bp.*").distinct()
    }

    var basePotencial6AC = {
      // Se halla la venta potencial mínima por consultora y la cantidad
      // de pedidos puros a nivel de consultora
      val basePotencialAux2 = basePotencialAux6AC
        .groupBy(
          $"CODTACTICA", $"CODPRODUCTO", $"CODREGION",
          $"CODCOMPORTAMIENTOROLLING", $"CODEBELISTA"
        ).agg(
        min($"VENTAPOTENCIAL").as("VENTAPOTENCIALMIN"),
        countDistinct($"ANIOCAMPANA").as("PEDIDOSPUROS")
      )

      // Se halla el promedio de la venta potencial mínima por consultora y el promedio
      // de la cantidad de pedidos puros (Frecuencia U6C GP) a nivel de la Base Potencial
      basePotencialAux2
        .groupBy(
          $"CODTACTICA", $"CODPRODUCTO", $"CODREGION", $"CODCOMPORTAMIENTOROLLING"
        ).agg(
        avg($"VENTAPOTENCIALMIN").as("PROMVENTAPOTENCIALMIN"),
        round(
          sum($"PEDIDOSPUROS").cast("double") *
            (lit(1d) / count($"PEDIDOSPUROS")
              ), 1).as("FRECUENCIA")
      )
    }

    /** Recencia y Frecuencia de U24C para GP **/
    var basePotencial24AC = {
      /** Base Potencial U24C para GP por Región y Segmento **/

      val basePotencialAux = {
        val vp = ventasProductos6AC.alias("vp")
        val fs = fstaebecam24AC.alias("fs")
        val dg = dwhDGeografiaCampana.alias("dg")

        vp.join(fs,
          $"fs.CODEBELISTA" === $"vp.CODEBELISTA"
            && $"fs.ANIOCAMPANA" === acProcesso.toString,
          "inner"
        ).join(dg,
          $"dg.CODTERRITORIO" === $"vp.CODTERRITORIO"
            && $"dg.ANIOCAMPANA" === acProcesso.toString
            && $"dg.CODPAIS" === params.codPais(),
          "inner"
        ).where(
          $"vp.ANIOCAMPANA".between(acInicio24UC.toString, acProcesso.toString)
            && $"fs.FLAGACTIVA" === 1
        ).groupBy(
          $"vp.ANIOCAMPANA", $"vp.CODEBELISTA", $"vp.CODTACTICA", $"vp.CODPRODUCTO",
          $"dg.CODREGION", $"fs.CODCOMPORTAMIENTOROLLING", $"vp.NROFACTURA"
        ).agg(
          sum($"REALVTAMNNETO").as("VENTAPOTENCIAL")
        ).select("*").where($"VENTAPOTENCIAL" > 0)
      }

      // Se filtran solo las consultoras que hayan realizado
      // como mínimo un pedido Web o Web Mixto
      var basePotencialAux24AC = {
        val bp = basePotencialAux.alias("bp")
        val fs = fstaebecam24AC.alias("fs")

        bp.join(fs,
          $"fs.CODEBELISTA" === $"bp.CODEBELISTA",
          "inner"
        ).where(
          $"fs.ANIOCAMPANA".between(acInicio6UC.toString, acProcesso.toString)
            && $"fs.CODIGOFACTURAINTERNET".isin("WEB", "WMX")
        ).selectExpr("bp.*").distinct()
      }

      // Se halla la cantidad de pedidos puros
      // frecuencia y el máximo de recencia a nivel de cada consultora
      val basePotencialConsultora24AC = basePotencialAux24AC
        .groupBy(
          $"CODTACTICA", $"CODPRODUCTO", $"CODREGION",
          $"CODCOMPORTAMIENTOROLLING", $"CODEBELISTA"
        ).agg(
        acUDF.delta(
          lit(acProcesso.toString),
          max($"ANIOCAMPANA")
        ).as("RECENCIA"),
        countDistinct($"ANIOCAMPANA").as("PEDIDOSPUROS")
      )

      // Se halla la recencia U24C y frecuencia U24C a nivel de la base potencial
      var basePotencialFreq24AC = basePotencialConsultora24AC
        .groupBy(
          $"CODTACTICA", $"CODPRODUCTO", $"CODREGION", $"CODCOMPORTAMIENTOROLLING"
        ).agg(
        avg($"RECENCIA".cast("double")).as("RECENCIAGP"),
        ceil(round(
          sum($"PEDIDOSPUROS").cast("double") *
            (lit(1d) / count($"PEDIDOSPUROS")
              ))).as("FRECUENCIAGP"),
        lit(0l).as("CICLORECOMPRAPOTENCIAL"),
        lit(0d).as("PRECIOOPTIMOGP")
      )

      // Cálculo de Ciclo de Recompra Potencial
      val baseCicloRecompra = {
        val deltaMin = lit(23) -
          acUDF.delta(min($"ANIOCAMPANA"), lit(acInicio24UC.toString))
        val deltaMax = acUDF.delta(lit(acProcesso.toString), max($"ANIOCAMPANA"))

        val expresionCicloRecompraPotencial = (deltaMin - deltaMax) *
          lit(1d) / (count($"ANIOCAMPANA") - 1)

        basePotencialAux
          .groupBy(
            $"CODEBELISTA", $"CODTACTICA", $"CODPRODUCTO",
            $"CODREGION", $"CODCOMPORTAMIENTOROLLING"
          ).agg(
          max($"ANIOCAMPANA").as("ANIOCAMPANAMAX"),
          expresionCicloRecompraPotencial.as("CICLORECOMPRAPOTENCIAL"),
          (count($"ANIOCAMPANA") > 1).as("VALIDORECOMPRA")
        ).select("*").where($"VALIDORECOMPRA")
      }

      // Actualizo Ciclo Recompra Potencial
      basePotencialFreq24AC = {
        val cicloRecompraAgregado = baseCicloRecompra
          .groupBy(
            $"CODTACTICA", $"CODPRODUCTO",
            $"CODREGION", $"CODCOMPORTAMIENTOROLLING")
          .agg(
            ceil(sum($"CICLORECOMPRAPOTENCIAL") / count($"CODEBELISTA"))
              .as("CRPRECALCULO"))

        val bpf = basePotencialFreq24AC.alias("bpf")
        val cra = cicloRecompraAgregado.alias("cra")

        bpf
          .join(cra, makeEquiJoin("bpf", "cra", Seq(
            "CODTACTICA", "CODPRODUCTO", "CODREGION", "CODCOMPORTAMIENTOROLLING")), "left")
          .select($"bpf.*", $"cra.CRPRECALCULO")
          .withColumn("CICLORECOMPRAPOTENCIAL", $"CRPRECALCULO")
          .drop($"CRPRECALCULO")
      }

      // Calcular la nueva Recencia y Frecuencia de U24C para GP para la
      // consultoras que no han comprado los productos
      val tempConsultorasSemCompra = {
        val bp = basePotencialAux24AC.alias("bp")
        val po = precioOptimoFinal.alias("po")

        val expresionDelta = acUDF.delta(
          lit(acProcesso.toString), max($"bp.ANIOCAMPANA"))

        // Se setean en 24 a los registros cuya recencia es 0
        val expressionRecencia = when(expresionDelta === 0, 24)
          .otherwise(expresionDelta)

        val bpAgregado = bp.groupBy(
          $"CODEBELISTA", $"CODTACTICA", $"CODPRODUCTO",
          $"CODREGION", $"CODCOMPORTAMIENTOROLLING"
        ).agg(
          countDistinct($"ANIOCAMPANA").as("PEDIDOSPUROS"),
          expressionRecencia.as("RECENCIA")
        ).alias("bpAgregado")

        bpAgregado.join(po,
          $"po.CODEBELISTA" === $"bpAgregado.CODEBELISTA"
            && $"po.CODPRODUCTO" === $"bpAgregado.CODPRODUCTO"
            && $"po.CODTACTICA" === $"bpAgregado.CODTACTICA"
            && $"po.POSICION" === 1
        ).select(
          $"bpAgregado.*",
          $"po.PRECIOOFERTA".as("PRECIOOPTIMO")
        )
      }

      // Se suman los pedidos puros
      val tempConsultorasSomaPedidos = {
        val tc = totalConsultorasRegion.alias("tc")

        val ts = tempConsultorasSemCompra
          .groupBy(
            $"CODREGION", $"CODCOMPORTAMIENTOROLLING",
            $"CODTACTICA", $"CODPRODUCTO"
          ).agg(
            sum($"PEDIDOSPUROS").as("TOTALPEDIDOSPUROS"),
            sum($"RECENCIA").as("TOTALRECENCIA"),
            sum($"PRECIOOPTIMO").as("TOTALPRECIOOPTIMO"),
            countDistinct($"CODEBELISTA").as("TOTALCONSULTORASCONVENTA")
          ).alias("ts")

        ts.join(tc,
          $"tc.CODREGION" === $"ts.CODREGION"
            && $"tc.CODCOMPORTAMIENTOROLLING" === $"ts.CODCOMPORTAMIENTOROLLING",
          "inner"
        ).select($"ts.*", $"tc.TOTALCONSULTORAS".as("TOTALCONSULTORAS"))
      }


      // Se actualiza el nuevo valor de la Frecuencia, Recencia y Precio Óptimo U24C GP
      {
        val bp = basePotencialFreq24AC.alias("bp")
        val po = precioOptimoMinimo.alias("po")
        val tc = tempConsultorasSomaPedidos.alias("tc")

        bp.join(tc,
          $"tc.CODREGION" === $"bp.CODREGION"
            && $"tc.CODCOMPORTAMIENTOROLLING" === $"bp.CODCOMPORTAMIENTOROLLING"
            && $"tc.CODPRODUCTO" === $"bp.CODPRODUCTO",
          "inner")
          .join(po,
            $"po.CODTACTICA" === $"bp.CODTACTICA"
              && $"po.CODPRODUCTO" === $"bp.CODPRODUCTO",
            "inner")
          .select(
            $"bp.*",
            when($"tc.TOTALCONSULTORAS" === 0, 0)
              .otherwise(round(
                $"tc.TOTALPEDIDOSPUROS" * lit(1d) / $"tc.TOTALCONSULTORAS",
                2))
              .as("FRECUENCIAGP___2"),
            when($"tc.TOTALCONSULTORAS" === 0, 0)
              .otherwise(
                ($"tc.TOTALRECENCIA" + lit(24) * (
                  $"tc.TOTALCONSULTORAS" - $"tc.TOTALCONSULTORASCONVENTA"
                  )).cast("double") / $"tc.TOTALCONSULTORAS")
              .as("RECENCIAGP___2"),
            when($"tc.TOTALCONSULTORAS" === 0, 0)
              .otherwise(
                ($"tc.TOTALPRECIOOPTIMO" + $"po.PRECIOMINIMO" * (
                  $"tc.TOTALCONSULTORAS" - $"tc.TOTALCONSULTORASCONVENTA"
                  )).cast("double") / $"tc.TOTALCONSULTORAS")
              .as("PRECIOOPTIMOGP___2"))
          .withColumn("FRECUENCIAGP", $"FRECUENCIAGP___2")
          .withColumn("RECENCIAGP", $"RECENCIAGP___2")
          .withColumn("PRECIOOPTIMOGP", $"PRECIOOPTIMOGP___2")
          .drop($"FRECUENCIAGP___2")
          .drop($"RECENCIAGP___2")
          .drop($"PRECIOOPTIMOGP___2")


      }
    }

    // Se actualiza el nuevo valor de la VentaMinima U6C
    basePotencial6AC = {
      // Se halla la venta potencial mínima por Consultora a nivel de consultora
      val tempVentaMinimaConsultora = basePotencialAux6AC
        .groupBy(
          $"CODEBELISTA", $"CODTACTICA", $"CODPRODUCTO",
          $"CODREGION", $"CODCOMPORTAMIENTOROLLING"
        ).agg(
          min($"VENTAPOTENCIAL").as("VENTAPOTENCIALMIN")
        )

      // Se halla el promedio de la venta potencial mínima por
      // Consultora a nivel de la Base Potencial
      val tempVentaMinimaProducto = {
        val ventaAgregada = tempVentaMinimaConsultora
          .groupBy($"CODTACTICA", $"CODPRODUCTO", $"CODREGION", $"CODCOMPORTAMIENTOROLLING")
          .agg(sum($"VENTAPOTENCIALMIN").as("VENTAPOTENCIALMIN"))
          .alias("ventaAgregada")

        val va = ventaAgregada.alias("va")
        val tcr = totalConsultorasRegion.alias("tcr")

        va.join(tcr, makeEquiJoin("va", "tcr",
            Seq("CODREGION", "CODCOMPORTAMIENTOROLLING")))
          .select($"va.*", $"tcr.TOTALCONSULTORAS")
      }


      // Se halla el total de consultoras por Región y Comportamiento
      val bp = basePotencial6AC.alias("bp")
      val tv = tempVentaMinimaProducto.alias("tv")

      bp.join(tv, makeEquiJoin("bp", "tv", Seq(
        "CODTACTICA", "CODPRODUCTO", "CODREGION", "CODCOMPORTAMIENTOROLLING")))
        .select(
          $"bp.*",
          when($"tv.TOTALCONSULTORAS" === 0, 0)
            .otherwise(
              round($"tv.VENTAPOTENCIALMIN".cast("double") / $"tv.TOTALCONSULTORAS", 2))
            .as("PROMVENTAPOTENCIALMIN___2"))
        .withColumn("PROMVENTAPOTENCIALMIN", $"PROMVENTAPOTENCIALMIN___2")
        .drop($"PROMVENTAPOTENCIALMIN___2")
    }

    // Marcar como cache
    basePotencial24AC.persist(StorageLevel.MEMORY_AND_DISK_SER)
    basePotencial6AC.persist(StorageLevel.MEMORY_AND_DISK_SER)

    // Actualizar las variables por GP
    // GP por Región y Segmento
    val listadoConsultoraAtualizado = {
      val lc = listadoConsultora.alias("lc")
      val bp24 = basePotencial24AC.alias("bp24")
      val bp6 = basePotencial6AC.alias("bp6")

      // Se actualiza el Ciclo de Recompra Potencial (Se busca en la Base Potencial)
      // Se actualiza la Venta Potencial Mínima U6C (Se busca en la Base Potencial)
      // Se actualiza la Frecuencia U24C, Recencia U24C y Precio Óptimo GP para
      // las consultoras que no tuvieron venta (Se busca de la Base Potencial)
      lc
        .join(bp24, makeEquiJoin("lc", "bp24", Seq(
          "CODTACTICA", "CODPRODUCTO",
          "CODREGION", "CODCOMPORTAMIENTOROLLING")))
        .join(bp6, makeEquiJoin("lc", "bp6", Seq(
          "CODTACTICA", "CODPRODUCTO",
          "CODREGION", "CODCOMPORTAMIENTOROLLING")))
        .select($"lc.*",
          $"bp24.CICLORECOMPRAPOTENCIAL".as("CICLORECOMPRAPOTENCIAL___2"),
          $"bp6.PROMVENTAPOTENCIALMIN".as("VENTAPOTENCIALMINU6C___2"),
          when($"lc.FLAGCOMPRA" === 0, $"bp24.FRECUENCIAGP")
            .otherwise($"lc.FRECUENCIAU24C").as("FRECUENCIAU24C___2"),
          when($"lc.FLAGCOMPRA" === 0, $"bp24.RECENCIAGP")
            .otherwise($"lc.RECENCIAU24C").as("RECENCIAU24C___2"),
          when($"lc.FLAGCOMPRA" === 0, $"bp24.PRECIOOPTIMOGP")
            .otherwise($"lc.PRECIOOPTIMO").as("PRECIOOPTIMO___2"))
        .withColumn("CICLORECOMPRAPOTENCIAL", $"CICLORECOMPRAPOTENCIAL___2")
        .withColumn("PROMVENTAPOTENCIALMIN", $"VENTAPOTENCIALMINU6C___2")
        .withColumn("FRECUENCIAU24C", $"FRECUENCIAU24C___2")
        .withColumn("RECENCIAU24C", $"RECENCIAU24C___2")
        .withColumn("PRECIOOPTIMO", $"PRECIOOPTIMO___2")
        .drop($"CICLORECOMPRAPOTENCIAL___2")
        .drop($"VENTAPOTENCIALMINU6C___2")
        .drop($"FRECUENCIAU24C___2")
        .drop($"RECENCIAU24C___2")
        .drop($"PRECIOOPTIMO___2")
    }

    val grupoPotencial = listadoConsultoraAtualizado
      .groupBy($"CODTACTICA", $"CODREGION", $"CODCOMPORTAMIENTOROLLING")
      .agg(
        avg($"GATILLADOR").as("GATILLADORGP"),
        avg($"FRECUENCIAU24C").as("FRECUENCIAU24CGP")
      )

    // Se actualiza el Gatillador para las consultoras que no tuvieron venta
    val listadoConsultoraCompleto = {
      val lc = listadoConsultoraAtualizado.alias("lc")
      val gp = grupoPotencial.alias("gp")

      lc.join(gp, makeEquiJoin("lc", "gp", Seq(
        "CODTACTICA", "CODREGION", "CODCOMPORTAMIENTOROLLING")))
        .select(
          $"lc.*",
          when($"lc.FLAGCOMPRA" === 0, $"gp.GATILLADORGP")
            .otherwise($"lc.GATILLADOR").as("GATILLADOR___2"))
        .withColumn("GATILLADOR", $"GATILLADOR___2")
        .drop($"GATILLADOR___2")
    }

    listadoConsultoraCompleto.persist(StorageLevel.MEMORY_AND_DISK_SER)

    // Cargar a tabla de Grupo Potencial
    val grupoPotencialCompleto = {
      val bp = basePotencial24AC.alias("bp")
      val bp6 = basePotencial6AC.alias("bp6")
      val gp = grupoPotencial.alias("gp")

      bp
        .join(bp6, makeEquiJoin("bp", "bp6", Seq(
          "CODTACTICA", "CODPRODUCTO", "CODREGION", "CODCOMPORTAMIENTOROLLING")))
        .join(gp, makeEquiJoin("bp", "gp", Seq(
          "CODTACTICA", "CODREGION", "CODCOMPORTAMIENTOROLLING")))
        .select(
          $"bp.CODTACTICA", $"bp.CODPRODUCTO",
          $"bp.CODREGION", $"bp.CODCOMPORTAMIENTOROLLING",
          repeat(lit(" "), 10).as("PERFILGP"),
          $"bp.FRECUENCIAGP".as("FRECUENCIAU24C"),
          $"bp.RECENCIAGP".as("RECENCIAU24C"),
          $"bp.PRECIOOPTIMOGP".as("PRECIOPOTIMO"),
          $"bp.CICLORECOMPRAPOTENCIAL",
          $"bp6.PROMVENTAPOTENCIALMIN".as("PROMVENTAPOTENCIALMINU6C"),
          $"gp.GATILLADORGP".as("GATILLADOR"),
          lit(params.codPais()).as("CODPAIS"),
          lit(acProcesso.toString).as("ANIOCAMPANAPROCESO"),
          lit(params.anioCampanaExpo()).as("ANIOCAMPANAEXPO"),
          lit(params.tipoARP()).as("TIPOARP"),
          lit(params.tipoPersonalizacion()).as("TIPOPERSONALIZACION"),
          lit(params.perfil()).as("PERFIL"),
          lit("Individual").as("TIPOTACTICA"),
          lit(params.tipoGP()).as("TIPOGP")
        )
    }

    // partitionBy(
    //  "CODPAIS", "ANIOCAMPANAPROCESO", "ANIOCAMPANAEXPO",
    //  "TIPOARP", "TIPOPERSONALIZACION", "PERFIL", "TIPOTACTICA", "TIPOGP")

    // Bundle
    // GP por Región y Segmento
    val (grupoPotencialBundle, listadoConsultoraBundleAtualizado) = {
      // Se actualiza El ciclo de Recompra Potencial
      // se busca en la Base Potencial: BASE POTENCIAL BUNDLE
      val lb = listadoConsultoraBundle.alias("lb")
      val bp = lb
        .groupBy($"CODREGION", $"CODCOMPORTAMIENTOROLLING", $"CODTACTICA", $"CODPRODUCTO")
        .agg(
          ceil(sum($"CICLORECOMPRAPOTENCIAL".cast("double")) / count($"CODEBELISTA"))
            .as("CICLORECOMPRAPOTENCIAL"),
          avg($"PRECIOMINIMO").as("PRECIOMINIMO"))
        .alias("bp")

      val nuevoListado = lb
        .join(bp, makeEquiJoin("lb", "bp", Seq(
          "CODREGION", "CODCOMPORTAMIENTOROLLING", "CODTACTICA", "CODPRODUCTO")))
        .select(
          $"lb.*",
          $"bp.CICLORECOMPRAPOTENCIAL".as("CICLORECOMPRAPOTENCIAL___2"),
          when($"lb.PRECIOOPTIMO" === 0, $"bp.PRECIOMINIMO")
            .otherwise($"lb.PRECIOOPTIMO").as("PRECIOOPTIMO____2"))
        .withColumn("CICLORECOMPRAPOTENCIAL", $"CICLORECOMPRAPOTENCIAL___2")
        .withColumn("PRECIOOPTIMO", $"PRECIOOPTIMO____2")
        .drop($"CICLORECOMPRAPOTENCIAL___2")
        .drop($"PRECIOOPTIMO____2")

      val grupoPotencial = lb.select(
        $"CODTACTICA", $"CODPRODUCTO",
        $"CODREGION", $"CODCOMPORTAMIENTOROLLING",
        repeat(lit(" "), 10).as("PERFILGP"),
        lit(0d).as("FRECUENCIAU24C"),
        lit(0d).as("RECENCIAU24C"),
        $"PRECIOMINIMO".as("PRECIOOPTIMO"),
        $"CICLORECOMPRAPOTENCIAL",
        lit(0d).as("PROMVENTAPOTENCIALMINU6C"),
        lit(0d).as("GATILLADOR"),
        lit(params.codPais()).as("CODPAIS"),
        lit(acProcesso.toString).as("ANIOCAMPANAPROCESO"),
        lit(params.anioCampanaExpo()).as("ANIOCAMPANAEXPO"),
        lit(params.tipoARP()).as("TIPOARP"),
        lit(params.tipoPersonalizacion()).as("TIPOPERSONALIZACION"),
        lit(params.perfil()).as("PERFIL"),
        lit("Bundle").as("TIPOTACTICA"),
        lit(params.tipoGP()).as("TIPOGP")
      )

      (grupoPotencial, updateListadoConsultoraBundle(nuevoListado))
    }

    val listadoTacticaBundle = createTacticaBundle(listadoConsultoraBundleAtualizado)
      .persist(StorageLevel.MEMORY_AND_DISK_SER)

    val basePotencialTacticaBundle = listadoTacticaBundle
      .groupBy($"CODREGION", $"CODCOMPORTAMIENTOROLLING", $"CODTACTICA")
      .agg(
        (sum($"VENTAPOTENCIALMINU6C") / countDistinct($"CODEBELISTA"))
          .as("VENTAPOTENCIALMINU6C_GP"),
        (sum($"FRECUENCIAU24C") / countDistinct($"CODEBELISTA"))
          .as("FRECUENCIAU24C_GP"),
        avg($"RECENCIAU24C_GP".cast("double")).as("RECENCIAU24C_GP"),
        (sum($"GATILLADOR") / countDistinct($"CODEBELISTA"))
          .as("GATILLADOR_GP")
      )

    val basePotencialTacticaBundlePO = listadoTacticaBundle
      .where($"GAPPRECIOOPTIMO" =!= 0)
      .groupBy($"CODREGION", $"CODCOMPORTAMIENTOROLLING", $"CODTACTICA")
      .agg((sum($"GAPPRECIOOPTIMO") / countDistinct($"CODEBELISTA"))
        .as("GAPPRECIOOPTIMO_GP"))

    // Se actualiza a nivel de Táctica
    val listadoTacticaBundleAtualizado = {
      val lt = listadoTacticaBundle.alias("lt")
      val bp = basePotencialTacticaBundle.alias("bp")
      val bo = basePotencialTacticaBundlePO.alias("bo")

      lt.join(bp, makeEquiJoin("lt", "bp", Seq("CODREGION", "CODTACTICA", "CODCOMPORTAMIENTOROLLING")))
        .join(bo,
          $"bo.CODREGION" === $"lt.CODREGION" &&
            $"bo.CODTACTICA" === $"lt.CODTACTICA" &&
            $"bo.CODCOMPORTAMIENTOROLLING" === $"lt.CODCOMPORTAMIENTOROLLING",
          "inner")
        .updateWith(Seq($"lt.*"), Map(
          "BRECHAVENTA" ->
            when($"lt.BRECHAVENTA" === 0, $"bp.VENTAPOTENCIALMINU6C_GP")
              .otherwise($"lt.BRECHAVENTA"),
          "FRECUENCIAU24C" ->
            when($"lt.FRECUENCIAU24C" === 0, $"bp.FRECUENCIAU24C_GP")
              .otherwise($"lt.FRECUENCIAU24C"),
          "GATILLADOR" ->
            when($"lt.GATILLADOR" === 0, $"bp.GATILLADOR_GP")
              .otherwise($"lt.GATILLADOR"),
          "GAPPRECIOOPTIMO" ->
            when($"lt.PRECIOOPTIMO" === 0, $"bo.GAPPRECIOOPTIMO_GP")
              .otherwise($"lt.GAPPRECIOOPTIMO")))
    }

    // Actualizo la tabla de Grupo Potencial
    val grupoPotencialBundleCompleto = {
      val gp = grupoPotencialBundle.alias("gp")
      val bp = basePotencialTacticaBundle.alias("bp")

      gp.join(bp, makeEquiJoin("gp", "bp", Seq("CODREGION", "CODTACTICA", "CODCOMPORTAMIENTOROLLING")))
        .updateWith(Seq($"gp.*"), Map(
          "FRECUENCIAU24C" -> $"bp.FRECUENCIAU24C_GP",
          "RECENCIAU24C" -> $"bp.RECENCIAU24C_GP",
          "PROMVENTAPOTENCIALMINU6C" -> $"bp.VENTAPOTENCIALMINU6C_GP",
          "GATILLADOR" -> $"bp.GATILLADOR_GP"
        ))
    }

    val listadoTacticaBundleCompleto = listadoTacticaBundleAtualizado
      .updateWith(Seq($"*"), Map("RECENCIAU24C" -> $"RECENCIAU24C_GP"))

    listadoTacticaBundleCompleto.persist(StorageLevel.MEMORY_AND_DISK_SER)

    (
      grupoPotencialCompleto, grupoPotencialBundleCompleto,
      listadoConsultoraCompleto, listadoTacticaBundleCompleto
    )
  }

  def porPerfil(): (DataFrame, DataFrame, DataFrame, DataFrame) = {
    /** Base Potencial U6C para GP por Perfil **/

    // Se obtiene la Base Potencial para las últimas 6 campañas anteriores
    // a la campaña de proceso.
    // Para hallar la Venta Potencial Mínima U6C y la Frecuencia U6C GP
    // Se filtran solo las consultoras que hayan realizado como mínimo
    // un pedido Web o Web Mixto y Activas en la campañana de proceso
    val basePotencialAux3 = {
      val vp = ventasProductos6AC.alias("vp")
      val fs = fstaebecam24AC.alias("fs")
      val ic = infoConsultora.alias("ic")

      vp.join(fs,
        $"fs.CODEBELISTA" === $"vp.CODEBELISTA"
          && $"fs.ANIOCAMPANA" === acProcesso.toString,
        "inner"
      ).join(ic,
        $"ic.CODEBELISTA" === $"vp.CODEBELISTA",
        "inner"
      ).where(
        $"vp.ANIOCAMPANA".between(acInicio6UC.toString, acProcesso.toString)
          && $"fs.FLAGACTIVA" === 1
      ).groupBy(
        $"vp.ANIOCAMPANA", $"vp.CODEBELISTA", $"vp.CODTACTICA",
        $"vp.CODPRODUCTO", $"ic.PERFILGP", $"vp.NROFACTURA"
      ).agg(
        sum($"REALVTAMNNETO").as("VENTAPOTENCIAL")
      ).select("*").where($"VENTAPOTENCIAL" > 0)
    }

    // Se filtran solo las consultoras que hayan realizado
    // como mínimo un pedido Web o Web Mixto
    val basePotencialAux6AC = {
      val bp = basePotencialAux3.alias("bp")
      val fs = fstaebecam24AC.alias("fs")

      bp.join(fs,
        $"fs.CODEBELISTA" === $"bp.CODEBELISTA",
        "inner"
      ).where(
        $"fs.ANIOCAMPANA".between(acInicio6UC.toString, acProcesso.toString)
          && $"fs.CODIGOFACTURAINTERNET".isin("WEB", "WMX")
      ).select($"bp.*").distinct()
    }

    var basePotencial6AC = {
      // Se halla la venta potencial mínima por consultora y la cantidad
      // de pedidos puros a nivel de consultora
      val basePotencialAux2 = basePotencialAux6AC
        .groupBy(
          $"CODTACTICA", $"CODPRODUCTO", $"PERFILGP", $"CODEBELISTA"
        ).agg(
        min($"VENTAPOTENCIAL").as("VENTAPOTENCIALMIN"),
        countDistinct($"ANIOCAMPANA").as("PEDIDOSPUROS")
      )

      // Se halla el promedio de la venta potencial mínima por consultora y el promedio
      // de la cantidad de pedidos puros (Frecuencia U6C GP) a nivel de la Base Potencial
      basePotencialAux2
        .groupBy(
          $"CODTACTICA", $"CODPRODUCTO", $"PERFILGP"
        ).agg(
        avg($"VENTAPOTENCIALMIN").as("PROMVENTAPOTENCIALMIN"),
        round(
          sum($"PEDIDOSPUROS").cast("double") *
            (lit(1d) / count($"PEDIDOSPUROS")
              ), 1).as("FRECUENCIA")
      )
    }

    /** Base Potencial U24C para GP por Perfil **/
    var basePotencial24AC = {
      val basePotencialAux = {
        val fv = ventasProductos24AC.alias("fv")
        val fs = fstaebecam24AC.alias("fs")
        val ic = infoConsultora.alias("ic")

        fv.join(fs,
          $"fs.CODEBELISTA" === $"fv.CODEBELISTA"
            && $"fs.ANIOCAMPANA" === acProcesso.toString,
          "inner"
        ).join(ic,
          $"ic.CODEBELISTA" === $"fv.CODEBELISTA",
          "inner"
        ).where(
          $"fv.ANIOCAMPANA".between(acInicio24UC.toString, acProcesso.toString)
            && $"fs.FLAGACTIVA" === 1
        ).groupBy(
          $"fv.ANIOCAMPANA", $"fv.CODEBELISTA", $"fv.CODTACTICA",
          $"fv.CODPRODUCTO", $"ic.PERFILGP", $"fv.NROFACTURA"
        ).agg(
          sum($"REALVTAMNNETO").as("VENTAPOTENCIAL")
        ).select("*").where($"VENTAPOTENCIAL" > 0)
      }

      val basePotencialAuxDistinct = {
        val bp = basePotencialAux.alias("bp")
        val fs = fstaebecam24AC.alias("fs")

        bp.join(fs,
          $"fs.CODEBELISTA" === $"bp.CODEBELISTA",
          "inner"
        ).where(
          $"fs.ANIOCAMPANA".between(acInicio6UC.toString, acProcesso.toString)
            && $"fs.CODIGOFACTURAINTERNET".isin("WEB", "WMX")
        ).select($"bp.*").distinct()
      }

      // Se halla la cantidad de pedidos puros - frecuencia y
      // el máximo de recencia a nivel de cada consultora
      val tempPedidosConsultora = basePotencialAuxDistinct
        .groupBy($"CODTACTICA", $"CODPRODUCTO", $"PERFILGP", $"CODEBELISTA")
        .agg(
          acUDF.delta(lit(acProcesso.toString), max($"ANIOCAMPANA")).as("RECENCIA"),
          countDistinct($"ANIOCAMPANA").as("PEDIDOSPUROS"))

      var basePotencialFinal = tempPedidosConsultora
        .groupBy($"CODTACTICA", $"CODPRODUCTO", $"PERFILGP")
        .agg(
          lit(0L).as("CICLORECOMPRAPOTENCIAL"),
          lit(0d).as("PRECIOOPTIMOGP"),
          avg($"RECENCIA".cast("double")).as("RECENCIAGP"),
          ceil(round(
            sum($"PEDIDOSPUROS") * lit(1d) / count($"PEDIDOSPUROS"), 1
          )).as("FRECUENCIAGP")
        )

      // Cálculo de Ciclo de Recompra Potencial
      val baseCicloRecompra = {
        val expressionReciclo = (
          lit(23l) -
            acUDF.delta(min($"ANIOCAMPANA"), lit(acInicio24UC.toString)) -
            acUDF.delta(lit(acProcesso.toString), max($"ANIOCAMPANA"))
          ) * lit(1d) / (count($"ANIOCAMPANA") - lit(1))

        basePotencialAux
          .groupBy($"CODTACTICA", $"CODPRODUCTO", $"PERFILGP", $"CODEBELISTA")
          .agg(
            max($"ANIOCAMPANA").as("ANIOCAMPANAMAX"),
            expressionReciclo.as("CICLORECOMPRAPOTENCIAL"),
            (count($"ANIOCAMPANA") > lit(1)).as("LINEAVALIDA"))
          .select("*").where($"LINEAVALIDA")
      }

      // Actualizo Ciclo Recompra Potencial
      basePotencialFinal = {
        val cicloAgregado = baseCicloRecompra
          .groupBy($"CODTACTICA", $"CODPRODUCTO", $"PERFILGP")
          .agg(
            ceil(sum($"CICLORECOMPRAPOTENCIAL") /
              count($"CODEBELISTA").cast("double")).as("CICLORECOMPRAPOTENCIAL"))

        val ca = cicloAgregado.alias("ca")
        val bpf = basePotencialFinal.alias("bpf")

        bpf
          .join(ca, makeEquiJoin("bpf", "ca", Seq(
            "CODTACTICA", "CODPRODUCTO", "PERFILGP")))
          .updateWith(Seq($"bpf.*"), Map(
            "CICLORECOMPRAPOTENCIAL"-> $"ca.CICLORECOMPRAPOTENCIAL"
          ))
      }

      // Calcular la nueva Recencia y Frecuencia de U24C para GP para la consultoras
      // que no han comprado los productos
      {
        val expressionRecencia = acUDF.delta(lit(acProcesso.toString), max($"ANIOCAMPANA"))
        val conditionalRecencia = when(expressionRecencia === 0, 24)
          .otherwise(expressionRecencia)

        val tempRecencia = basePotencialAuxDistinct
          .groupBy($"CODTACTICA", $"CODPRODUCTO", $"PERFILGP", $"CODEBELISTA")
          .agg(
            countDistinct($"ANIOCAMPANA").as("PEDIDOSPUROS"),
            conditionalRecencia.as(s"RECENCIA")
          )

        val tempRecenciaPrecio = {
          val tr = tempRecencia.alias("tr")
          val po = precioOptimoFinal.alias("po")

          tr.join(po,
            $"po.CODEBELISTA" === $"tr.CODEBELISTA"
              && $"po.CODPRODUCTO" === $"tr.CODPRODUCTO"
              && $"po.CODTACTICA" === $"tr.CODTACTICA"
              && $"po.POSICION" === 1,
            "inner")
            .select($"tr.*", $"po.PRECIOOFERTA".as("PRECIOOPTIMO"))
        }

        val tempAgregadoVentas = {
          val tc = totalConsultorasGP.alias("tc")
          val ts = tempRecenciaPrecio
            .groupBy($"PERFILGP", $"CODTACTICA", $"CODPRODUCTO")
            .agg(
              sum($"PEDIDOSPUROS").as("TOTALPEDIDOSPUROS"),
              sum($"RECENCIA").as("TOTALRECENCIA"),
              sum($"PRECIOOPTIMO").as("TOTALPRECIOOPTIMO"),
              countDistinct($"CODEBELISTA").as("TOTALCONSULTORASCONVENTA"))
            .alias("ts")

          ts.join(tc,
            $"tc.PERFILGP" === $"ts.PERFILGP",
            "inner"
          ).select($"ts.*", $"tc.TOTALCONSULTORAS".as("TOTALCONSULTORAS"))
        }

        // Se actualiza el nuevo valor de la Frecuencia, Recencia y Precio Óptimo U24C GP
        {
          val bp = basePotencialFinal.alias("bp")
          val po = precioOptimoMinimo.alias("po")
          val tc = tempAgregadoVentas.alias("tc")

          bp.join(tc,
            $"tc.PERFILGP" === $"bp.PERFILGP"
              && $"tc.CODPRODUCTO" === $"tc.CODPRODUCTO",
            "inner")
            .join(po,
              $"po.CODTACTICA" === $"bp.CODTACTICA"
                && $"po.CODPRODUCTO" === $"tc.CODPRODUCTO",
              "inner")
            .updateWith(Seq($"bp.*"), Map(
              "FRECUENCIAGP" ->
                when($"tc.TOTALCONSULTORAS" === 0, 0)
                  .otherwise(round(
                    $"tc.TOTALPEDIDOSPUROS" * lit(1d) / $"tc.TOTALCONSULTORAS",
                    2)),
              "RECENCIAGP" ->
                when($"tc.TOTALCONSULTORAS" === 0, 0)
                  .otherwise(
                    ($"tc.TOTALRECENCIA" + lit(24) * (
                      $"tc.TOTALCONSULTORAS" - $"tc.TOTALCONSULTORASCONVENTA"
                      )).cast("double") / $"tc.TOTALCONSULTORAS"),
              "PRECIOOPTIMOGP" ->
                when($"tc.TOTALCONSULTORAS" === 0, 0)
                  .otherwise(
                    ($"tc.TOTALPRECIOOPTIMO" + $"po.PRECIOMINIMO" * (
                      $"tc.TOTALCONSULTORAS" - $"tc.TOTALCONSULTORASCONVENTA"
                      )).cast("double") / $"tc.TOTALCONSULTORAS")))
        }
      }
    }

    // Se actualiza el nuevo valor de la VentaMinima U6C
    basePotencial6AC = {
      // Se halla la venta potencial mínima por Consultora a nivel de consultora
      val tempVentaMinimaConsultora = basePotencialAux6AC
        .groupBy($"CODEBELISTA", $"CODTACTICA", $"CODPRODUCTO", $"PERFILGP")
        .agg(min($"VENTAPOTENCIAL").as("VENTAPOTENCIALMIN"))

      // Se halla el promedio de la venta potencial mínima por
      // Consultora a nivel de la Base Potencial
      val tempVentaMinimaProducto = {
        val ventaAgregada = tempVentaMinimaConsultora
          .groupBy($"CODTACTICA", $"CODPRODUCTO", $"PERFILGP")
          .agg(sum($"VENTAPOTENCIALMIN").as("VENTAPOTENCIALMIN"))

        val va = ventaAgregada.alias("va")
        val tcgp = totalConsultorasGP.alias("tcgp")

        va
          .join(tcgp, makeEquiJoin("va", "tcgp", Seq("PERFILGP")))
          .select($"va.*", $"tcgp.TOTALCONSULTORAS")
      }

      // Se actualiza el nuevo valor de la VentaMinima U6C
      val bp = basePotencial6AC.alias("bp")
      val tv = tempVentaMinimaProducto.alias("tv")

      bp
        .join(tv, makeEquiJoin("bp", "tv",
          Seq("CODTACTICA", "CODPRODUCTO", "PERFILGP")))
        .updateWith(Seq($"bp.*"), Map(
          "PROMVENTAPOTENCIALMIN" ->
            when($"tv.TOTALCONSULTORAS" === 0, 0)
              .otherwise(
                round($"tv.VENTAPOTENCIALMIN".cast("double") / $"tv.TOTALCONSULTORAS", 2)
              )))
    }

    basePotencial24AC.persist(StorageLevel.MEMORY_AND_DISK_SER)
    basePotencial6AC.persist(StorageLevel.MEMORY_AND_DISK_SER)

    // Actualizar las variables por GP
    // GP por Región y Segmento
    val listadoConsultoraAtualizado = {
      val lc = listadoConsultora.alias("lc")
      val bp24 = basePotencial24AC.alias("bp24")
      val bp6 = basePotencial6AC.alias("bp6")

      // Se actualiza el Ciclo de Recompra Potencial (Se busca en la Base Potencial)
      // Se actualiza la Venta Potencial Mínima U6C (Se busca en la Base Potencial)
      // Se actualiza la Frecuencia U24C, Recencia U24C y Precio Óptimo GP para
      // las consultoras que no tuvieron venta (Se busca de la Base Potencial)
      lc.join(bp24, makeEquiJoin("lc", "bp24", Seq("CODTACTICA", "CODPRODUCTO", "PERFILGP")))
        .join(bp6, makeEquiJoin("lc", "bp6", Seq(
          "CODTACTICA", "CODPRODUCTO", "PERFILGP")))
        .updateWith(Seq($"lc.*"), Map(
          "CICLORECOMPRAPOTENCIAL" -> $"bp24.CICLORECOMPRAPOTENCIAL",
          "VENTAPOTENCIALMINU6C" -> $"bp6.PROMVENTAPOTENCIALMIN",
          "FRECUENCIAU24C" -> when($"lc.FLAGCOMPRA" === 0, $"bp24.FRECUENCIAGP")
            .otherwise($"lc.FRECUENCIAU24C"),
          "RECENCIAU24C" -> when($"lc.FLAGCOMPRA" === 0, $"bp24.RECENCIAGP")
            .otherwise($"lc.RECENCIAU24C"),
          "PRECIOOPTIMO" -> when($"lc.FLAGCOMPRA" === 0, $"bp24.PRECIOOPTIMOGP")
            .otherwise($"lc.PRECIOOPTIMO")))
    }

    val grupoPotencial = listadoConsultoraAtualizado
      .groupBy($"CODTACTICA", $"PERFILGP")
      .agg(
        avg($"GATILLADOR").as("GATILLADORGP"),
        avg($"FRECUENCIAU24C").as("FRECUENCIAU24CGP")
      )

    // Se actualiza el Gatillador para las consultoras que no tuvieron venta
    val listadoConsultoraCompleto = {
      val lc = listadoConsultoraAtualizado.alias("lc")
      val gp = grupoPotencial.alias("gp")

      lc.join(gp, makeEquiJoin("lc", "gp", Seq("CODTACTICA", "PERFILGP")))
        .updateWith(Seq($"lc.*"), Map(
          "GATILLADOR" ->
            when($"lc.FLAGCOMPRA" === 0, $"gp.GATILLADORGP")
              .otherwise($"lc.GATILLADOR")))
    }

    listadoConsultoraCompleto.persist(StorageLevel.MEMORY_AND_DISK_SER)

    // Cargar a tabla de Grupo Potencial
    val grupoPotencialCompleto = {
      val bp = basePotencial24AC.alias("bp")
      val bp6 = basePotencial6AC.alias("bp6")
      val gp = grupoPotencial.alias("gp")

      bp
        .join(bp6,
          $"bp.CODTACTICA" === $"bp6.CODTACTICA" &&
            $"bp.CODPRODUCTO" === $"bp6.CODPRODUCTO" &&
            $"bp.PERFILGP" === $"bp6.PERFILGP")
        .join(gp,
          $"bp.CODTACTICA" === $"gp.CODTACTICA" &&
            $"bp.PERFILGP" === $"gp.PERFILGP")
        .select(
          $"bp.CODTACTICA", $"bp.CODPRODUCTO",
          repeat(lit(" "), 3).as("CODREGION"),
          lit(0).as("CODCOMPORTAMIENTOROLLING"),
          $"bp.PERFILGP",
          $"bp.FRECUENCIAGP".as("FRECUENCIAU24C"),
          $"bp.RECENCIAGP".as("RECENCIAU24C"),
          $"bp.PRECIOOPTIMOGP".as("PRECIOPOTIMO"),
          $"bp.CICLORECOMPRAPOTENCIAL",
          $"bp6.PROMVENTAPOTENCIALMIN".as("PROMVENTAPOTENCIALMINU6C"),
          $"gp.GATILLADORGP".as("GATILLADOR"),
          lit(params.codPais()).as("CODPAIS"),
          lit(acProcesso.toString).as("ANIOCAMPANAPROCESO"),
          lit(params.anioCampanaExpo()).as("ANIOCAMPANAEXPO"),
          lit(params.tipoARP()).as("TIPOARP"),
          lit(params.tipoPersonalizacion()).as("TIPOPERSONALIZACION"),
          lit(params.perfil()).as("PERFIL"),
          lit("Individual").as("TIPOTACTICA"),
          lit(params.tipoGP()).as("TIPOGP")
        )
    }

    // partitionBy(
    // "CODPAIS", "ANIOCAMPANAPROCESO", "ANIOCAMPANAEXPO",
    // "TIPOARP", "TIPOPERSONALIZACION", "PERFIL", "TIPOTACTICA", "TIPOGP")

    // Bundle
    // GP por Perfil
    val (grupoPotencialBundle, listadoConsultoraBundleAtualizado) = {
      // Se actualiza El ciclo de Recompra Potencial
      // se busca en la Base Potencial: BASE POTENCIAL BUNDLE
      val lb = listadoConsultoraBundle.alias("lb")
      val bp = lb
        .groupBy($"PERFILGP", $"CODTACTICA", $"CODPRODUCTO")
        .agg(
          ceil(sum($"CICLORECOMPRAPOTENCIAL".cast("double")) / count($"CODEBELISTA"))
            .as("CICLORECOMPRAPOTENCIAL"),
          avg($"PRECIOMINIMO").as("PRECIOMINIMO"))
        .alias("bp")

      val nuevoListado = lb.join(bp, makeEquiJoin("lb", "bp", Seq("PERFILGP", "CODTACTICA", "CODPRODUCTO")))
        .updateWith(Seq($"lb.*"), Map(
          "CICLORECOMPRAPOTENCIAL" ->
            $"bp.CICLORECOMPRAPOTENCIAL",
          "PRECIOOPTIMO" ->
            when($"lb.PRECIOOPTIMO" === 0, $"bp.PRECIOMINIMO")
              .otherwise($"lb.PRECIOOPTIMO")
        ))

      val grupoPotencial = lb.select(
        $"CODTACTICA", $"CODPRODUCTO",
        repeat(lit(" "), 3).as("CODREGION"),
        lit(0l).as("CODCOMPORTAMIENTOROLLING"),
        $"PERFILGP",
        lit(0d).as("FRECUENCIAU24C"),
        lit(0d).as("RECENCIAU24C"),
        $"PRECIOMINIMO".as("PRECIOOPTIMO"),
        $"CICLORECOMPRAPOTENCIAL",
        lit(0d).as("PROMVENTAPOTENCIALMINU6C"),
        lit(0d).as("GATILLADOR"),
        lit(params.codPais()).as("CODPAIS"),
        lit(acProcesso.toString).as("ANIOCAMPANAPROCESO"),
        lit(params.anioCampanaExpo()).as("ANIOCAMPANAEXPO"),
        lit(params.tipoARP()).as("TIPOARP"),
        lit(params.tipoPersonalizacion()).as("TIPOPERSONALIZACION"),
        lit(params.perfil()).as("PERFIL"),
        lit("Bundle").as("TIPOTACTICA"),
        lit(params.tipoGP()).as("TIPOGP")
      )

      (grupoPotencial, updateListadoConsultoraBundle(nuevoListado))
    }

    listadoConsultoraBundleAtualizado.persist(StorageLevel.MEMORY_AND_DISK_SER)

    val listadoTacticaBundle = createTacticaBundle(listadoConsultoraBundleAtualizado)

    val basePotencialTacticaBundle = listadoTacticaBundle
      .groupBy($"PERFILGP", $"CODTACTICA")
      .agg(
        (sum($"VENTAPOTENCIALMINU6C") / countDistinct($"CODEBELISTA"))
          .as("VENTAPOTENCIALMINU6C_GP"),
        (sum($"FRECUENCIAU24C") / countDistinct($"CODEBELISTA"))
          .as("FRECUENCIAU24C_GP"),
        avg($"RECENCIAU24C_GP".cast("double")).as("RECENCIAU24C_GP"),
        (sum($"GATILLADOR") / countDistinct($"CODEBELISTA"))
          .as("GATILLADOR_GP"))

    val basePotencialTacticaBundlePO = listadoTacticaBundle
      .where($"GAPPRECIOOPTIMO" =!= 0)
      .groupBy($"PERFILGP", $"CODTACTICA")
      .agg((sum($"GAPPRECIOOPTIMO") / countDistinct($"CODEBELISTA"))
        .as("GAPPRECIOOPTIMO_GP"))

    // Se actualiza a nivel de Táctica
    val listadoTacticaBundleAtualizado = {
      val lt = listadoTacticaBundle.alias("lt")
      val bp = basePotencialTacticaBundle.alias("bp")
      val bo = basePotencialTacticaBundlePO.alias("bo")

      lt.join(bp,
          $"bp.CODTACTICA" === $"lt.CODTACTICA" &&
            $"bp.PERFILGP" === $"lt.PERFILGP")
        .join(bo,
          $"bo.CODTACTICA" === $"lt.CODTACTICA" &&
            $"bo.PERFILGP" === $"lt.PERFILGP")
        .updateWith(Seq($"lt.*"), Map(
          "BRECHAVENTA" ->
            when($"lt.BRECHAVENTA" === 0, $"bp.VENTAPOTENCIALMINU6C_GP")
              .otherwise($"lt.BRECHAVENTA"),
          "FRECUENCIAU24C" ->
            when($"lt.FRECUENCIAU24C" === 0, $"bp.FRECUENCIAU24C_GP")
              .otherwise($"lt.FRECUENCIAU24C"),
          "RECENCIAU24C" ->
            when($"lt.RECENCIAU24C" === 0, $"bp.RECENCIAU24C_GP")
              .otherwise($"lt.RECENCIAU24C"),
          "GATILLADOR" ->
            when($"lt.GATILLADOR" === 0, $"bp.GATILLADOR_GP")
              .otherwise($"lt.GATILLADOR"),
          "GAPPRECIOOPTIMO" ->
            when($"lt.PRECIOOPTIMO" === 0, $"bo.GAPPRECIOOPTIMO_GP")
              .otherwise($"lt.GAPPRECIOOPTIMO")
        ))
    }.persist(StorageLevel.MEMORY_AND_DISK_SER)

    // Actualizo la tabla de Grupo Potencial
    val grupoPotencialBundleCompleto = {
      val gp = grupoPotencialBundle.alias("gp")
      val bp = basePotencialTacticaBundle.alias("bp")

      gp.join(bp, makeEquiJoin("gp", "bp", Seq("PERFILGP", "CODTACTICA")))
        .updateWith(Seq($"gp.*"), Map(
          "FRECUENCIAU24C" -> $"bp.FRECUENCIAU24C_GP",
          "RECENCIAU24C" -> $"bp.RECENCIAU24C_GP",
          "PROMVENTAPOTENCIALMINU6C" -> $"bp.VENTAPOTENCIALMINU6C_GP",
          "GATILLADOR" -> $"bp.GATILLADOR_GP"))
    }

    // partitionBy(
    //  "CODPAIS", "ANIOCAMPANAPROCESO", "ANIOCAMPANAEXPO",
    //  "TIPOARP", "TIPOPERSONALIZACION", "PERFIL", "TIPOTACTICA", "TIPOGP")

    (
      grupoPotencialCompleto, grupoPotencialBundleCompleto,
      listadoConsultoraCompleto, listadoTacticaBundleAtualizado
    )
  }

  private def updateListadoConsultora(input: DataFrame): DataFrame = {
    // Se actualiza la Brecha de Recompra Potencial = Recencia U24C
    // Ciclo Recompra Potencial
    val act1 = input
      .select("*")
      .withColumn("BRECHARECOMPRAPOTENCIAL",
        $"RECENCIAU24C" - $"CICLORECOMPRAPOTENCIAL")
      .alias("act1")

    val pc = productosCUCIndividual.alias("pc")
    val act2 = act1.join(pc, makeEquiJoin("act1", "pc", Seq("CODPRODUCTO", "CODTACTICA")))
      .updateWith(Seq($"act1.*"), Map(
        "GAPPRECIOOPTIMO" ->
          round($"pc.PRECIOOFERTA" - $"act1.PRECIOOPTIMO", 4)))
      .select("*")
      .alias("act2")

    // Cálculo de Brechas - GAP Sin Motor Canibalización
    // Si Venta es mayor a cero en las U24C
    act2
      // Condición 2 - Recompró
      // Condición 3  -- No figura en el documento
      .withColumn("GAP", when(
        $"act2.VENTAACUMU24C" > 0 && $"act2.FRECUENCIAU24C" > 1,
          when($"act2.ANTIGUEDAD" > 24, $"act2.VENTAACUMU6C" - $"act2.VENTAACUMU6C_AA")
            .when($"act2.ANTIGUEDAD" <= 24, $"act2.VENTAACUMU6C" - $"act2.VENTAACUMPU6C")
            .otherwise($"act2.GAP")))
      // Condición 1
      // Condición 2 - Recompró
      // Condición 3 - No figura en el documento
      // Condición 4
      .withColumn("BRECHAVENTA", when(
        $"act2.VENTAACUMU24C" <= 0, $"act2.VENTAPOTENCIALMINU6C"
      ).otherwise(
        when($"act2.FRECUENCIAU24C" >= 1, $"act2.VENTAPROMU24C")
          .otherwise($"act2.BRECHAVENTA")
      ))
  }

  private def updateListadoConsultoraBundle(input: DataFrame): DataFrame = {
    input
      .selectExpr(
        "*",
        "CASE WHEN BRECHARECOMPRAPOTENCIAL > 0 " +
          "THEN 1 ELSE FLAGBRP END AS FLAGBRP___2",
        "CASE WHEN VENTAACUMU6C - VENTAACUMU6C_AA <= 0 " +
          "THEN 1 ELSE FLAGVENTAU6CMENOSAA END AS FLAGVENTAU6CMENOSAA___2",
        "CASE WHEN VENTAACUMU6C - VENTAACUMPU6C <= 0 " +
          "THEN 1 ELSE FLAGVENTAU6CMENOSPP END AS FLAGVENTAU6CMENOSPP___2")
      .withColumn("FLAGBRP", $"FLAGBRP___2")
      .withColumn("FLAGVENTAU6CMENOSAA", $"FLAGVENTAU6CMENOSAA___2")
      .withColumn("FLAGVENTAU6CMENOSPP", $"FLAGVENTAU6CMENOSPP___2")
      .drop($"FLAGBRP___2")
      .drop($"FLAGVENTAU6CMENOSAA___2")
      .drop($"FLAGVENTAU6CMENOSPP___2")
  }

  private def createTacticaBundle(input: DataFrame): DataFrame = {
    val tacticaBundle = input
      .groupBy(
        $"CODEBELISTA",$"CODREGION",$"CODCOMPORTAMIENTOROLLING",
        $"ANTIGUEDAD",$"TIPOTACTICA",$"CODTACTICA",$"PERFILGP")
      .agg(
        lit(0d).as("GAPREGALO"),
        lit(0).as("NUMASPEOS"),
        lit(0d).as("GATILLADORREGALO"),
        lit(0d).as("FRECUENCIANOR"),
        lit(0d).as("RECENCIANOR"),
        lit(0d).as("BRECHAVENTANOR"),
        lit(0d).as("BRECHAVENTA_MCNOR"),
        lit(0d).as("GAPPRECIOOPTIMONOR"),
        lit(0d).as("GATILLADORNOR"),
        lit(0d).as("OPORTUNIDAD"),
        lit(0d).as("OPORTUNIDAD_MC"),
        lit(0d).as("OPORTUNIDADNOR"),
        lit(0d).as("OPORTUNIDAD_MCNOR"),
        lit(0d).as("SCORE"),
        lit(0d).as("SCORE_MC"),
        lit(0).as("UNIDADESTACTICA"),
        lit(0d).as("SCORE_UU"),
        lit(0d).as("SCORE_MC_UU"),
        lit("A").as("PERFILOFICIAL"),
        lit(0).as("FLAGSERECOMIENDA"),
        sum($"VENTAACUMU6C").as("VENTAACUMU6C"),
        sum($"VENTAACUMPU6C").as("VENTAACUMPU6C"),
        sum($"VENTAACUMU6C_AA").as("VENTAACUMU6C_AA"),
        sum($"VENTAACUMU24C").as("VENTAACUMU24C"),
        avg($"VENTAPROMU24C").as("VENTAPROMU24C"),
        avg($"FRECUENCIAU24C").as("FRECUENCIAU24C"),
        avg($"RECENCIAU24C").as("RECENCIAU24C"),
        when(avg($"RECENCIAU24C") === 0, 24)
          .otherwise(avg($"RECENCIAU24C")).as("RECENCIAU24C_GP"),
        avg($"CICLORECOMPRAPOTENCIAL").as("CICLORECOMPRAPOTENCIAL"),
        sum($"BRECHARECOMPRAPOTENCIAL").as("BRECHARECOMPRAPOTENCIAL"),
        avg($"VENTAPOTENCIALMINU6C").as("VENTAPOTENCIALMINU6C"),
        avg($"GAP").as("GAP"),
        avg($"VENTAPROMU24C").as("BRECHAVENTA"),
        avg($"BRECHAVENTA_MC").as("BRECHAVENTA_MC"),
        avg($"FLAGCOMPRA").as("FLAGCOMPRA"),
        sum($"PRECIOOPTIMO").as("PRECIOOPTIMO"),
        avg($"GAPPRECIOOPTIMO").as("GAPPRECIOOPTIMO"),
        sum($"FLAGBRP").as("FLAGBRP"),
        sum($"FLAGVENTAU6CMENOSAA").as("FLAGVENTAU6CMENOSAA"),
        sum($"FLAGVENTAU6CMENOSPP").as("FLAGVENTAU6CMENOSPP"),
        avg($"GATILLADOR").as("GATILLADOR"),
        max($"FLAGTOP").as("FLAGTOP")
      ).alias("tacticaBundle")

    // Cálculo de Brechas - GAP Sin Motor de Canibalización
    val poPrecioOptimo = {
      val tb = tacticaBundle.alias("tb")
      val pcb = productosCUCBundle.alias("pcb")

      tb.join(pcb, makeEquiJoin("tb", "pcb", Seq("CODTACTICA")))
        .groupBy($"tb.CODEBELISTA", $"tb.CODTACTICA")
        .agg(
          (sum($"pcb.PRECIOOFERTA") - sumDistinct($"tb.PRECIOOPTIMO"))
            .as("GAPPRECIOOPTIMO"))
    }

    // Base Potencial de Bundle
    // Se actualiza el GAP Precio Óptimo a nivel de Táctica
    val tb = tacticaBundle.alias("tb")
    val po = poPrecioOptimo.alias("po")

    tb.join(po, makeEquiJoin("tb", "po", Seq("CODTACTICA", "CODEBELISTA")))
      .updateWith(Seq($"tb.*"), Map(
        "GAPPRECIOOPTIMO" -> $"po.GAPPRECIOOPTIMO"))
  }

  private def montarListadoConsolidado(individual: DataFrame, bundle: DataFrame): DataFrame = {
    // Inserto las tácticas Bundle
    val listadoSelecionadoBundle = bundle.select(
      $"CODEBELISTA", $"CODREGION", $"CODCOMPORTAMIENTOROLLING",
      $"ANTIGUEDAD", $"TIPOTACTICA", $"CODTACTICA",
      $"VENTAACUMU6C", $"VENTAACUMPU6C", $"VENTAACUMU6C_AA",
      $"VENTAACUMU24C", $"VENTAPROMU24C", $"FRECUENCIAU24C",
      $"RECENCIAU24C", $"CICLORECOMPRAPOTENCIAL", $"BRECHARECOMPRAPOTENCIAL",
      $"VENTAPOTENCIALMINU6C", $"GAP", $"BRECHAVENTA",
      $"BRECHAVENTA_MC", $"FLAGCOMPRA", $"PRECIOOPTIMO",
      $"GAPPRECIOOPTIMO", $"GAPREGALO", $"NUMASPEOS",
      $"GATILLADOR", $"GATILLADORREGALO", $"FRECUENCIANOR",
      $"RECENCIANOR", $"BRECHAVENTANOR", $"BRECHAVENTA_MCNOR",
      $"GAPPRECIOOPTIMONOR", $"GATILLADORNOR", $"OPORTUNIDAD",
      $"OPORTUNIDAD_MC", $"OPORTUNIDADNOR", $"OPORTUNIDAD_MCNOR",
      $"SCORE", $"SCORE_MC", $"UNIDADESTACTICA",
      $"SCORE_UU", $"SCORE_MC_UU", $"PERFILOFICIAL",
      $"FLAGSERECOMIENDA", $"FLAGTOP"
    )

    val listadoSelecionadoIndividual = individual
      .groupBy(
        $"CODEBELISTA", $"CODREGION", $"CODCOMPORTAMIENTOROLLING",
        $"ANTIGUEDAD", $"TIPOTACTICA", $"CODTACTICA")
      .agg(
        sum($"VENTAACUMU6C").as("VENTAACUMU6C"),
        sum($"VENTAACUMPU6C").as("VENTAACUMPU6C"),
        sum($"VENTAACUMU6C_AA").as("VENTAACUMU6C_AA"),
        sum($"VENTAACUMU24C").as("VENTAACUMU24C"),
        avg($"VENTAPROMU24C").as("VENTAPROMU24C"),
        avg($"FRECUENCIAU24C").as("FRECUENCIAU24C"),
        avg($"RECENCIAU24C").as("RECENCIAU24C"),
        avg($"CICLORECOMPRAPOTENCIAL").as("CICLORECOMPRAPOTENCIAL"),
        sum($"BRECHARECOMPRAPOTENCIAL").as("BRECHARECOMPRAPOTENCIAL"),
        avg($"VENTAPOTENCIALMINU6C").as("VENTAPOTENCIALMINU6C"),
        avg($"GAP").as("GAP"),
        avg($"BRECHAVENTA").as("BRECHAVENTA"),
        avg($"BRECHAVENTA_MC").as("BRECHAVENTA_MC"),
        avg($"FLAGCOMPRA").as("FLAGCOMPRA"),
        sum($"PRECIOOPTIMO").as("PRECIOOPTIMO"),
        avg($"GAPPRECIOOPTIMO").as("GAPPRECIOOPTIMO"),
        lit(0d).as("GAPREGALO"),
        lit(0).as("NUMASPEOS"),
        avg($"GATILLADOR").as("GATILLADOR"),
        lit(0d).as("GATILLADORREGALO"),
        lit(0d).as("FRECUENCIANOR"),
        lit(0d).as("RECENCIANOR"),
        lit(0d).as("BRECHAVENTANOR"),
        lit(0d).as("BRECHAVENTA_MCNOR"),
        lit(0d).as("GAPPRECIOOPTIMONOR"),
        lit(0d).as("GATILLADORNOR"),
        lit(0d).as("OPORTUNIDAD"),
        lit(0d).as("OPORTUNIDAD_MC"),
        lit(0d).as("OPORTUNIDADNOR"),
        lit(0d).as("OPORTUNIDAD_MCNOR"),
        lit(0d).as("SCORE"),
        lit(0d).as("SCORE_MC"),
        lit(0).as("UNIDADESTACTICA"),
        lit(0d).as("SCORE_UU"),
        lit(0d).as("SCORE_MC_UU"),
        lit(0d).as("PERFILOFICIAL"),
        lit(0).as("FLAGSERECOMIENDA"),
        max($"FLAGTOP").as("FLAGTOP")
      )

    /** Regalos **/

    // El ARP tiene la funcionalidad de incluir los regalos en las variables GAP Precio:
    // GAP Precio = GAP Precio -
    // (Suma(Precio Normal de los Regalos)/Suma(Unidades de la oferta))
    val temporalRegalo = {
      val dm = dwhDMatrizCampana.alias("dm")
      val to = dwhDTipoOferta.alias("to")
      val pr = productosRegalo.alias("pr")

      // Obtengo el mayor precio normal de cada producto SAP
      val t1 = dm.join(pr, $"pr.CODSAP" === $"dm.CODSAP", "inner")
        .join(to,
          $"to.CODTIPOOFERTA" === $"dm.CODTIPOOFERTA"
            && $"to.CODPAIS" === params.codPais(),
          "inner")
        .where($"dm.PRECIONORMALMN" > 0
          && $"dm.CODPAIS" === params.codPais())
        .groupBy($"pr.CODTACTICA", $"pr.CODPRODUCTO", $"pr.CODSAP", $"pr.UNIDADES")
        .agg(max($"dm.PRECIONORMALMN").as("PRECIONORMALMN"))

      // Se multiplican las unidades por el precio unitario de cada Producto CUC
      val t2 = t1.select("*")
        .groupBy($"CODTACTICA", $"CODPRODUCTO", $"UNIDADES")
        .agg(
          round(avg($"PRECIONORMALMN"), 2).as("PRECIOOFERTA"),
          ($"UNIDADES" * round(avg($"PRECIONORMALMN"), 2))
            .as("PRECIOOFERTATOTAL")
        ).alias("t2")

      val ut = unidadesTactica.alias("ut")

      t2.join(ut, makeEquiJoin("t2", "ut", Seq("CODTACTICA")))
        .groupBy($"t2.CODTACTICA")
        .agg(
          (sum($"t2.PRECIOOFERTATOTAL") / sum($"ut.TOTALUNIDADES"))
            .as("GAPRegalo")
        )
    }

    // Se obtiene el total de pedidos con regalo por cada consultora de la base
    val fvtaproebecamRegalos = {
      val fv = dwhFVtaProEbeCam.alias("fv")
      val to = dwhDTipoOferta.alias("to")
      val bc = baseConsultoras.alias("bc")

      fv.join(to,
        $"to.CODTIPOOFERTA" === $"fv.CODTIPOOFERTA"
          && $"to.CODPAIS" === params.codPais(),
        "inner")
        .join(bc, makeEquiJoin("fv", "bc", Seq("CODEBELISTA")))
        .where(
          $"fv.ANIOCAMPANA".between(acInicio24UC.toString, acProcesso.toString)
            && $"fv.ANIOCAMPANA" === $"fv.ANIOCAMPANAREF"
            && $"to.CODTIPOPROFIT" === "01"
            && $"fv.REALVTAMNNETO" === 0
            && $"fv.REALUUVENDIDAS" >= 1
            && $"fv.CODPAIS" === params.codPais())
        .groupBy($"fv.CODEBELISTA")
        .agg(countDistinct($"fv.ANIOCAMPANA").as("TOTALPEDIDOS"))
    }

    val totalPedidosConsultora = baseEstadoConsultoras
      .where($"FLAGPASOPEDIDO" === 1)
      .groupBy($"CODEBELISTA")
      .agg(countDistinct($"ANIOCAMPANA").as("TOTALCAMPANIAS"))

    // Se almacena el logaritmo log (#Pedidos con regalos/#Campañas con Pedidos)
    val gatilladorRegalos = {
      val fv = fvtaproebecamRegalos.alias("fv")
      val tp = totalPedidosConsultora.alias("tp")

      fv.join(tp, makeEquiJoin("fv", "tp", Seq("CODEBELISTA")))
        .select(
          $"fv.CODEBELISTA",
          ($"fv.TOTALPEDIDOS".cast("double") / tp("TOTALCAMPANIAS"))
            .as("GATILLADOR")
        )
    }

    val tacticasConRegalo = productosRegalo.select($"CODTACTICA").distinct()

    // Se actualiza el GAP Regalos y el Gatillador Regalos en la tabla final
    val listadoConsultoraTotal = {
      var lct = listadoSelecionadoBundle
        .unionByName(listadoSelecionadoIndividual)
        .select("*")
        .alias("lct")

      val tr = temporalRegalo.alias("tr")
      val gr = gatilladorRegalos.alias("gr")
      val tt = tacticasConRegalo.alias("tt")
      val ut = unidadesTactica.alias("ut")

      lct = lct.join(tr, makeEquiJoin("lct", "tr", Seq("CODTACTICA")))
        .updateWith(Seq($"lct.*"), Map(
          "GAPREGALO" -> $"tr.GAPREGALO")).alias("lct")

      lct = lct.join(gr, makeEquiJoin("lct", "gr", Seq("CODEBELISTA")))
        .join(tt, $"lct.CODTACTICA" === $"tt.CODTACTICA", "left")
        .updateWith(Seq($"lct.*"), Map(
          "GATILLADORREGALO" ->
            when($"tt.CODTACTICA".isNotNull, sqrt($"gr.GATILLADOR") / 10)
              .otherwise($"lct.GATILLADORREGALO"))).alias("lct")

      lct = lct
        .withColumn("GAPPRECIOOPTIMO", $"GAPPRECIOOPTIMO" - $"GAPREGALO")
        .withColumn("GATILLADOR", $"GATILLADOR" + $"GATILLADORREGALO")
        .alias("lct")

      lct = lct.join(ut, makeEquiJoin("lct", "ut", Seq("CODTACTICA")))
        .updateWith(Seq($"lct.*"), Map(
          "UNIDADESTACTICA" -> $"ut.TOTALUNIDADES"))

      lct.alias("lct")
    }

    /** Carga tabla ListadoVariablesRFM **/
    val listadoVariablesRFM = listadoConsultoraTotal.select(
      $"CODEBELISTA", $"CODREGION", $"CODCOMPORTAMIENTOROLLING",
      $"ANTIGUEDAD", $"TIPOTACTICA", $"CODTACTICA",
      $"VENTAACUMU6C", $"VENTAACUMPU6C", $"VENTAACUMU6C_AA",
      $"VENTAACUMU24C", $"VENTAPROMU24C", $"FRECUENCIAU24C",
      $"RECENCIAU24C", $"CICLORECOMPRAPOTENCIAL", $"BRECHARECOMPRAPOTENCIAL",
      $"VENTAPOTENCIALMINU6C", $"GAP", $"BRECHAVENTA",
      $"BRECHAVENTA_MC", $"FLAGCOMPRA", $"PRECIOOPTIMO",
      $"GAPPRECIOOPTIMO", $"GAPREGALO", $"NUMASPEOS",
      $"GATILLADOR", $"GATILLADORREGALO", $"FRECUENCIANOR",
      $"RECENCIANOR", $"BRECHAVENTANOR", $"BRECHAVENTA_MCNOR",
      $"GAPPRECIOOPTIMONOR", $"GATILLADORNOR", $"OPORTUNIDAD",
      $"OPORTUNIDAD_MC", $"OPORTUNIDADNOR", $"OPORTUNIDAD_MCNOR",
      $"SCORE", $"SCORE_MC", $"UNIDADESTACTICA",
      $"SCORE_UU", $"SCORE_MC_UU", $"PERFILOFICIAL",
      $"FLAGSERECOMIENDA", $"FLAGTOP",
      lit(params.codPais()).as("CODPAIS"),
      lit(params.anioCampanaProceso()).as("ANIOCAMPANAPROCESO"),
      lit(params.anioCampanaExpo()).as("ANIOCAMPANAEXPO"),
      lit(params.tipoARP()).as("TIPOARP"),
      lit(params.tipoPersonalizacion()).as("TIPOPERSONALIZACION"),
      lit(params.perfil()).as("PERFIL")
    )

    // partitionBy(
    //  "CODPAIS", "ANIOCAMPANAPROCESO", "ANIOCAMPANAEXPO",
    //  "TIPOARP", "TIPOPERSONALIZACION", "PERFIL")
    listadoVariablesRFM
  }

  private val acProcesso = AnioCampana.parse(params.anioCampanaProceso())
  private val acInicio6UC = acProcesso - 5
  private val acInicio24UC = acProcesso - 23

  /** 1. Estabelecidas **/

  // Leo la Base de Consultoras
  // Extraigo el Perfil para Grupo Potencial
  private val infoConsultora = {
    val bc = baseConsultorasReal.alias("bc")
    val po = mdlPerfilOutput.alias("po")

    bc.join(po,
      $"bc.CODEBELISTA" === $"po.CODEBELISTA" &&
        $"bc.CODPAIS" === $"po.CODPAIS", "left")
      .where(
        $"bc.ANIOCAMPANAPROCESO" === acProcesso.toString
          && $"bc.CODPAIS" === params.codPais()
          && $"bc.TIPOARP" === "E"
          && $"bc.PERFIL" === params.perfil())
      .select($"bc.*", $"po.Perfil".as("PerfilGP"))
  }

  private val baseConsultoras = infoConsultora.select($"CODEBELISTA").distinct()

  // Se obtienen los productos a nivel CUC
  private val productosCUC = {
    val lp = listadoProductos.alias("lp")
    val dp = dwhDProducto.alias("dp")

    lp.join(dp, $"lp.CODCUC" === $"dp.CUC", "inner")
      .where($"dp.DESCRIPCUC".isNotNull)
      .groupBy(
        $"dp.CUC".as("CODPRODUCTO"),
        $"TIPOTACTICA", $"CODTACTICA", $"CODVENTA", $"UNIDADES",
        $"lp.PRECIOOFERTA".as("PRECIOOFERTA"),
        $"INDICADORPADRE", $"FLAGTOP")
      .agg(
        lit("").cast("string").as("CODSAP"),
        max($"dp.DESCRIPCUC".as("DESPRODUCTO")),
        max($"dp.DESMARCA").as("DESMARCA"),
        max($"dp.DESCATEGORIA".as("DESCATEGORIA")),
        max($"LIMUNIDADES").as("LIMUNIDADES"),
        max($"FLAGULTMINUTO").as("FLAGULTMINUTO"))
  }

  // Se obtienen los productos regalos a nivel de Item (CODSAP)
  private val productosRegalo = {
    val lr = listadoRegalos.alias("lr")
    val dp = dwhDProducto.alias("dp")

    lr.join(dp, $"lr.CODCUC" === $"dp.CUC", "inner")
      .select(
        $"CODTACTICA",
        $"TIPOTACTICA",
        $"dp.CUC".as("CODPRODUCTO"),
        $"UNIDADES",
        $"dp.DESCRIPCUC".as("DESPRODUCTO"),
        $"lr.PRECIOOFERTA".as("PRECIOOFERTA"),
        $"CODMARCA", $"dp.CODSAP".as("CODSAP"))
  }

  // Se guardan las unidades por Táctica
  private val unidadesTactica = productosCUC
    .groupBy($"CODTACTICA")
    .agg(sum($"UNIDADES").as("TOTALUNIDADES"))

  /** 1.1. Individual **/

  private val listadoConsultora = {
    val lvi = listadoVariablesIndividual.alias("lvi")
    val ic = infoConsultora.alias("ic")

    lvi.join(ic, makeEquiJoin("lvi", "ic", Seq("CODEBELISTA")))
      .select($"lvi.*", $"ic.PERFILGP")
  }

  // Se obtienen los productos a nivel de Item (CODSAP)
  private val productos = {
    val lp = listadoProductos.alias("lp")
    val dp = dwhDProducto.alias("dp")

    lp.join(dp, $"lp.CODCUC" === $"dp.CUC", "inner")
      .select(
        $"TIPOTACTICA", $"CODTACTICA",
        $"dp.CUC".as("CODPRODUCTO"),
        $"UNIDADES",
        $"dp.DESCRIPCUC".as("DESPRODUCTO"),
        $"lp.PRECIOOFERTA".as("PRECIOOFERTA"),
        $"CODMARCA", $"dp.CODSAP".as("CODSAP"))
  }

  // A nivel de Item (CODSAP)
  private val productosIndividual = {
    val lp = listadoProductos.alias("lp")
    val dp = dwhDProducto.alias("dp")

    lp.join(dp,
      $"lp.CODCUC" === $"dp.CUC"
        && $"TIPOTACTICA" === "Individual",
      "inner"
    ).select(
      $"TIPOTACTICA", $"CODTACTICA",
      $"dp.CUC".as("CODPRODUCTO"),
      $"UNIDADES",
      $"dp.DESCRIPCUC".as("DESPRODUCTO"),
      $"lp.PRECIOOFERTA".as("PRECIOOFERTA"),
      $"CODMARCA", $"dp.CODSAP".as("CODSAP"),
      $"DESMARCA", $"DESCATEGORIA", $"FLAGTOP"
    )
  }

  // A nivel CUC
  private val productosCUCIndividual = productosIndividual
    .where($"DESPRODUCTO".isNotNull)
    .groupBy(
      $"CODTACTICA",
      $"TIPOTACTICA",
      $"CODPRODUCTO",
      $"UNIDADES",
      $"PRECIOOFERTA",
      $"FLAGTOP"
    ).agg(max($"DESPRODUCTO"))

  // Se crea la tabla temporal donde se guardan
  // la venta de las últimas 24 campañas
  private val fvtaproebecam24AC = {
    val fv = dwhFVtaProEbeCam.alias("fv")
    val bc = baseConsultoras.alias("bc")

    fv.join(bc, makeEquiJoin("fv", "bc", Seq("CODEBELISTA")))
      .where(
        $"fv.ANIOCAMPANA".between(acInicio24UC.toString, acProcesso.toString)
          && $"fv.CODPAIS" === params.codPais()
      ).select(
      $"fv.ANIOCAMPANA", $"fv.CODEBELISTA", $"fv.CODSAP",
      $"fv.CODTIPOOFERTA", $"fv.CODTERRITORIO", $"fv.NROFACTURA",
      $"fv.CODVENTA", $"fv.ANIOCAMPANAREF", $"fv.REALUUVENDIDAS", $"fv.REALVTAMNNETO",
      $"fv.REALVTAMNFACTURA", $"fv.REALVTAMNCATALOGO"
    )
  }

  private val fstaebecam24AC = {
    dwhFStaEbeCam
      .where(
        $"ANIOCAMPANA".between(acInicio24UC.toString, acProcesso.toString)
          && $"CODPAIS" === params.codPais()
      ).select(
      $"CODEBELISTA", $"ANIOCAMPANA", $"FLAGPASOPEDIDO", $"FLAGACTIVA",
      $"CODCOMPORTAMIENTOROLLING", $"CODIGOFACTURAINTERNET"
    )
  }

  // Obtengo el estado de las consultoras que conforman
  // la base en las últimas 24 campañas
  private val baseEstadoConsultoras = {
    val bc = baseConsultoras.alias("bc")
    val fs = fstaebecam24AC.alias("fs")

    bc.join(fs, makeEquiJoin("bc", "fs", Seq("CODEBELISTA")))
      .select(
        $"bc.CODEBELISTA", $"fs.ANIOCAMPANA",
        $"fs.FLAGPASOPEDIDO", $"fs.FLAGACTIVA"
      )
  }

  private val totalConsultorasRegion = infoConsultora
    .groupBy($"CODREGION", $"CODCOMPORTAMIENTOROLLING")
    .agg(countDistinct($"CODEBELISTA").as("TOTALCONSULTORAS"))

  private val totalConsultorasGP = infoConsultora
    .groupBy($"PERFILGP")
    .agg(countDistinct($"CODEBELISTA").as("TOTALCONSULTORAS"))

  private val fvtaproebecamInt = {
    val fv = fvtaproebecam24AC.alias("fv")
    val dp = productos.alias("dp")
    val dt = dwhDTipoOferta.alias("dt")

    fv.join(dp, $"dp.CODSAP" === $"fv.CODSAP", "inner")
      .join(dt, $"dt.CODTIPOOFERTA" === $"fv.CODTIPOOFERTA", "inner")
      .where(
        $"fv.ANIOCAMPANA".between(acInicio24UC.toString, acProcesso.toString)
          && $"fv.ANIOCAMPANA" === $"fv.ANIOCAMPANAREF"
          && $"dt.CODTIPOPROFIT" === "01"
          && not($"dt.CODTIPOOFERTA".isin("030", "040", "051"))
          && $"fv.REALVTAMNNETO" > 0
      ).select(
      $"fv.ANIOCAMPANA", $"fv.CODEBELISTA", $"fv.CODSAP", $"fv.CODTIPOOFERTA",
      $"fv.CODTERRITORIO", $"fv.CODVENTA", $"fv.ANIOCAMPANAREF",
      $"fv.REALUUVENDIDAS", $"fv.REALVTAMNNETO", $"fv.REALVTAMNFACTURA",
      $"fv.REALVTAMNCATALOGO", $"fv.NROFACTURA"
    )
  }

  private val ventasProductos24AC = {
    val fv = fvtaproebecamInt.alias("fv")
    val pi = productosIndividual.alias("pi")

    fv.join(pi, makeEquiJoin("fv", "pi", Seq("CODSAP")))
      .select($"fv.*", $"pi.CODTACTICA", $"pi.CODPRODUCTO")
  }

  private val ventasProductos6AC = ventasProductos24AC.where(
    $"ANIOCAMPANA".between(acInicio6UC.toString, acProcesso.toString)
  ).select("*")

  /** Precio Óptimo **/

  //Obtengo los productos vendidos en las últimas 24 campañas
  private val poVenta = ventasProductos24AC.select(
    $"ANIOCAMPANA", $"ANIOCAMPANAREF", $"CODEBELISTA",
    $"CODPRODUCTO", $"CODTACTICA", $"CODSAP", $"CODTIPOOFERTA", $"CODVENTA",
    $"REALVTAMNNETO", $"REALVTAMNCATALOGO", $"REALUUVENDIDAS"
  )

  private val poProdXConsultora = poVenta.select(
    $"CODEBELISTA", $"CODTACTICA", $"CODPRODUCTO", $"CODSAP"
  ).distinct()

  // Obtengo la matriz de venta de los productos propuestos
  private val poCatalogo = {
    val dm = dwhDMatrizCampana.alias("dm")
    val pi = productosIndividual.alias("pi")
    val dt = dwhDTipoOferta.alias("dt")

    dm.join(pi,
      $"pi.CODSAP" === $"dm.CODSAP",
      "inner"
    ).join(dt,
      $"dt.CODTIPOOFERTA" === $"dm.CODTIPOOFERTA",
      "inner"
    ).where(
      $"ANIOCAMPANA".between(acInicio24UC.toString, acProcesso.toString)
        && $"dm.PRECIOOFERTA" > 0
        && $"CODTIPOPROFIT" === "01"
        && not($"dt.CODTIPOOFERTA".isin("030", "040", "051"))
        && $"dm.VEHICULOVENTA".isin("REVISTA", "CATÁLOGO", "CATALOGO")
        && $"dm.CODPAIS" === params.codPais()
    ).select(
      $"pi.CODTACTICA", $"pi.CODPRODUCTO", $"dm.*"
    )
  }

  // Se arma la tabla de Precio Óptimo
  // Se cuentan las campañas en las que se muestra el producto
  // a dicho precio y la consultora ha pasado pedido
  private var precioOptimo = {
    val ppc = poProdXConsultora.alias("ppc")
    val pco = poCatalogo.alias("pco")
    val be = baseEstadoConsultoras.alias("be")

    ppc.join(pco,
      $"pco.CODSAP" === $"ppc.CODSAP",
      "inner"
    ).join(be,
      $"be.CODEBELISTA" === $"ppc.CODEBELISTA"
        && $"be.ANIOCAMPANA" === $"pco.ANIOCAMPANA",
      "inner"
    ).where(
      $"be.FLAGPASOPEDIDO" === 1
    ).groupBy(
      $"ppc.CODEBELISTA", $"ppc.CODTACTICA", $"ppc.CODPRODUCTO", $"PRECIOOFERTA"
    ).agg(
      lit(0).as("PEDIDOSPUROS"),
      lit(0.0).cast("double").as("PROBABILIDAD"),
      countDistinct($"pco.ANIOCAMPANA").as("NUMASPEOS")
    )
  }

  // Pedidos puros
  private val poPedidosPuros = {
    val pv = poVenta.alias("pv")
    val pco = poCatalogo.alias("pco")

    pv.join(pco, makeEquiJoin("pv", "pco", Seq("CODSAP", "ANIOCAMPANA", "CODTIPOOFERTA", "CODVENTA")))
      .groupBy(
        $"pv.CODEBELISTA", $"pv.CODTACTICA", $"pv.CODPRODUCTO", $"PRECIOOFERTA"
      ).agg(
        countDistinct($"pco.ANIOCAMPANA").as("PEDIDOSPUROS")
      )
  }

  precioOptimo = precioOptimo
    .updateFrom(poPedidosPuros,
      Seq("CODEBELISTA", "CODTACTICA", "CODPRODUCTO", "PRECIOOFERTA"),
      "PEDIDOSPUROS")
    .withColumn("PROBABILIDAD",
      $"PEDIDOSPUROS".cast("double") / $"NUMASPEOS")

  // Ordena por probabilidad y luego por número de pedido puro
  private val precioOptimoFinal = {
    val po = precioOptimo.alias("po")
    val window = Window.partitionBy(
      $"po.CODEBELISTA", $"po.CODTACTICA", $"po.CODPRODUCTO", $"po.PRECIOOFERTA"
    ).orderBy($"po.PROBABILIDAD")

    po.select(
      $"po.*",
      row_number().over(window).as("POSICION")
    ).orderBy(
      $"po.CODEBELISTA", $"po.CODTACTICA", $"po.CODPRODUCTO", $"po.PRECIOOFERTA"
    )
  }

  private val precioOptimoMinimo = precioOptimo
    .groupBy($"CODTACTICA", $"CODPRODUCTO")
    .agg(min($"PRECIOOFERTA").as("PRECIOMINIMO"))

  private val listadoConsultoraBundle = {
    val lvb = listadoVariablesBundle.alias("lvb")
    val ic = infoConsultora.alias("ic")

    lvb.join(ic, makeEquiJoin("lvb", "ic", Seq("CODEBELISTA")))
      .select($"lvb.*", $"ic.PERFILGP")
  }

  private val productosBundle = {
    val lcb = listadoConsultoraBundle.alias("lcb")
    val dp = dwhDProducto.alias("dp")

    lcb
      .join(dp,
        $"dp.CUC" === $"lcb.CODPRODUCTO"
        && $"TIPOTACTICA" === "Bundle",
        "inner")
      .select(
        $"CODTACTICA", $"TIPOTACTICA", $"dp.CUC".as("CODPRODUCTO"),
        $"lcb.UnidadesTactica".as("UNIDADES"), $"dp.DESCRIPCUC".as("DESPRODUCTO"),
        $"lcb.PRECIOOPTIMO".as("PRECIOOFERTA"), $"CODMARCA", $"CODSAP", $"DESMARCA",
        $"DESCATEGORIA", $"FLAGTOP")
  }

  private val productosCUCBundle = productosBundle
    .where($"DESPRODUCTO".isNotNull)
    .groupBy(
      $"CODTACTICA", $"TIPOTACTICA", $"CODPRODUCTO",
      $"UNIDADES", $"PRECIOOFERTA", $"FLAGTOP")
    .agg(max($"DESPRODUCTO"))
}
