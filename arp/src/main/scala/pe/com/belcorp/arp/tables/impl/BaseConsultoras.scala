package pe.com.belcorp.arp.tables.impl

import org.apache.spark.sql.functions._
import org.apache.spark.sql.{DataFrame, SparkSession}
import pe.com.belcorp.arp.tables.Table
import pe.com.belcorp.arp.utils.AnioCampana.{Spark => acUDF}
import pe.com.belcorp.arp.utils.Extensions._
import pe.com.belcorp.arp.utils.{AnioCampana, ProcessParams}

class BaseConsultoras(params: ProcessParams,
  dwhDebelista: DataFrame, dwhDGeografiaCampana: DataFrame,
  dwhDTipoOferta: DataFrame, dwhFStaEbeCam: DataFrame,
  dwhFVtaProEbeCam: DataFrame, mdlPerfilOutput: DataFrame) extends Table {

  override def get(spark: SparkSession): DataFrame = {
    params.perfil() match {
      case "X" => createUnprofiledConsultantsBase(spark)
      case _ => createProfiledConsultantsBase(spark)
    }
  }

  private val acProceso = AnioCampana.parse(params.anioCampanaProceso())


  private def createUnprofiledConsultantsBase(spark: SparkSession): DataFrame = {
    /** 1. Sin Perfil **/

    /** 1.1. Base de Consultoras Nuevas **/
    // Consultoras Nuevas en su 1er, 2do, 3er y 4to pedido
    val baseConsultorasNuevas = {
      val fs = dwhFStaEbeCam.alias("fs")
      val de = dwhDebelista.alias("de")

      var base = fs.join(de,
        makeEquiJoin("fs", "de", Seq("CodEbelista", "CodPais")))
        .where($"fs.CodPais" === params.codPais() &&
          $"de.AnioCampanaIngreso".between(
            (acProceso - 3).toString, acProceso.toString
          ))
        .select($"fs.CODEBELISTA")
        .distinct()

      if (Seq("ODD", "SR") contains params.tipoPersonalizacion()) {
        base = base
          .join(mdlPerfilOutput, Seq("CodEbelista"), "left_anti")
          .select($"CodEbelista")
      }

      base
    }

    /** 1.2. Base de Consultoras Establecidas **/
    val baseConsultorasEstabelecidas = {
      val fv = dwhFVtaProEbeCam.alias("fv")
      val dt = dwhDTipoOferta.alias("dt")

      // Base de Consultoras: Aquellas con pedidos en las 3 últimas campanas
      val tmpBaseConsultoras1 = fv
        .join(dt, makeEquiJoin("fv", "dt", Seq("CodTipoOferta", "CodPais")))
        .where($"fv.CodPais" === params.codPais() &&
          $"fv.AnioCampana".between(
            (acProceso - 2).toString, acProceso.toString) &&
          $"dt.CodTipoProfit" === "01" &&
          $"fv.AnioCampana" === $"fv.AnioCampanaRef")
        .groupBy($"fv.CodEbelista")
        .agg(sum($"fv.RealVTAMNNeto").as("ValorPedidos"))
        .select("*")
        .where($"ValorPedidos" > 0)
        .select($"CodEbelista")

      val fs = dwhFStaEbeCam.alias("fs")
      val de = dwhDebelista.alias("de")

      // Consultoras Nuevas en su 5to y 6to pedido
      val tmpBaseConsultoras2 = fs
        .join(de, makeEquiJoin("fs", "de", Seq("CodEbelista", "CodPais")))
        .where($"fs.CodPais" === params.codPais() &&
          $"de.AnioCampanaIngreso".between(
            (acProceso - 5).toString, (acProceso - 4).toString))
        .select($"fs.CodEbelista")

      // Se agregan las consultoras nuevas en su 5to y 6to pedido a las establecidas
      val tmpBaseConsultoras3 = tmpBaseConsultoras1.union(tmpBaseConsultoras2)

      tmpBaseConsultoras3
        .join(baseConsultorasNuevas, Seq("CodEbelista"), "left_anti")
        .select($"CodEbelista")
        .distinct()
    }

    val baseConsultorasFinal = {
      val nuevas = baseConsultorasNuevas.select($"CodEbelista")
        .withColumn("TipoBase", lit("N"))
      val estabelecidas = baseConsultorasEstabelecidas.select($"CodEbelista")
        .withColumn("TipoBase", lit("E"))

      nuevas.unionByName(estabelecidas)
    }

    // Se obtiene la Información Actual de las Consultoras, Región, Segmento
    val de = dwhDebelista.alias("de")
    val fs = dwhFStaEbeCam.alias("fs")
    val dg = dwhDGeografiaCampana.alias("dg")
    val bc = baseConsultorasFinal.alias("bcf")

    // infoConsultoras
    de.join(bc,
        makeEquiJoin("de", "bcf", Seq("CodEbelista")))
      .join(fs, makeEquiJoin("de", "fs", Seq("CodEbelista", "CodPais")))
      .join(dg, makeEquiJoin("fs", "dg",
        Seq("CodTerritorio", "CodPais", "AnioCampana")))
      .where(
        $"de.CodPais" === params.codPais() &&
        $"fs.AnioCampana" === acProceso.toString)
      .select(
        $"de.CodPais", $"de.CodEbelista", $"dg.CodRegion",
        $"fs.CodComportamientoRolling", $"dg.DesRegion",
        $"dg.CodZona", $"dg.DesZona",
        lit(acProceso.toString).as("AnioCampanaProceso"),
        lit(params.perfil()).as("Perfil"),
        (acUDF.delta(lit(acProceso.toString), $"de.AnioCampanaIngreso") + 1)
          .as("Antiguedad"), $"bcf.TipoBase".as("TipoARP"))
  }

  /** 2. Con Perfil **/
  private def createProfiledConsultantsBase(spark: SparkSession): DataFrame = {
    val baseConsultorasFinal = mdlPerfilOutput
      .where($"CodPais" === params.codPais() &&
        $"AnioCampanaProceso" === acProceso.toString &&
        $"Perfil" === params.perfil().toInt)
      .select($"CodEbelista")
      .withColumn("TipoBase", lit("E"))

    // Se obtiene la Información Actual de las Consultoras, Región, Segmento
    val de = dwhDebelista.alias("de")
    val fs = dwhFStaEbeCam.alias("fs")
    val dg = dwhDGeografiaCampana.alias("dg")
    val bc = baseConsultorasFinal.alias("bcf")

    // infoConsultoras
    de.join(bc,
      makeEquiJoin("de", "bcf", Seq("CodEbelista")))
      .join(fs, makeEquiJoin("de", "fs", Seq("CodEbelista", "CodPais")))
      .join(dg, makeEquiJoin("fs", "dg",
        Seq("CodTerritorio", "CodPais", "AnioCampana")))
      .where(
        $"de.CodPais" === params.codPais() &&
          $"fs.AnioCampana" === acProceso.toString)
      .select(
        $"de.CodPais", $"de.CodEbelista", $"dg.CodRegion",
        $"fs.CodComportamientoRolling", $"dg.DesRegion",
        $"dg.CodZona", $"dg.DesZona",
        lit(acProceso.toString).as("AnioCampanaProceso"),
        lit(params.perfil()).as("Perfil"),
        (acUDF.delta(lit(acProceso.toString), $"de.AnioCampanaIngreso") + 1)
          .as("Antiguedad"), $"bcf.TipoBase".as("TipoARP"))
  }
}