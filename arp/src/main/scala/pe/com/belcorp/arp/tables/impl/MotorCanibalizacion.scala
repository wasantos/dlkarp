package pe.com.belcorp.arp.tables.impl

import org.apache.spark.sql.functions._
import org.apache.spark.sql.{DataFrame, SparkSession}
import pe.com.belcorp.arp.tables.Constants
import pe.com.belcorp.arp.utils.Extensions._
import pe.com.belcorp.arp.utils.{AnioCampana, ProcessParams}

class MotorCanibalizacion(params: ProcessParams, arpListadoVariablesRFM_tb: DataFrame, mdlPerfilOutput: DataFrame) {

  val acProceso: AnioCampana = AnioCampana.parse(params.anioCampanaProceso())

  type Results = (DataFrame, DataFrame)

  def get(spark: SparkSession): Results = {
    if (params.flagMC().toInt == Constants.CARGA_PERSONALIZACION) {
      (insertArpListadoVariablesRFM(), noUsarMotorCanibalizacion)
    } else {
      (insertArpListadoVariablesRFM(), usarMotorCanibalizacion)
    }
  }

  /** Establecidas **/
  //#ListadoConsultoraTotal
  def listadoConsultoraTotal: DataFrame = {
    arpListadoVariablesRFM_tb.alias("lvr")
      .join(mdlPerfilOutput.alias("mpo"),
        $"lvr.CODEBELISTA" === $"mpo.CODEBELISTA" &&
        $"mpo.CODPAIS" === params.codPais() &&
        $"mpo.ANIOCAMPANAPROCESO" === acProceso.toString, "left")
      .where($"lvr.ANIOCAMPANAPROCESO" === acProceso.toString &&
        $"lvr.ANIOCAMPANAEXPO" === params.anioCampanaExpo() &&
        $"lvr.CODPAIS" === params.codPais() &&
        $"lvr.TIPOARP" === params.tipoARP() &&
        $"lvr.TIPOPERSONALIZACION" === params.tipoPersonalizacion() &&
        $"lvr.PERFIL" === params.perfil())
      .select($"lvr.*", $"mpo.PERFIL".as("PERFILGP"))
  }

  //#ListadoConsultoraIndividual
  def listadoConsultoraIndividual: DataFrame = listadoConsultoraTotal.where($"TIPOTACTICA" === "Individual")

  //#ListadoConsultoraBundle
  def listadoConsultoraBundle: DataFrame = listadoConsultoraTotal.where($"TIPOTACTICA" === "Bundle")

  /** 1. Táctica Individual **/

  /** 1.1. Cálculo de Brechas - GAP Normal **/

  /** 1.2. Cálculo de Brechas - GAP Motor Canibalización **/

  //#ListadoConsultoraIndividual
  def updateListadoConsultoraIndividual(): DataFrame = {

    //Si Venta es mayor a cero en las U24C
    //Individual
    val condicion = when($"VentaAcumU24C" <= 0, $"VentaPotencialMinU6C")
      .when($"FrecuenciaU24C" === 1,
        when($"BrechaRecompraPotencial" > 0, $"VentaAcumU24C").otherwise(lit(-1)))
      .when($"FrecuenciaU24C" > 1 && $"Antiguedad" > 24,
        when($"VentaAcumU6C" - $"VentaAcumU6C_AA" <= 0, $"VentaAcumU24C").otherwise(lit(-1)))
      .when($"Antiguedad" <= 24 && $"VentaAcumU6C" - $"VentaAcumU6C" <= 0, $"VentaPromU24C").otherwise(lit(-1))

    listadoConsultoraIndividual.withColumn("BrechaVenta_MC", condicion)
  }

  /** 2. Táctica Bundle **/

  /** 2.1. Cálculo de Brechas - GAP Normal **/

  /** 2.2. Cálculo de Brechas - GAP Motor Canibalización **/

  //#ListadoConsultoraBundle
  def updateListadoConsultoraBundle(): DataFrame = {

    //Si Venta es mayor a cero en las U24C
    //Bundle
    val condicion = when($"FrecuenciaU24C" === 1,
      when($"BrechaRecompraPotencial" > 0, $"VentaAcumU24C").otherwise(lit(-1)))
      .when($"FrecuenciaU24C" > 1 && $"Antiguedad" > 24,
        when($"VentaAcumU6C" - $"VentaAcumU6C_AA" <= 0, $"VentaAcumU24C").otherwise(lit(-1)))
      .when($"Antiguedad" <= 24 && $"VentaAcumU6C" - $"VentaAcumU6C" <= 0, $"VentaPromU24C").otherwise(lit(-1))

    listadoConsultoraBundle.withColumn("BrechaVenta_MC", condicion)

  }

  //#ListadoConsultoraBundle
  //Se actualiza la Brecha Venta con Motor de Canibalización a nivel de Táctica
  def basePotencialTacticaBundle: DataFrame = {

    val bundle = updateListadoConsultoraBundle().alias("bundle")
    val tatcBundle = bundle.groupBy($"CodRegion", $"CodComportamientoRolling", $"CodTactica")
      .agg((sum($"VentaPotencialMinU6C") / countDistinct($"CODEbelista")).alias("VentaPotencialMinU6C_GP")).alias("tatcBundle")

    bundle
      .join(tatcBundle, $"bundle.CodRegion" === $"tatcBundle.CodRegion" &&
        $"bundle.CodComportamientoRolling" === $"tatcBundle.CodComportamientoRolling" &&
        $"bundle.CodTactica" === $"tatcBundle.CodTactica", joinType = "Inner")
      .select($"bundle.*", $"tatcBundle.VentaPotencialMinU6C_GP")
      .withColumn("BrechaVenta_MC",
        when($"BrechaVenta_MC" === 0, $"VentaPotencialMinU6C_GP")
          .otherwise($"BrechaVenta_MC"))
      .drop($"VentaPotencialMinU6C_GP")

  }

  //#ListadoConsultoraBundle
  //Se actualiza la Brecha Venta con Motor de Canibalización a nivel de Táctica
  def basePotencialTacticaBundleP: DataFrame = {

    val bundle = updateListadoConsultoraBundle().alias("bundle")
    val tatcBundleP = listadoConsultoraBundle.groupBy($"PerfilGP", $"CodTactica")
      .agg((sum($"VentaPotencialMinU6C") / countDistinct($"CODEbelista")).alias("VentaPotencialMinU6C_GP")).alias("tatcBundleP")

    bundle
      .join(tatcBundleP, $"bundle.CodTactica" === $"tatcBundleP.CodTactica" &&
        $"bundle.PerfilGP" === $"tatcBundleP.PerfilGP", joinType = "Inner")
      .select($"bundle.*", $"tatcBundleP.VentaPotencialMinU6C_GP")
      .withColumn("BrechaVenta_MC",
        when($"BrechaVenta_MC" === 0, $"VentaPotencialMinU6C_GP")
          .otherwise($"BrechaVenta_MC"))
      .drop($"VentaPotencialMinU6C_GP")
  }

  //#ListadoConsultoraBundle
  def updatedListadoConsultoraBundleByTipoGP(): DataFrame = if (params.tipoGP().toInt == Constants.CARGA_ESTIMACION) basePotencialTacticaBundle else basePotencialTacticaBundleP

  /** 3. Cálculo del Score **/

  //Unión de tácticas
  //#ListadoConsultora_Total
  def listadoConsultora_Total: DataFrame = updateListadoConsultoraIndividual().union(updatedListadoConsultoraBundleByTipoGP())

  //#ListadoConsultora_Total
  //Considerar 4 Decimales
  def updateListadoConsultora_Total(): DataFrame = listadoConsultora_Total
    .withColumn("BrechaVentaRound", round($"BrechaVenta", 4))
    .withColumn("PrecioOptimoRound", round($"PrecioOptimo", 4))
    .withColumn("BrechaVenta_MCRound", round($"BrechaVenta_MC", 4))

  //#ListadoPromDSTotal
  //Se obtienen los Promedios y Desviaciones
  def listadoPromDSTotal: DataFrame = {

    updateListadoConsultora_Total()
      .where($"BrechaVenta" >= 0)
      .agg(round(sum($"FrecuenciaU24C") / countDistinct($"CODEbelista"), 4).alias("PromFrecuenciaU24C"),
        round(stddev_pop($"FrecuenciaU24C"), 4).alias("DSFrecuenciaU24C"),
        round(sum($"RecenciaU24C") / countDistinct($"CODEbelista"), 4).alias("PromRecenciaU24C"),
        round(stddev_pop($"RecenciaU24C"), 4).alias("DSRecenciaU24C"),
        round(sum($"BrechaVenta") / countDistinct($"CODEbelista"), 4).alias("PromBrechaVenta"),
        round(stddev_pop($"BrechaVenta"), 4).alias("DSBrechaVenta"),
        round(sum($"BrechaVenta_MC") / countDistinct($"CODEbelista"), 4).alias("PromBrechaVenta_MC"),
        round(stddev_pop($"BrechaVenta_MC"), 4).alias("DSBrechaVenta_MC"),
        round(sum($"Gatillador") / countDistinct($"CODEbelista"), 4).alias("PromGatillador"),
        round(stddev_pop($"Gatillador"), 4).alias("DSGatillador"),
        round(sum($"GAPPrecioOptimo") / countDistinct($"CODEbelista"), 4).alias("PromGAPPrecioOptimo"),
        round(stddev_pop($"GAPPrecioOptimo"), 4).alias("DSGAPPrecioOptimo"))
  }

  //#ListadoConsultora_Total
  //Se calculan los datos normalizados
  def updateListadoConsultora_TotalListadoPromDSTotal(): DataFrame = {

    val lc_t = updateListadoConsultora_Total().alias('lc_t)
    val lpds = listadoPromDSTotal.alias('lpds)

    val conditionTrue = lc_t.filter($"lc_t.BrechaVenta" >= 0)
    val conditionFalse = lc_t.filter($"lc_t.BrechaVenta" < 0)

    val trueConditionsTolistadoConsultora_Total = conditionTrue.alias('ct)
      .crossJoin(lpds)
      .updateWith(Seq($"ct.*"), Map(
        "FrecuenciaNor" -> when($"lpds.DSFrecuenciaU24C" =!= 0, round(($"ct.FrecuenciaU24C" - $"lpds.PromFrecuenciaU24C") / $"lpds.DSFrecuenciaU24C", 4)).otherwise(lit(0d)),
        "RecenciaNor" -> when($"lpds.DSRecenciaU24C" =!= 0, round(($"ct.RecenciaU24C" - $"lpds.PromRecenciaU24C") / $"lpds.DSRecenciaU24C", 4)).otherwise(lit(0d)),
        "BrechaVentaNor" -> when($"lpds.DSBrechaVenta" =!= 0, round(($"ct.BrechaVenta" - $"lpds.PromBrechaVenta") / $"lpds.DSBrechaVenta", 4)).otherwise(lit(0d)),
        "BrechaVenta_MCNor" -> when($"lpds.DSBrechaVenta_MC" =!= 0, round(($"ct.BrechaVenta_MC" - $"lpds.PromBrechaVenta_MC") / $"lpds.DSBrechaVenta_MC", 4)).otherwise(lit(0d)),
        "GAPPrecioOptimoNor" -> when($"lpds.DSGAPPrecioOptimo" =!= 0, round(($"ct.GAPPrecioOptimo" - $"lpds.PromGAPPrecioOptimo") / $"lpds.DSGAPPrecioOptimo", 4)).otherwise(lit(0d)),
        "GatilladorNor" -> when($"lpds.DSGatillador" =!= 0, round(($"ct.Gatillador" - $"lpds.PromGatillador") / $"lpds.DSGatillador", 4)).otherwise(lit(0d))
      ))

    val falseConditionsTolistadoConsultora_Total = conditionFalse.alias('cf)
      .crossJoin(lpds)
      .updateWith(Seq($"cf.*"), Map(
        "FrecuenciaNor" -> lit(0d),
        "RecenciaNor" -> lit(0d),
        "BrechaVentaNor" -> lit(0d),
        "BrechaVenta_MCNor" -> lit(0d),
        "GAPPrecioOptimoNor" -> lit(0d),
        "GatilladorNor" -> lit(0d)))
      
    trueConditionsTolistadoConsultora_Total.unionByName(
      falseConditionsTolistadoConsultora_Total)
  }

  //#ListadoConsultora_Total
  //Se calcula de la Oportunidad
  def updateListadoConsultora_TotalOportunidad(): DataFrame = {
    updateListadoConsultora_TotalListadoPromDSTotal()
      .withColumn("Oportunidad", round($"FrecuenciaNor" - $"RecenciaNor" + $"BrechaVentaNor" - $"GAPPrecioOptimoNor" + $"GatilladorNor", 4))
      .withColumn("Oportunidad_MC", round($"FrecuenciaNor" - $"RecenciaNor" + $"BrechaVenta_MCNor" - $"GAPPrecioOptimoNor" + $"GatilladorNor", 4))
  }

  //#ListadoPromDSTotalOportunidad
  //Se calcula el Promedio y Desviación Estándar de la Oportunidad
  def listadoPromDSTotalOportunidad: DataFrame = {
    updateListadoConsultora_TotalOportunidad()
      .where($"BrechaVenta" >= 0)
      .agg(round(sum($"Oportunidad") / countDistinct($"CODEbelista"), 4).alias("PromOportunidad"),
        round(stddev_pop($"Oportunidad"), 4).alias("DSOportunidad"))
  }

  //#ListadoPromDSTotalOportunidad_MC
  def listadoPromDSTotalOportunidad_MC: DataFrame = {
    updateListadoConsultora_TotalOportunidad()
      .where($"BrechaVenta_MC" >= 0)
      .agg(round(sum($"Oportunidad_MC") / countDistinct($"CODEbelista"), 4).alias("PromOportunidad_MC"),
        round(stddev_pop($"Oportunidad_MC"), 4).alias("DSOportunidad_MC"))
  }

  //#ListadoConsultora_Total
  //Se normaliza la variable Oportunidad
  def listadoPromDSTotalOportunidadNormaliza: DataFrame = {

    val lc_t = updateListadoConsultora_TotalOportunidad().alias('lc_t)
    val lpdsto = listadoPromDSTotalOportunidad.alias('lpdsto)

    val conditionTrue = lc_t.filter($"BrechaVenta" >= 0)
    val conditionFalse = lc_t.filter($"BrechaVenta" < 0)

    val trueConditionsTolistadoConsultora_Total = conditionTrue
      .crossJoin(lpdsto)
      .updateWith(Seq($"lc_t.*"), Map(
        "OportunidadNor" -> when($"lpdsto.DSOportunidad" =!= 0,
          round(($"lc_t.Oportunidad" - $"lpdsto.PromOportunidad") / $"lpdsto.DSOportunidad", 4))
            .otherwise(lit(0d))))

    trueConditionsTolistadoConsultora_Total.unionByName(
      conditionFalse.withColumn("OportunidadNor", lit(0d)))
  }

  //#ListadoConsultora_Total
  //Se normaliza la variable Oportunidad
  def listadoPromDSTotalOportunidad_MCNormaliza: DataFrame = {

    val lc_t = listadoPromDSTotalOportunidadNormaliza.alias('lc_t)
    val lpdsto = listadoPromDSTotalOportunidad_MC.alias('lpdsto)

    val conditionTrue = lc_t.filter($"BrechaVenta_MC" >= 0)
    val conditionFalse = lc_t.filter($"BrechaVenta_MC" < 0)

    val trueConditionsTolistadoConsultora_Total = conditionTrue
      .crossJoin(lpdsto)
      .updateWith(Seq($"lc_t.*"), Map(
        "Oportunidad_MCNor" -> when($"lpdsto.DSOportunidad_MC" =!= 0,
          round(($"lc_t.Oportunidad_MC" - $"lpdsto.PromOportunidad_MC") / $"lpdsto.DSOportunidad_MC", 4))
          .otherwise(lit(0))))

    trueConditionsTolistadoConsultora_Total.unionByName(
      conditionFalse.withColumn("Oportunidad_MCNor", lit(0d)))

  }

  //#ListadoConsultora_Total
  //Se calcula el Score en base a la Oportunidad
  def updateListadoConsultora_TotalScore(): DataFrame = {

    val conditionTrue = listadoPromDSTotalOportunidad_MCNormaliza.filter($"BrechaVenta" >= 0)
    val conditionFalse = listadoPromDSTotalOportunidad_MCNormaliza.filter($"BrechaVenta" < 0)

    val trueConditionsTolistadoConsultora_Total = conditionTrue
      .withColumn("Score", exp($"OportunidadNor") / (exp($"OportunidadNor") + 1))

    trueConditionsTolistadoConsultora_Total.unionByName(
      conditionFalse.withColumn("Score", lit(0d)))

  }

  //#ListadoConsultora_Total
  //Se calcula el Score en base a la Oportunidad
  def updateListadoConsultora_TotalScoreMC(): DataFrame = {

    val conditionTrue = updateListadoConsultora_TotalScore().filter($"BrechaVenta_MC" >= 0)
    val conditionFalse = updateListadoConsultora_TotalScore().filter($"BrechaVenta_MC" < 0)

    val trueConditionsTolistadoConsultora_Total = conditionTrue
      .withColumn("Score", exp($"Oportunidad_MCNor") / (exp($"Oportunidad_MCNor") + 1))

    trueConditionsTolistadoConsultora_Total.unionByName(
      conditionFalse.withColumn("Score", lit(0d)))

  }

  //#ListadoConsultora_Total
  //Se calcula el Score_UU en base a la Oportunidad multiplicada por el número de unidades
  def updateListadoConsultora_TotalScoreUU(): DataFrame = {

    val conditionTrue = updateListadoConsultora_TotalScoreMC().filter($"BrechaVenta" >= 0)
    val conditionFalse = updateListadoConsultora_TotalScoreMC().filter($"BrechaVenta" < 0)

    val trueConditionsTolistadoConsultora_Total = conditionTrue
      .withColumn("Score", when($"OportunidadNor" * $"UnidadesTactica" > 700, exp(lit(700) / (exp(lit(700)) + 1)))
        .otherwise(exp($"OportunidadNor" * $"UnidadesTactica") / ($"OportunidadNor" * $"UnidadesTactica" + 1)))

    trueConditionsTolistadoConsultora_Total.unionByName(
      conditionFalse.withColumn("Score", lit(0d)))

  }

  //#ListadoConsultora_Total
  //Se calcula el Score_UU en base a la Oportunidad multiplicada por el número de unidades
  def updateListadoConsultora_TotalScoreMCUU(): DataFrame = {

    val conditionTrue = updateListadoConsultora_TotalScoreUU().filter($"BrechaVenta_MC" >= 0)
    val conditionFalse = updateListadoConsultora_TotalScoreUU().filter($"BrechaVenta_MC" < 0)

    val trueConditionsTolistadoConsultora_Total = conditionTrue
      .withColumn("Score", when($"Oportunidad_MCNor" * $"UnidadesTactica" > 700, exp(lit(700) / (exp(lit(700)) + 1)))
        .otherwise(exp($"Oportunidad_MCNor" * $"UnidadesTactica") / ($"Oportunidad_MCNor" * $"UnidadesTactica" + 1)))

    trueConditionsTolistadoConsultora_Total.unionByName(
      conditionFalse.withColumn("Score", lit(0d)))

  }

  def insertArpListadoVariablesRFM(): DataFrame = {

    val columnsName = Seq($"CodPais", $"AnioCampanaProceso", $"AnioCampanaExpo", $"TipoARP", $"TipoPersonalizacion", $"CodEbelista", $"CodRegion", $"CodComportamientoRolling",
      $"Antiguedad", $"TipoTactica", $"CodTactica", $"VentaAcumU6C", $"VentaAcumPU6C", $"VentaAcumU6C_AA", $"VentaAcumU24C", $"VentaPromU24C", $"FrecuenciaU24C",
      $"RecenciaU24C", $"CicloRecompraPotencial", $"BrechaRecompraPotencial", $"VentaPotencialMinU6C", $"GAP", $"BrechaVenta", $"BrechaVenta_MC",
      $"FlagCompra", $"PrecioOptimo", $"GAPPrecioOptimo", $"GAPRegalo", $"NumAspeos", $"Gatillador", $"GatilladorRegalo", $"FrecuenciaNor", $"RecenciaNor",
      $"BrechaVentaNor", $"BrechaVenta_MCNor", $"GAPPrecioOptimoNor", $"GatilladorNor", $"Oportunidad", $"Oportunidad_MC", $"OportunidadNor",
      $"Oportunidad_MCNor", $"Score", $"Score_MC", $"UnidadesTactica", $"Score_UU", $"Score_MC_UU", $"PerfilOficial", $"FlagSeRecomienda", $"FlagTop" , $"Perfil")

    updateListadoConsultora_TotalScoreMCUU()
      .select(columnsName: _*)
  }

  /** No usar Motor de Canibalizacion **/
  def noUsarMotorCanibalizacion: DataFrame = {

    val columnsName = Seq($"CodPais", $"AnioCampanaProceso", $"AnioCampanaExpo", $"CodEbelista", $"CodTActica", $"FlagTop", $"Score_UU".alias("Probabilidad"),
      $"TipoARP", $"TipoPersonalizacion", lit(params.flagMC()).as("FlagMC"), $"Perfil")

    updateListadoConsultora_TotalScoreMCUU()
      .select(columnsName: _*)
  }

  /** Usar Motor de Canibalizacion **/
  def usarMotorCanibalizacion: DataFrame = {

    val columnsName = Seq($"CodPais", $"AnioCampanaProceso", $"AnioCampanaExpo", $"CodEbelista", $"CodTActica", $"FlagTop", $"Score_MC_UU".alias("Probabilidad"),
      $"TipoARP", $"TipoPersonalizacion", lit(params.flagMC()).as("FlagMC"), $"Perfil")

    updateListadoConsultora_TotalScoreMCUU()
      .select(columnsName: _*)
  }
}


