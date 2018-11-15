package pe.com.belcorp.arp.main

import java.util.UUID

import org.apache.spark.sql.{DataFrame, SparkSession}
import pe.com.belcorp.arp.tables._
import pe.com.belcorp.arp.tables.impl._
import pe.com.belcorp.arp.utils.{ProcessParams, SparkUtils}

object RunARPMotorCanibalizacion {
  def main(args: Array[String]): Unit = {
    val params = new ProcessParams(args)
    val processId = UUID.randomUUID().toString

    implicit val spark: SparkSession = SparkUtils.getSparkSession

    val history = new ProcessHistory(processId, params)
    history.pushOK("Ejecucion iniciada")

    val repository = new Repository(spark, params)
    val arpParams = new ParameterTables.ARP(spark, params)

    spark.catalog.setCurrentDatabase(params.schemaDatalake())

    try {
      // Fetch parameter tables
      val mdlPerfilOutput_tb = arpParams.mdlPerfilOutputTb

      // Fetch source tables
      val listadoVariablesRFM_tb = repository.arpTable("ARP_ListadoVariablesRFM")

      /** 1.4. Módulo Motor de Canibalización **/
      if (params.tipoARP() == Constants.ARP_ESTABLECIDAS) {
        val (arpListadoVariablesRFM: DataFrame, arpListadoProbabilidades: DataFrame) = new MotorCanibalizacion(params, listadoVariablesRFM_tb, mdlPerfilOutput_tb).get(spark)

        val cleanFromARP_ListadoVariablesRFM = Seq("AnioCampanaProceso", "AnioCampanaExpo", "CodPais", "TipoARP", "TipoPersonalizacion", "Perfil")
        val cleanFromARP_ListadoProbabilidades = Seq("CodPais", "AnioCampanaProceso", "AnioCampanaExpo", "TipoARP", "TipoPersonalizacion", "FlagMC", "Perfil")

        (
          repository.saveArpTable(spark, arpListadoVariablesRFM,
            "ARP_ListadoVariablesRFM", cleanFromARP_ListadoVariablesRFM,
            overwriteSelf = true),
          repository.saveArpTable(spark, arpListadoProbabilidades,
            "ARP_ListadoProbabilidades", cleanFromARP_ListadoProbabilidades,
            overwriteSelf = true)
        )
      }

      history.pushOK("Ejecucion completada")
    } catch {
      case e: Exception =>
        history.pushError(s"Ejecucion con erro: ${e.getMessage}")
        throw e
    }
    finally {
      history.flush(spark, repository.arpTableName("ARP_Ejecucion_Log"))
      spark.stop()
    }
  }
}
