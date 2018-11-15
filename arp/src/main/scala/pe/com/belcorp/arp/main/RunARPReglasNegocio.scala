package pe.com.belcorp.arp.main

import java.util.UUID

import org.apache.spark.sql.{DataFrame, SparkSession}
import pe.com.belcorp.arp.tables._
import pe.com.belcorp.arp.tables.impl._
import pe.com.belcorp.arp.utils.{ProcessParams, SparkUtils}

object RunARPReglasNegocio {
  def main(args: Array[String]): Unit = {
    val params = new ProcessParams(args)
    val processId = UUID.randomUUID().toString

    implicit val spark: SparkSession = SparkUtils.getSparkSession
    import spark.implicits._

    val history = new ProcessHistory(processId, params)
    history.pushOK("Ejecucion iniciada")

    val repository = new Repository(spark, params)
    val arpParams = new ParameterTables.ARP(spark, params)

    spark.catalog.setCurrentDatabase(params.schemaDatalake())

    try {
      // Fetch parameter tables
      val arpParametros_tb = arpParams.parametrosTb
      val arpParametrosEst_tb = arpParams.parametrosEstTb
      val arpEspaciosForzados_tb = arpParams.espaciosForzadosTb
      val arpEspaciosForzadosEst_tb = arpParams.espactiosForzadosEstTb

      // Fetch source tables
      val dwhDproducto_tb = repository.sourceTable("DWH_DPRODUCTO")

      val dwhDebelista_tb = repository.sourceTable("DWH_DEBELISTA")
        .where($"CodPais" === params.codPais())

      val dwhDmatrizcampana_tb = repository.sourceTable("DWH_DMATRIZCAMPANA")
        .where($"CodPais" === params.codPais())

      val arpListadoProbabilidades = repository.arpTable("ARP_ListadoProbabilidades")
      val arpListadoVariablesProducto = repository.arpTable("ARP_ListadoVariablesProducto")

      /** 1.5.1 Módulo Reglas de Negocio SR **/

      if (params.tipoPersonalizacion() == "SR") {
        val arpTotalProductosEstimados = new ReglasNegocioSR(params, arpParametrosEst_tb, arpEspaciosForzadosEst_tb,
          dwhDproducto_tb, dwhDmatrizcampana_tb, Option(arpListadoProbabilidades), "SR").get(spark)

        repository.saveArpTable(spark, arpTotalProductosEstimados, "ARP_TotalProductosEstimados")
      }

      /** 1.5.2 Módulo Reglas de Negocio ODD **/

      if (params.tipoPersonalizacion() == "ODD") {
        //TODO -> arpParametrosDiaInicio é igual a arpParametros_tb ???

        val tipoPersonalizacionODD = "ODD"
        val arpOfertaPersonalizadaC01 = new ReglasNegocioODD(params, arpParametros_tb, arpEspaciosForzados_tb,
          dwhDproducto_tb, dwhDmatrizcampana_tb, Option(arpListadoProbabilidades), arpParametros_tb, dwhDebelista_tb,
          Option(arpListadoVariablesProducto), "ODD")

        val valid = arpParametros_tb
          .where($"CodPais" === params.codPais() &&
            $"AnioCampanaExpo" === params.anioCampanaExpo() &&
            $"TipoPersonalizacion" === tipoPersonalizacionODD &&
            $"Perfil" === 1)
          .head(1).nonEmpty

        if (valid) {
          repository.saveArpTable(spark, arpOfertaPersonalizadaC01.getExists(spark), "ARP_OfertaPersonalizadaC01")
        } else {
          repository.saveArpTable(spark, arpOfertaPersonalizadaC01.getNotExists(spark), "ARP_OfertaPersonalizadaC01")
        }

      }

      history.pushOK("Ejecucion completada")
    } catch {
      case e: Exception =>
        history.pushError(s"Ejecucion con erro: ${e.getMessage}")
        throw e
    } finally {
      history.flush(spark, repository.arpTableName("ARP_Ejecucion_Log"))
      spark.stop()
    }
  }
}
