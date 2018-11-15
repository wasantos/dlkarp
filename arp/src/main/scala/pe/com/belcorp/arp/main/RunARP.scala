package pe.com.belcorp.arp.main

import java.util.UUID

import org.apache.spark.sql.{DataFrame, SparkSession}
import pe.com.belcorp.arp.tables._
import pe.com.belcorp.arp.tables.impl.CargaOEstimacion.cargaOEstimacion
import pe.com.belcorp.arp.tables.impl._
import pe.com.belcorp.arp.utils.Extensions._
import pe.com.belcorp.arp.utils.Goodies.logIt
import pe.com.belcorp.arp.utils.ProcessParams

object RunARP {
  def main(args: Array[String]): Unit = {
    val params = new ProcessParams(args)
    val processId = UUID.randomUUID().toString

    implicit val spark: SparkSession = SparkSession
      .builder()
      .appName("EjecutaARP-Belcorp")
      .enableHiveSupport
      .config("spark.serializer", "org.apache.spark.serializer.KryoSerializer")
      .config("spark.sql.sources.partitionOverwriteMode", "dynamic")
      .config("spark.sql.retainGroupColumns", "true")
      .config("hive.optimize.s3.query", "true")
      .config("hive.exec.parallel", "true")
      .config("hive.exec.dynamic.partition", "true")
      .config("hive.exec.dynamic.partition.mode", "nonstrict")
      .getOrCreate()

    val history = new ProcessHistory(processId, params)
    history.pushOK("Ejecucion iniciada")

    import spark.implicits._

    val repository = new Repository(spark, params)
    val arpParams = new ParameterTables.ARP(spark, params)

    spark.catalog.setCurrentDatabase(params.schemaDatalake())

    try {
      // Fetch parameter tables
      val arpParametros_tb = arpParams.parametrosTb
      val arpParametrosEst_tb = arpParams.parametrosEstTb
      val arpEspaciosForzados_tb = arpParams.espaciosForzadosTb
      val arpEspaciosForzadosEst_tb = arpParams.espactiosForzadosEstTb
      val mdlPerfilOutput_tb = arpParams.mdlPerfilOutputTb

      // Fetch source tables
      val dwhDproducto_tb = repository.sourceTable("DWH_DPRODUCTO")

      val dwhFstaebecam_tb = repository.sourceTable("DWH_FSTAEBECAM")
        .where($"CodPais" === params.codPais())

      val dwhDebelista_tb = repository.sourceTable("DWH_DEBELISTA")
        .where($"CodPais" === params.codPais())

      val dwhGeografiaCampana_tb = repository.sourceTable("DWH_DGEOGRAFIACAMPANA")
        .where($"CodPais" === params.codPais())

      val dwhFvtaproebecam_tb = repository.sourceTable("DWH_FVTAPROEBECAM")
        .where($"CodPais" === params.codPais())

      val dwhDtipooferta_tb = repository.sourceTable("DWH_DTIPOOFERTA")
        .where($"CodPais" === params.codPais())

      val dwhDmatrizcampana_tb = repository.sourceTable("DWH_DMATRIZCAMPANA")
        .where($"CodPais" === params.codPais())

      // Fetch estimation tables
      val (
        listadoProductos_df, listadoRegalos_df,
        espaciosForzados_df, campaniaExpoEspacios_df,
        productosTotales_df
        ) = cargaOEstimacion(spark, params,
        arpParametros_tb, arpParametrosEst_tb,
        arpEspaciosForzados_tb, arpEspaciosForzadosEst_tb
      )


      /** 1.0. Módulo Validación de Parámetros **/
      val validacion = new ParameterValidation(params, dwhDproducto_tb,
        Some(arpParametros_tb), Some(arpParametrosEst_tb))

      val mensajes = validacion()

      if (mensajes.exists) {
        logIt("!! ERROS DE VALIDACION !!")
        for (message <- mensajes.select($"message".as[String]).collect()) {
          logIt(message)
          history.pushError(message)
        }
      }

      /** 1.1. Módulo Base de Consultoras **/
      val baseConsultoras_df = {
        val df = new BaseConsultoras(
          params, dwhDebelista_tb, dwhGeografiaCampana_tb,
          dwhDtipooferta_tb, dwhFstaebecam_tb, dwhFvtaproebecam_tb,
          mdlPerfilOutput_tb
        ).get(spark)

        repository.saveArpTable(spark, df, "ARP_BaseConsultoras",
          Seq("CodPais", "AnioCampanaProceso", "Perfil"))
      }

      System.gc()

      /** 1.2. Módulo RFM **/
      val (
        listadoVariablesIndividual,
        listadoVariablesBundle,
        _,
        _
        ) = {
        val dfs = new RFM(params,
          dwhDebelista_tb,
          dwhGeografiaCampana_tb, dwhDmatrizcampana_tb,
          dwhDproducto_tb, dwhDtipooferta_tb,
          dwhFstaebecam_tb, dwhFvtaproebecam_tb,
          listadoProductos_df, baseConsultoras_df).get(spark)

        (
          dfs._1.map(
            repository.saveArpTable(spark, _, "ARP_ListadoVariablesIndividual")),
          dfs._2.map(
            repository.saveArpTable(spark, _, "ARP_ListadoVariablesBundle")),
          dfs._3.map(
            repository.saveArpTable(spark, _, "ARP_ListadoProbabilidades")),
          dfs._4.map(
            repository.saveArpTable(spark, _, "ARP_ListadoVariablesProducto"))
        )
      }

      if (params.tipoARP() == Constants.ARP_ESTABLECIDAS) {
        /** 1.3. Módulo Grupo Potencial **/
        val (grupoPotencial_df: DataFrame, grupoPotencialBundle_df: DataFrame, listadoVariablesRFM: DataFrame) =
          new GrupoPotencial(
            params,
            dwhGeografiaCampana_tb, dwhDmatrizcampana_tb,
            dwhDproducto_tb, dwhDtipooferta_tb,
            dwhFstaebecam_tb, dwhFvtaproebecam_tb,
            listadoProductos_df, listadoRegalos_df,
            baseConsultoras_df, mdlPerfilOutput_tb,
            listadoVariablesIndividual.get, listadoVariablesBundle.get
          ).get(spark)

        val gpPartitions01 = Seq("CODPAIS", "ANIOCAMPANAPROCESO", "ANIOCAMPANAEXPO",
          "TIPOARP", "TIPOPERSONALIZACION", "PERFIL", "TIPOTACTICA", "TIPOGP")

        val gpPartitions02 = Seq(
          "CODPAIS", "ANIOCAMPANAPROCESO", "ANIOCAMPANAEXPO",
          "TIPOARP", "TIPOPERSONALIZACION", "PERFIL")

        (
          repository.saveArpTable(spark, grupoPotencial_df,
            "ARP_BaseGrupoPotencial", gpPartitions01),
          repository.saveArpTable(spark, grupoPotencialBundle_df,
            "ARP_BaseGrupoPotencial", gpPartitions01),
          repository.saveArpTable(spark, listadoVariablesRFM,
            "ARP_ListadoVariablesRFM", gpPartitions02)
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
