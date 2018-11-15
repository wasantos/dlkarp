package pe.com.belcorp.arp.tables.impl.blocks

import org.apache.spark.sql.{DataFrame, SparkSession}
import pe.com.belcorp.arp.tables.Table
import pe.com.belcorp.arp.utils.{AnioCampana, ProcessParams}

/**
  * Obtengo el estado de las consultoras que conforman la base en las últimas 24 campañas
  *
  * @param params
  * @param dwhFStaEbeCam
  * @param acInicio
  */
class FStaEbeCamLimitado(params: ProcessParams,
  dwhFStaEbeCam: DataFrame, acInicio: AnioCampana) extends Table {

  override def get(spark: SparkSession): DataFrame = {
    import spark.implicits._

    val acProceso = AnioCampana.parse(params.anioCampanaProceso())

    dwhFStaEbeCam
      .where($"AnioCampana".between(acInicio.toString, acProceso.toString)
        && $"CodPais" === params.codPais())
      .select(
      $"CodEbelista", $"AnioCampana", $"FlagPasoPedido", $"FlagActiva",
      $"CodComportamientoRolling", $"CodigoFacturaInternet")
  }
}
