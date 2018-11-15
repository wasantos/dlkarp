package pe.com.belcorp.arp.tables.impl.blocks

import org.apache.spark.sql.{DataFrame, SparkSession}
import pe.com.belcorp.arp.tables.Table
import pe.com.belcorp.arp.utils.ProcessParams
import pe.com.belcorp.arp.utils.Extensions._

/**
  * Obtengo el estado de las consultoras que conforman la base en las últimas 24 campañas
  *
  * @param params
  * @param baseConsultoras
  * @param tmpFStaEbeCam
  */
class BaseEstadoConsultoras(params: ProcessParams,
  baseConsultoras: DataFrame, tmpFStaEbeCam: DataFrame) extends Table {

  override def get(spark: SparkSession): DataFrame = {
    val bc = baseConsultoras.alias("bc")
    val fs = tmpFStaEbeCam.alias("fs")

    bc.join(fs, makeEquiJoin("bc", "fs", Seq("CodEbelista")))
      .select(
        $"bc.CodEbelista", $"fs.AnioCampana",
        $"fs.FlagPasoPedido", $"fs.FlagActiva")
  }
}
