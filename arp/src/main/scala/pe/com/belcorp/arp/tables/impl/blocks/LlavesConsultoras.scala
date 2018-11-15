package pe.com.belcorp.arp.tables.impl.blocks

import org.apache.spark.sql.{DataFrame, SparkSession}
import pe.com.belcorp.arp.tables.Table

class LlavesConsultoras(baseConsultoras: DataFrame) extends Table {
  override def get(spark: SparkSession): DataFrame =
    baseConsultoras.select("CodEbelista")
}
