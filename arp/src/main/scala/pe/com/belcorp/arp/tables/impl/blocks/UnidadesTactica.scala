package pe.com.belcorp.arp.tables.impl.blocks

import org.apache.spark.sql.functions.sum
import org.apache.spark.sql.{DataFrame, SparkSession}
import pe.com.belcorp.arp.tables.Table

class UnidadesTactica(productosCUC: DataFrame) extends Table {
  override def get(spark: SparkSession): DataFrame = {
    import spark.implicits._

    productosCUC
      .groupBy($"CodTactica")
      .agg(sum($"Unidades").as("TotalUnidades"))
      .select("*")
  }
}
