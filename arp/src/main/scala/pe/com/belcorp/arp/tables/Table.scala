package pe.com.belcorp.arp.tables

import org.apache.spark.sql.{DataFrame, SparkSession}

trait Table {
  /**
    * Generates a reusable DataFrame
    * @return a Spark Dataframe
    */
  def get(spark: SparkSession): DataFrame

  /**
    * Helper method to call get using a implicit SparkSession
    */
  def apply()(implicit spark: SparkSession): DataFrame = get(spark)
}
