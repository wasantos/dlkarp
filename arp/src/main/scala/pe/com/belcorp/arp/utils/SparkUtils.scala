package pe.com.belcorp.arp.utils

import org.apache.hadoop.fs.Path
import org.apache.spark.sql.SparkSession

object SparkUtils {
  def getSparkSession: SparkSession = {
    val spark = SparkSession
      .builder()
      .appName("EjecutaARP-Belcorp")
      .enableHiveSupport
      .config("spark.serializer", "org.apache.spark.serializer.KryoSerializer")
      .config("spark.sql.sources.partitionOverwriteMode", "dynamic")
      .config("spark.sql.retainGroupColumns", "true")
      .config("spark.sql.parquet.writeLegacyFormat", "true")
      .config("hive.optimize.s3.query", "true")
      .config("hive.exec.parallel", "true")
      .config("hive.exec.dynamic.partition", "true")
      .config("hive.exec.dynamic.partition.mode", "nonstrict")
      .getOrCreate()

    val checkpointPath = new Path(
      System.getenv("SPARK_YARN_STAGING_DIR"), "checkpoints")
    spark.sparkContext.setCheckpointDir(checkpointPath.toString)

    spark
  }
}
