package pe.com.belcorp.arp.tables

import org.apache.spark.sql.{DataFrame, SaveMode, SparkSession}
import pe.com.belcorp.arp.utils.ProcessParams

class Repository(val sparkSession: SparkSession, val params: ProcessParams) {
  final val REPARTITION_NUM = 500

  def schemaDatalake(): String = params.schemaDatalake.getOrElse("belcorpdb")
  def schemaARP(): String = params.schemaARP.getOrElse("belcorpdb")

  def sourceTable(name: String): DataFrame = {
    sparkSession.table(s"${schemaDatalake()}.$name")
  }

  def arpTable(name: String): DataFrame = {
    val fullname = arpTableName(name)

    if(sparkSession.catalog.tableExists(fullname)) {
      sparkSession.table(fullname)
    } else {
      null
    }
  }

  def arpTableName(name: String): String = s"${schemaARP()}.$name"

  def saveArpTable(spark: SparkSession, dataFrame: DataFrame,
    name: String, partitions: Seq[String] = Seq.empty,
    pass: Boolean = false, overwriteSelf: Boolean = false): DataFrame = {

    val fullName = arpTableName(name)
    if (pass) return spark.table(fullName)

    if(partitions.isEmpty) {
      dataFrame.write
        .mode(SaveMode.Overwrite)
        .saveAsTable(fullName)
    } else {
      val columns = dataFrame.columns
      val reorderedDf = dataFrame.selectExpr(reorderColumns(columns, partitions): _*)
        .repartition(REPARTITION_NUM)

      val finalDf =
        if(overwriteSelf) reorderedDf.checkpoint()
        else reorderedDf

      if (spark.catalog.tableExists(fullName)) {
        finalDf.write
          .mode(SaveMode.Overwrite)
          .insertInto(fullName)
      } else {
        finalDf.write
          .partitionBy(partitions: _*)
          .saveAsTable(fullName)
      }
    }

    spark.table(fullName)
  }

  private def reorderColumns(columns: Seq[String], partitions: Seq[String]): Seq[String] = {
    val lowercasePartitions = partitions.map(_.toLowerCase)
    val lowercaseColumns = columns.map(_.toLowerCase)

    val filtered = lowercaseColumns.filterNot(lowercasePartitions.contains)

    filtered ++ lowercasePartitions
  }
}
