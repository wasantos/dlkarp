package pe.com.belcorp.arp.tables

import org.apache.spark.sql.{DataFrame, SparkSession}
import pe.com.belcorp.arp.utils.ProcessParams

object ParameterTables {
  class CSVBase(val params: ProcessParams, val baseFile: String) {
    def get(spark: SparkSession): DataFrame = {
      import spark.implicits._

      spark.read
        .format("csv")
        .option("header","true")
        .load(path)
        .where($"CODPAIS" === params.codPais())
        .cache()
    }

    def path: String = {
      val folder = params.carpetaParametros
        .getOrElse("pocbelcorp/datalake/dwh")
      s"s3://$folder/$baseFile"
    }
  }

  class ARPParametros(params: ProcessParams)
    extends CSVBase(params, "arp_parametros/arp_parametros.csv")
  class ARPParametrosEst(params: ProcessParams)
    extends CSVBase(params, "arp_parametros/arp_parametros_est.csv")
  class ARPEspaciosForzados(params: ProcessParams)
    extends CSVBase(params, "arp_parametros/arp_espacioforzados.csv")
  class ARPEspaciosForzadosEst(params: ProcessParams)
    extends CSVBase(params, "arp_parametros/arp_espacioforzados_est.csv")
  class MDLPerfilOutput(params: ProcessParams)
    extends CSVBase(params, "MDL_PERFILOUTPUT_PE201810.csv")

  /**
    * Caching global object for ARP parameter tables
    * Not thread-safe
    */
  class ARP(spark: SparkSession, params: ProcessParams) {
    import pe.com.belcorp.arp.utils.OnceBox.{empty => emptyBox}

    def parametrosTb: DataFrame =
      parametrosBox.set(new ARPParametros(params).get(spark)).get
    def parametrosEstTb: DataFrame =
      parametrosEstBox.set(new ARPParametrosEst(params).get(spark)).get
    def espaciosForzadosTb: DataFrame =
      espaciosForzadosBox.set(new ARPEspaciosForzados(params).get(spark)).get
    def espactiosForzadosEstTb: DataFrame =
      espactiosForzadosEstBox.set(new ARPEspaciosForzadosEst(params).get(spark)).get
    def mdlPerfilOutputTb: DataFrame =
      mdlPerfilOutputBox.set(new MDLPerfilOutput(params).get(spark)).get

    private val parametrosBox = emptyBox[DataFrame]
    private val parametrosEstBox = emptyBox[DataFrame]
    private val espaciosForzadosBox = emptyBox[DataFrame]
    private val espactiosForzadosEstBox = emptyBox[DataFrame]
    private val mdlPerfilOutputBox = emptyBox[DataFrame]
  }

}
