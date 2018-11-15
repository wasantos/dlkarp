package pe.com.belcorp.arp.tables.impl.blocks

import org.apache.spark.sql.functions.{lit, max}
import org.apache.spark.sql.{DataFrame, SparkSession}
import pe.com.belcorp.arp.tables.Table
import pe.com.belcorp.arp.utils.ProcessParams

/**
  * Se obtienen los productos a nivel CUC
  *
  * @param params
  * @param listadoProductos
  * @param dwhDProducto
  */
class ProductosCUC(params: ProcessParams,
  listadoProductos: DataFrame, dwhDProducto: DataFrame) extends Table {

  override def get(spark: SparkSession): DataFrame = {
    import spark.implicits._

    val lp = listadoProductos.as("lp")
    val dp = dwhDProducto.as("dp")

    lp.join(dp, $"lp.CodCUC" === $"dp.CUC", "inner")
      .where($"dp.DescripCUC".isNotNull)
      .groupBy(
        $"dp.CUC".as("CodProducto"),
        $"TipoTactica", $"CodTactica", $"CodVenta", $"Unidades",
        $"lp.PrecioOferta", $"IndicadorPadre", $"FlagTop")
      .agg(
        lit("").cast("string").as("CodSAP"),
        max($"dp.DescripCUC").as("DesProducto"),
        max($"dp.DesMarca").as("DesMarca"),
        max($"dp.DesCategoria").as("DesCategoria"),
        max($"LimUnidades").as("LimUnidades"),
        max($"FlagUltMinuto").as("FlagUltMinuto"))
  }
}
