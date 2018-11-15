package pe.com.belcorp.arp.tables.impl.blocks

import org.apache.spark.sql.{DataFrame, SparkSession}
import pe.com.belcorp.arp.tables.Table
import pe.com.belcorp.arp.utils.ProcessParams

/**
  * Se obtienen los productos a nivel de Item (CODSAP?)
  *
  * @param params
  * @param listadoProductos
  * @param dwhDProducto
  */
class ProductosItem(params: ProcessParams,
  listadoProductos: DataFrame, dwhDProducto: DataFrame) extends Table {

  override def get(spark: SparkSession): DataFrame = {
    import spark.implicits._

    val lp = listadoProductos.alias("lp")
    val dp = dwhDProducto.alias("dp")

    lp.join(dp, $"lp.CodCUC" === $"dp.CUC", "inner")
      .select(
        $"TipoTactica", $"CodTactica",
        $"dp.CUC".as("CodProducto"), $"Unidades",
        $"dp.DescripCUC".as("DesProducto"),
        $"lp.PrecioOferta", $"CodMarca", $"dp.CodSAP", $"lp.FlagTop")
  }
}
