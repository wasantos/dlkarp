package pe.com.belcorp.arp.tables.impl.blocks

import org.apache.spark.sql.functions.not
import org.apache.spark.sql.{DataFrame, SparkSession}
import pe.com.belcorp.arp.tables.Table
import pe.com.belcorp.arp.utils.{AnioCampana, ProcessParams}
import pe.com.belcorp.arp.utils.Extensions._

/**
  * Se crea la tabla temporal donde se guardan
  * la venta de las últimas N campañas
  *
  * @param params
  * @param baseConsultoras
  * @param dwhFVtaProEbeCam
  * @param acInicio
  */
class FVtaProEbeCamLimitado(params: ProcessParams, dwhDTipoOferta: DataFrame,
  baseConsultoras: DataFrame, dwhFVtaProEbeCam: DataFrame,
  productos: DataFrame, acInicio: AnioCampana) extends Table {

  override def get(spark: SparkSession): DataFrame = {
    val fv = dwhFVtaProEbeCam.alias("fv")
    val bc = baseConsultoras.alias("bc")
    val dt = dwhDTipoOferta.alias("dt")
    val dp = productos.alias("dp")

    val limited = fv
      .join(bc, makeEquiJoin("fv", "bc", Seq("CodEbelista")), "left_semi")
      .where(
        $"fv.AnioCampana".between(acInicio.toString, params.anioCampanaProceso())
          && $"fv.CodPais" === params.codPais())
      .select(
        $"fv.AnioCampana", $"fv.CodEbelista", $"fv.CodSAP",
        $"fv.CodTipoOferta", $"fv.CodTerritorio", $"fv.NroFactura",
        $"fv.CodVenta", $"fv.AnioCampanaRef", $"fv.RealUUVendidas", $"fv.RealVTAMNNeto",
        $"fv.RealVTAMNFactura", $"fv.RealVTAMNCatalogo")
      .alias("lfv")

    limited
      .join(dp, $"dp.CodSAP" === $"lfv.CodSAP", "left_semi")
      .join(dt, $"dt.CodTipoOferta" === $"lfv.CodTipoOferta", "inner")
      .where($"lfv.AnioCampana" === $"lfv.AnioCampanaRef"
          && $"dt.CodTipoProfit" === "01"
          && not($"dt.CodTipoOferta".isin("030", "040", "051"))
          && $"lfv.RealVTAMNNeto" > 0)
      .select($"lfv.*")
  }
}
