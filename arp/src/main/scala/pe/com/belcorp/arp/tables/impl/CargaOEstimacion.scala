package pe.com.belcorp.arp.tables.impl

import org.apache.spark.sql.{DataFrame, SparkSession}
import pe.com.belcorp.arp.tables.Constants
import pe.com.belcorp.arp.utils.ProcessParams

object CargaOEstimacion {
  type Resultados = (DataFrame, DataFrame, DataFrame, DataFrame, DataFrame)

  def cargaOEstimacion(spark: SparkSession, params: ProcessParams,
    arpParametrosTb: DataFrame, arpParametrosEstTb: DataFrame,
    arpEspaciosForzadosTb: DataFrame, arpEspaciosForzadosEstTb: DataFrame
  ): Resultados = {
    import spark.implicits._

    val (arpParametrosFuente_tb, arpEspaciosForzadosFuente_tb) =
      if(params.flagCarga() == Constants.CARGA_PERSONALIZACION) {
        (arpParametrosTb, arpEspaciosForzadosTb)
      } else {
        (arpParametrosEstTb, arpEspaciosForzadosEstTb)
      }

    val arpParametrosFuente_df = arpParametrosFuente_tb
      .where(
        $"TIPOARP" === params.tipoARP()
          && $"TIPOPERSONALIZACION" === params.tipoPersonalizacion()
          && $"PERFIL" === params.perfil()
          && $"ANIOCAMPANAPROCESO" === params.anioCampanaProceso()
          // && $"ANIOCAMPANAEXPO" === params.anioCampanaExpo()
      ).select(
        $"CODPAIS",
        $"TIPOTACTICA",
        $"CODTACTICA",
        $"CODCUC",
        $"UNIDADES",
        $"PRECIOOFERTA",
        $"CODVENTA",
        $"INDICADORPADRE",
        $"FLAGTOP",
        $"LIMUNIDADES",
        $"FLAGULTMINUTO",
        $"CODVINCULOOF",
        $"ANIOCAMPANAEXPO",
        $"ESPACIOS",
        $"ESPACIOSTOP"
      )

    /** Se eligen productos sin regalo para los cálculos **/
    val listadoProductos_df = arpParametrosFuente_df
      .where($"PRECIOOFERTA" > 0)
      .select(
        $"TIPOTACTICA",
        $"CODTACTICA",
        $"CODCUC",
        $"UNIDADES",
        $"PRECIOOFERTA",
        $"CODVENTA",
        $"INDICADORPADRE",
        $"FLAGTOP",
        $"LIMUNIDADES",
        $"FLAGULTMINUTO",
        $"CODVINCULOOF"
      ).distinct()

    /** Se eligen los regalos para los cálculos **/
    val listadoRegalos_df = arpParametrosFuente_df
      .where($"PRECIOOFERTA" === 0)
      .select(
        $"TIPOTACTICA",
        $"CODTACTICA",
        $"CODCUC",
        $"UNIDADES",
        $"PRECIOOFERTA",
        $"CODPAIS"
      ).distinct()

    /** Se guardan el números de espacios forzados **/
    val espaciosForzados_df = arpEspaciosForzadosFuente_tb.select(
      $"MARCA",
      $"CATEGORIA",
      $"TIPOFORZADO",
      $"VINCULOESPACIO"
    )

    /** Se guarda el números de espacios total **/
    val campaniaExpoEspacios_df = arpParametrosFuente_df.select(
      $"ANIOCAMPANAEXPO",
      $"ESPACIOS",
      $"ESPACIOSTOP"
    ).distinct()

    /** Se eligen productos sin regalo para los cálculos - PARA ESTIMACIÓN **/
    val productosTotales_df = arpParametrosFuente_df.select(
      $"TIPOTACTICA",
      $"CODTACTICA",
      $"CODCUC",
      $"UNIDADES",
      $"PRECIOOFERTA",
      $"CODVENTA",
      $"INDICADORPADRE",
      $"FLAGTOP"
    ).distinct()

    (
      listadoProductos_df, listadoRegalos_df,
      espaciosForzados_df, campaniaExpoEspacios_df,
      productosTotales_df
    )
  }

}
