package pe.com.belcorp.arp.tables.impl

import org.apache.spark.sql.{ColumnName, DataFrame, SparkSession}
import org.apache.spark.sql.expressions.Window
import org.apache.spark.sql.functions._
import pe.com.belcorp.arp.tables.Constants
import pe.com.belcorp.arp.tables.impl.blocks.ReglasNegocio
import pe.com.belcorp.arp.utils.Extensions._
import pe.com.belcorp.arp.utils.ProcessParams

class ReglasNegocioODD(params: ProcessParams, arpParametros: DataFrame, arpEspaciosForzados: DataFrame,
                       dwhDproducto: DataFrame, dwhDmatrizcampana: DataFrame, arpListadoProbabilidades: Option[DataFrame],
                       arpParametrosDiaInicio: DataFrame, dwhDEbelista: DataFrame,
                       arpListadoVariablesProductos: Option[DataFrame], tipoPersonalizacion: String)
  extends ReglasNegocio(params, arpParametros, arpEspaciosForzados, dwhDproducto, dwhDmatrizcampana, tipoPersonalizacion) {


  val columnsName: Seq[ColumnName] = Seq($"CodPais", $"TipoPersonalizacion", $"AnioCampanaVenta", $"CodEbelista", $"CodCUC", $"CodSAP", $"CodVenta", $"ZonaPortal", $"DiaInicio", $"DiaFin", $"Orden", $"FlagManual",
    $"TipoARP", $"CodVinculo", $"PPU", $"LimUnidades", $"FlagUltMinuto", $"Perfil")

  def getExists(spark: SparkSession): DataFrame = {

    params.tipoARP() match {
      case Constants.ARP_ESTABLECIDAS => listadoInterfazFinalExistsEstablecidas.select(columnsName: _*)
      case Constants.ARP_NUEVAS => listadoInterfazFinalExistsNuevas.select(columnsName: _*)
    }
  }

  def getNotExists(spark: SparkSession): DataFrame = {

    params.tipoARP() match {
      case Constants.ARP_ESTABLECIDAS => listadoInterfazFinalNotExistsEstablecidas.select(columnsName: _*)
      case Constants.ARP_NUEVAS => listadoInterfazFinalNotExistsNuevas.select(columnsName: _*)
    }
  }

  def listadoConsultoraTotalODD: DataFrame = {
    arpListadoProbabilidades
      .get
      .where($"TipoPersonalizacion" === tipoPersonalizacion &&
        $"CodPais" === params.codPais() &&
        $"AnioCampanaProceso" === params.anioCampanaProceso() &&
        $"AnioCampanaExpo" === params.anioCampanaExpo() &&
        $"TipoARP" === params.tipoARP() &&
        $"FlagMC" === params.flagMC() &&
        $"Perfil" === params.perfil())
      .select("CodEbelista", "CodTactica", "FlagTop", "Probabilidad")
      .distinct()
  }

  /** Establecidas **/

  def temporalODDEstablecidas: DataFrame = {
    listadoConsultoraTotalODD
      .select($"CodEbelista", $"CodTactica", $"Probabilidad", row_number().over(Window
        .partitionBy($"CodEbelista")
        .orderBy(desc("Probabilidad"))).alias("Posicion"))
      .orderBy($"CodEbelista", $"Probabilidad".desc)
      .distinct()
  }

  def listaRecomendadosODDEstablecidas: DataFrame = {
    temporalODDEstablecidas
      .where($"Posicion" <= numEspacios)
      .select($"CodEbelista", $"CodTactica", $"Probabilidad", lit(0).alias("Orden"))
      .distinct()
  }

  def listadoODDEstablecidas: DataFrame = {
    listaRecomendadosODDEstablecidas
      .where($"Posicion" === 3)
      .select($"CodEbelista", $"CodTactica", $"Probabilidad", row_number().over(Window
        .partitionBy("CodEbelista")
        .orderBy("Probabilidad")).alias("Orden"))
  }

  def listadoInterfazFinalExistsEstablecidas: DataFrame = {
    listadoODDEstablecidas.alias("lODDEst")
      .join(productosCUC.alias("pCUC"), $"lODDEst.CodTactica" === $"pCUC.CodTactica" && $"pCUC.IndicadorPadre" === 1, "Inner")
      .join(dwhDEbelista.alias("dwhBelista"), $"lODDEst.CodEbelista" === $"dwhBelista.CodEbelista" && $"dwhBelista.CodPais" === params.codPais(), "Inner")
      .join(arpParametrosDiaInicio.alias("arpPD"), $"lODDEst.Orden" === $"arpPD.Orden" && $"arpPD.CodPais" === params.codPais() && $"arpPD.AnioCampanaVenta" === params.anioCampanaExpo() &&
        $"arpPD.TipoPersonalizacion" === "ODD" && $"arpPD.Estado" === 1 && $"TipoARP" === params.tipoARP(), "Inner")
      .select($"CodPais".alias("CodPais"), lit("ODD").alias("Tipo"), $"AnioCampanaExpo".alias("AnioCampana Venta"),
        $"CodEbelista", $"CodProducto", $"CodSAP", $"CodVenta", lit("IDP").alias("Portal"), $"arpPD.DiaInicio", lit(0).alias("DiaFin"),
        $"arpPD.OrdenxDia", lit(0).alias("FlagManual"), lit("E").alias("TipoARP"), lit(0).alias("CodVinculo"), lit(0.0).alias("PPU"), $"pCUC.LimUnidades", $"pCUC.FlagUltMinuto", $"Perfil")
      .distinct()
  }

  def listadoInterfazFinalNotExistsEstablecidas: DataFrame = {
    listadoODDEstablecidas.alias("lODDEst")
      .join(productosCUC.alias("pCUC"), $"lODDEst.CodTactica" === $"pCUC.CodTactica" && $"pCUC.IndicadorPadre" === 1, "Inner")
      .join(dwhDEbelista.alias("dwhBelista"), $"lODDEst.CodEbelista" === $"dwhBelista.CodEbelista" && $"dwhBelista.CodPais" === params.codPais(), "Inner")
      .select($"CodPais".alias("CodPais"), lit("ODD").alias("Tipo"), $"AnioCampanaExpo".alias("AnioCampana Venta"),
        $"CodEbelista", $"CodProducto", $"CodSAP", $"CodVenta", lit("IDP").alias("Portal"),
        when($"Orden" === 1, 0).otherwise(when($"Orden" === 2, -1).otherwise(when($"Orden" === 3, 1).otherwise(
          when($"Orden" === 4, 2).otherwise(when($"Orden" === 5, 3).otherwise(when($"Orden" === 6, 4)))))).alias("DiaIni"),
        lit(0).alias("DiaFin"), $"Orden", lit(0).alias("FlagManual"), lit("E").alias("TipoARP"),
        lit(0).alias("CodVinculo"), lit(0.0).alias("PPU"), $"pCUC.LimUnidades", $"pCUC.FlagUltMinuto", $"Perfil")
      .distinct()
  }

  /** Nuevas **/

  def temporalODDNuevas: DataFrame = {
    val T = listadoConsultoraTotalODD
      .agg(max("Probabilidad").alias("Probabilidad"))
      .select("CodEbelista", "CodTactica", "Probabilidad")
    T
      .select($"CodEbelista", $"CodTactica", $"Probabilidad", row_number().over(Window
        .partitionBy("CodEbelista")
        .orderBy($"Probabilidad".desc)).alias("Posicion"))
      .orderBy($"CodEbelista", $"Probabilidad".desc)
  }

  def listaRecomendadosODDNuevas: DataFrame = {
    temporalODDNuevas
      .where($"Posicion" <= numEspacios)
      .select($"CodEbelista", $"CodTactica", $"Probabilidad",
        lit(0).alias("Prioridad"), lit(0).alias("Orden"))
  }

  def listadoODDNuevas: DataFrame = {
    listaRecomendadosODDNuevas
      .select($"CodEbelista", $"CodTactica", $"Probabilidad", row_number().over(Window
        .partitionBy($"CodEbelista")
        .orderBy($"Probabilidad".desc)).alias("Orden"))
      .orderBy($"CodEbelista", $"CodTactica")
  }

  def listadoInterfaz: DataFrame = {
    listadoODDNuevas.alias("lODDNuevas")
      .join(productosCUC.alias("pCUC"), $"lODDNuevas.CodTactica" === $"pCUC.CodTactica" && $"pCUC.IndicadorPadre" === 1, "Inner")
      .join(dwhDEbelista.alias("dwhBelista"), $"lODDNuevas.CodEbelista" === $"dwhBelista.CodEbelista" && $"dwhBelista.CodPais" === params.codPais(), "Inner")
      .select($"dwhBelistaCodEbelista", $"pCUC.CodProducto", $"pCUC.CodSAP", $"pCUC.CodVenta", $"Orden", lit(0).alias("CodVinculo"),
        lit(0).alias("PPU"), $"pCUC.LimUnidades", $"pCUC.FlagUltMinuto")
      .distinct()
  }

  def insertListadoInterfaz(): DataFrame = {

    val T = arpListadoVariablesProductos
      .get
      .where($"CodPais" === params.codPais() && $"ANIOCAMPANAPROCESO" === params.anioCampanaProceso() &&
        $"anioCampanaExpo" === params.anioCampanaExpo() && $"TIPOPERSONALIZACION" === "ODD")
      .agg(max($"ProbabilidadCompra").alias("ProbabilidadCompra"))
      .select(lit("XXXXXXXXX").alias("CodEbelista"), $"CodTactica", $"ProbabilidadCompra")

    val A = T
      .select($"CodEbelista", $"CodTactica", $"ProbabilidadCompra", row_number().over(Window
        .orderBy("ProbabilidadCompra")).alias("Orden"))

    A.alias("A")

    val B = productosCUC
    A
      .join(productosCUC, $"A.CodTactica" === $"B.Codtactica" && $"B.IndicadorPadre" === 1)
      .select($"A.CodEbelista", $"B.CodProducto", $"B.CodSAP", $"B.CodVenta", $"Orden", lit(0).alias("CodVinculo"),
        lit(0).alias("PPU"), $"B.LimUnidades", $"B.FlagUltMinuto")
      .distinct()

    B.alias("B")
  }

  //Already exists - True
  def listadoInterfazFinalExistsNuevas: DataFrame = {
    listadoInterfaz.alias("lfaz")
    arpParametrosDiaInicio.alias("arpPDI")

    listadoInterfaz
      .join(arpParametrosDiaInicio, $"lfaz.Orden" === $"arpPDI.Orden" && $"arpPDI.CodPais" === params.codPais() && $"arpPDI.AnioCampanaVenta" === params.anioCampanaExpo() &&
        $"arpPDI.TipoPersonalizacion" === "ODD" && $"arpPDI.Estado" === 1 && $"TipoARP" === params.tipoARP(), "Inner")
      .select($"CodPais".alias("CodPais"), lit("ODD").alias("Tipo"), $"AnioCampanaExpo".alias("AnioCampana Venta"),
        $"CodEbelista", $"CodProducto", $"CodSAP", $"CodVenta", lit("IDP").alias("Portal"), $"arpPDI.DiaInicio", lit(0).alias("DiaFin"),
        $"arpPDI.OrdenxDia", lit(0).alias("FlagManual"), lit("N").alias("TipoARP"), $"CodVinculo", $"PPU",
        $"LimUnidades", $"FlagUltMinuto", $"Perfil")
      .distinct()
  }

  //Not exists - False
  def listadoInterfazFinalNotExistsNuevas: DataFrame = {
    listadoInterfaz
      .select($"CodPais".alias("CodPais"), lit("ODD").alias("Tipo"), $"AnioCampanaExpo".alias("AnioCampana Venta"),
        $"CodEbelista", $"CodProducto", $"CodSAP", $"CodVenta", lit("IDP").alias("Portal"),
        when($"Orden" === 1, 0).otherwise(when($"Orden" === 2, -1).otherwise(when($"Orden" === 3, 1).otherwise(
          when($"Orden" === 4, 2).otherwise(when($"Orden" === 5, 3).otherwise(when($"Orden" === 6, 4)))))).alias("DiaIni"),
        lit(0).alias("DiaFin"), lit(0).alias("Orden"), $"FlagManual", lit("N").alias("TipoARP"),
        $"CodVinculo", $"PPU", $"LimUnidades", $"FlagUltMinuto", $"Perfil")
      .distinct()
  }

}
