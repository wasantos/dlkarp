package pe.com.belcorp.arp.tables.impl

import org.apache.spark.sql.expressions.Window
import org.apache.spark.sql.functions._
import org.apache.spark.sql.{DataFrame, SparkSession}
import org.apache.spark.storage.StorageLevel
import pe.com.belcorp.arp.tables.Constants
import pe.com.belcorp.arp.tables.impl.blocks._
import pe.com.belcorp.arp.utils.Extensions._
import pe.com.belcorp.arp.utils.{AnioCampana, ProcessParams}

class RFM(params: ProcessParams, dwhDebelista: DataFrame,
  dwhDGeografiaCampana: DataFrame, dwhDMatrizCampana: DataFrame,
  dwhDProducto: DataFrame, dwhDTipoOferta: DataFrame,
  dwhFStaEbeCam: DataFrame, dwhFVtaProEbeCam: DataFrame,
  listadoProductos: DataFrame, baseConsultoras: DataFrame) {

  val acProceso: AnioCampana = AnioCampana.parse(params.anioCampanaProceso())

  type Results = (Option[DataFrame], Option[DataFrame], Option[DataFrame], Option[DataFrame])

  def get(spark: SparkSession): Results = {
    if (params.tipoARP() == Constants.ARP_ESTABLECIDAS) {
      processEstabilished(spark)
    } else {
      processNew(spark)
    }
  }

  private def processEstabilished(spark: SparkSession): Results = {
    val (baseConsultorasFiltrada, _, productos, _) =
      commonTables(spark)

    val tmpFStaEbeCam24AC = new FStaEbeCamLimitado(params, dwhFStaEbeCam, acProceso - 23)
      .get(spark)

    // Obtengo el estado de las consultoras que conforman
    // la base en las últimas 24 campañas
    val baseEstadoConsultoras = new BaseEstadoConsultoras(
      params, baseConsultorasFiltrada, tmpFStaEbeCam24AC).get(spark)

    // Se crea la tabla temporal donde se guardan
    // la venta de las últimas 24 campañas
    val tmpFVtaProEbeCam24AC = new FVtaProEbeCamLimitado(
      params, dwhDTipoOferta, baseConsultoras,
      dwhFVtaProEbeCam, productos, acProceso - 23
    ).get(spark)

    /** 1.1. Táctica Individual **/

    /** 1.1.0. Productos Individual **/

    // A nivel de Item
    val productosIndividual = productos
      .where($"TipoTactica" === "Individual")
      .select("*")

    // A nivel CUC
    val productosCUCIndividual = productosIndividual
      .where($"DesProducto".isNotNull)
      .groupBy(
        $"CodTactica", $"TipoTactica", $"CodProducto",
        $"Unidades", $"PrecioOferta", $"FlagTop")
      .agg(max($"DesProducto").as("DesProducto"))

    /** 1.1.1. Información de las U24C (Últimas 24 campañas) **/
    val tmpFVtaProEbeCam24ACProducto = {
      val fv = tmpFVtaProEbeCam24AC.alias("fv")
      val pi = productosIndividual.alias("pi")

      fv.join(pi, makeEquiJoin("fv", "pi", Seq("CodSAP")))
        .select($"fv.*", $"pi.CodTactica", $"pi.CodProducto")
    }

    /** 1.1.2. Información de las U6C (Últimas 6 campañas) **/
    val venta6AC = tmpFVtaProEbeCam24ACProducto
      .where($"AnioCampana".between(
        (acProceso - 5).toString, acProceso.toString))
      .groupBy($"CodEbelista", $"CodTactica", $"CodProducto")
      .agg(sum($"RealVtaMNNeto").as("VentaAcumulada"))
      .select("*")
      .where($"VentaAcumulada" > 0)

    /** 1.1.3. Información de las U6C AA (Últimas 6 campañas del año anterior) **/
    val venta6ACAnioAnterior = tmpFVtaProEbeCam24ACProducto
      .where($"AnioCampana".between(
        (acProceso - 23).toString, (acProceso - 18).toString))
      .groupBy($"CodEbelista", $"CodTactica", $"CodProducto")
      .agg(sum($"RealVtaMNNeto").as("VentaAcumulada"))
      .select("*")
      .where($"VentaAcumulada" > 0)

    /** 1.1.4. Información de las PU6C (Penúltimas 6 campañas) **/
    val venta6ACPenultimas = tmpFVtaProEbeCam24ACProducto
      .where($"AnioCampana".between(
        (acProceso - 11).toString, (acProceso - 6).toString))
      .groupBy($"CodEbelista", $"CodTactica", $"CodProducto")
      .agg(sum($"RealVtaMNNeto").as("VentaAcumulada"))
      .select("*")
      .where($"VentaAcumulada" > 0)

    val venta24AC = {
      // Frecuencia, Recencia, Venta Acumulada y Venta Promedio U24C
      tmpFVtaProEbeCam24ACProducto
        .groupBy($"CodEbelista", $"CodTactica", $"CodProducto")
        .agg(
          countDistinct($"AnioCampana").as('Frecuencia),
          AnioCampana.Spark.delta(lit(acProceso.toString), max($"AnioCampana"))
            .as("Recencia"),
          sum($"RealVtaMNNeto").as("VentaAcumulada"),
          (sum($"RealVtaMNNeto") / countDistinct($"AnioCampana").cast("double"))
            .as("VentaPromedio"),
          lit(0d).as("PrecioOptimo"),
          lit(0d).as("Gatillador"))
        .select("*")
        .where($"VentaAcumulada" > 0)
    }

    val venta24ACGatillador = {
      /** 1.1.5. Cálculo del Gatillador =
        * (N° Pedidos CUC)/(N° Campañas aspeadas CUC) (Últimas 24 campañas) **/

      // N° Pedidos CUC
      val venta24ACPedidos = {
        val fv = tmpFVtaProEbeCam24ACProducto.alias("fv")
        val pi = productosCUCIndividual.alias("pi")

        val agregado = fv
          .join(pi, makeEquiJoin("fv", "pi", Seq("CodTactica", "CodProducto")))
          .where($"fv.RealUUVendidas" >= $"Unidades")
          .groupBy($"fv.CodEbelista", $"fv.CodTactica", $"fv.CodSAP",
            $"fv.CodProducto", $"fv.AnioCampana")
          .agg(sum($"fv.RealVtaMNNeto").as("VentaAcumulada"))
          .select("*")
          .where($"VentaAcumulada" > 0)

        agregado
          .groupBy($"CodEbelista", $"CodTactica", $"CodProducto")
          .agg(countDistinct($"AnioCampana").as("NumPedidosCUC"))
          .select("*")
      }

      // N° Campañas aspeadas CUC
      val aspeos24AC = {
        val fv = tmpFVtaProEbeCam24ACProducto.alias("fv")
        val dm = dwhDMatrizCampana.alias("dm")

        fv.join(dm, makeEquiJoin("fv", "dm", Seq(
          "CodSAP", "AnioCampana", "CodTipoOferta", "CodVenta")))
          .where($"dm.CodPais" === params.codPais())
          .select($"fv.CodProducto", $"dm.AnioCampana")
          .distinct()
      }

      // Se contabiliza el total de campañas en la que la consultora pasó pedido
      val pedidos24AC = baseEstadoConsultoras
        .where($"FlagPasoPedido" === 1)
        .select($"CodEbelista", $"AnioCampana")
        .distinct()

      val aspeos = {
        val bv = venta24AC.alias("bv")
        val as = aspeos24AC.alias("as")
        val pd = pedidos24AC.alias("pd")

        bv.join(as, makeEquiJoin("bv", "as", Seq("CodProducto")))
          .join(pd,
            $"pd.CodEbelista" === $"bv.CodEbelista" &&
              $"pd.AnioCampana" === $"as.AnioCampana",
            "inner")
          .groupBy($"bv.CodEbelista", $"bv.CodTactica", $"bv.CodProducto")
          .agg(countDistinct($"pd.AnioCampana").as("CampaniasAspeadas"))
      }

      // Cálculo del Gatillador
      val updateVenta = {
        val bv = venta24AC.as("bv")
        val pv = venta24ACPedidos.as("pv")
        val as = aspeos.as("as")

        bv
          .join(pv,
            makeEquiJoin("bv", "pv", Seq(
              "CodEbelista", "CodTactica", "CodProducto")))
          .join(as,
            makeEquiJoin("bv", "as", Seq(
              "CodEbelista", "CodTactica", "CodProducto")))
          .select(
            $"bv.CodEbelista", $"bv.CodTactica", $"bv.CodProducto",
            ($"pv.NumPedidosCUC".cast("double") / $"as.CampaniasAspeadas")
              .as("Gatillador"))
      }

      venta24AC.updateFrom(updateVenta,
        Seq("CodEbelista", "CodTactica", "CodProducto"),
        "Gatillador")
    }


    /** 1.1.7. Precio Óptimo **/

    // Obtengo los productos vendidos en las últimas 24 campañas
    val poVenta = tmpFVtaProEbeCam24ACProducto.alias("poVenta")
    val poProdPorConsultora = poVenta
      .select($"CodEbelista", $"CodTactica", $"CodProducto", $"CodSAP")
      .distinct()

    // Obtengo la matriz de venta de los productos propuestos
    val poCatalogo = {
      val dm = dwhDMatrizCampana.alias("dm")
      val pi = productosIndividual.alias("pi")
      val dt = dwhDTipoOferta.alias("dt")

      dm.join(pi, makeEquiJoin("dm", "pi", Seq("CodSAP")))
        .join(dt, makeEquiJoin("dm", "dt", Seq("CodTipoOferta")))
        .where(
          $"dm.AnioCampana".between((acProceso - 23).toString, acProceso.toString) &&
            $"dm.PrecioOferta" > 0 && $"dt.CodTipoProfit" === "01" &&
            not($"dt.CodTipoOferta".isin("030", "040", "051")) &&
            $"dm.VehiculoVenta".isin("REVISTA", "CATÁLOGO", "CATALOGO") &&
            $"dt.CodPais" === params.codPais() && $"dm.CodPais" === params.codPais())
        .select(
          $"pi.CodTactica", $"pi.CodProducto", $"dm.*")
    }

    // Se arma la tabla de Precio Óptimo
    // Se cuentan las campañas en las que se muestra el producto a
    // dicho precio y la consultora ha pasado pedido
    val precioOptimo = {
      val pp = poProdPorConsultora.alias("pp")
      val pc = poCatalogo.alias("pc")
      val bc = baseEstadoConsultoras.alias("bc")

      pp.join(pc, makeEquiJoin("pp", "pc", Seq("CodSAP")))
        .join(bc, $"bc.CodEbelista" === $"pp.CodEbelista" &&
          $"bc.AnioCampana" === $"pc.AnioCampana")
        .where($"bc.FlagPasoPedido" === 1)
        .groupBy(
          $"pp.CodEbelista", $"pp.CodTactica", $"pp.CodProducto", $"pc.PrecioOferta")
        .agg(
          lit(0).as("PedidosPuros"), lit(0d).as("Probabilidad"),
          countDistinct($"pc.AnioCampana").as("NumAspeos"))
    }

    // Pedidos puros
    val poPedidosPuros = {
      val pv = poVenta.alias("pv")
      val pc = poCatalogo.alias("pc")

      pv.join(pc, makeEquiJoin("pv", "pc", Seq(
        "AnioCampana", "CodSAP", "CodTipoOferta", "CodVenta")))
        .groupBy(
          $"pv.CodEbelista", $"pv.CodTactica", $"pv.CodProducto", $"pc.PrecioOferta")
        .agg(countDistinct($"pc.AnioCampana").as("PedidosPuros"))
    }

    // Actualiza precio optimo
    val precioOptimoPedidos = {
      val po = precioOptimo.alias("po")

      val updatePo = {
        val pp = poPedidosPuros.alias("pp")

        po.join(pp, makeEquiJoin("po", "pp", Seq(
          "CodEbelista", "CodTactica", "CodProducto", "PrecioOferta")))
          .select(
            $"po.CodEbelista", $"po.CodTactica", $"po.CodProducto", $"po.PrecioOferta",
            $"pp.PedidosPuros", ($"pp.PedidosPuros".cast("double") / $"po.NumAspeos")
              .as("Probabilidad"))
      }

      po.updateFrom(updatePo,
        Seq("CodEbelista", "CodTactica", "CodProducto", "PrecioOferta"),
        "PedidosPuros", "Probabilidad")
    }

    val precioOptimoFinal = precioOptimoPedidos
      .select(
        $"CodEbelista", $"CodTactica", $"CodProducto", $"PrecioOferta",
        $"NumAspeos", $"PedidosPuros", $"Probabilidad",
        row_number().over(
          Window.partitionBy($"CodEbelista", $"CodTactica", $"CodProducto")
            .orderBy($"Probabilidad".desc, $"PedidosPuros".desc, $"PrecioOferta".asc)
        ).as("Posicion"))
      .orderBy($"CodEbelista", $"CodTactica", $"CodProducto", $"PrecioOferta")

    // Actualizo el precio óptimo
    val venta24ACPrecio = {
      val vt = venta24ACGatillador.alias("vt")
      val po = precioOptimoFinal.alias("po")

      val ventaUpdate = vt
        .join(po, makeEquiJoin("vt", "po", Seq(
          "CodEbelista", "CodTactica", "CodProducto")))
        .where($"po.Posicion" === 1)
        .select(
          $"vt.CodEbelista", $"vt.CodTactica", $"vt.CodProducto",
          $"po.PrecioOferta".as("PrecioOptimo"))

      vt.updateFrom(ventaUpdate,
        Seq("CodEbelista", "CodTactica", "CodProducto"),
        "PrecioOptimo")
    }

    /** 1.1.9. Tabla final de Individual **/

    // Creación de la tabla de variables por producto y Consultora
    val listadoConsultoraInicial = {
      val lc = baseConsultorasFiltrada.alias("lc")
      val pc = productosCUCIndividual.alias("pc")

      lc.crossJoin(pc)
        .select(
          lc("*"),$"pc.CodTactica", $"pc.TipoTactica",$"pc.CodProducto", $"pc.DesProducto",
          lit(0d).as("VentaAcumU6C"),
          lit(0d).as("VentaAcumPU6C"),
          lit(0d).as("VentaAcumU6C_AA"),
          lit(0d).as("VentaAcumU24C"),
          lit(0d).as("VentaPromU24C"),
          lit(0d).as("FrecuenciaU24C"),
          lit(0d).as("RecenciaU24C"),
          lit(0).as("FrecuenciaU6C"),

          lit(0d).as("CicloRecompraPotencial"),
          lit(0d).as("BrechaRecompraPotencial"),
          lit(0d).as("VentaPotencialMinU6C"),
          lit(0d).as("GAP"),
          lit(0d).as("BrechaVenta"),
          lit(0d).as("BrechaVenta_MC"),
          lit(0).as("FlagCompra"),
          lit(0d).as("PrecioOptimo"),

          lit(0d).as("GAPPrecioOptimo"),
          lit(0).as("NumAspeos"),
          lit(0d).as("Gatillador"),
          lit(0d).as("FrecuenciaNor"),
          lit(0d).as("RecenciaNor"),
          lit(0d).as("BrechaVentaNor"),
          lit(0d).as("BrechaVenta_MCNor"),
          lit(0d).as("GAPPrecioOptimoNor"),

          lit(0d).as("GatilladorNor"),
          lit(0d).as("Oportunidad"),
          lit(0d).as("OportunidadNor"),
          lit(0d).as("Score"),
          lit(0).as("UnidadesTactica"),
          lit(0d).as("Score_UU"),
          lit(0d).as("Score_MC_UU"),
          lit("A").as("PerfilOficial"),
          lit(0).as("FlagBRP"),
          lit(0).as("FlagVentaU6CMenosAA"),
          lit(0).as("FlagVentaU6CMenosPP"),
          $"pc.FlagTop",
          lit(0d).as("PrecioMinimo"))
        .distinct()
    }

    // Se actualiza la Venta Acumulada de las U6C
    val listadoVentaAcumU6C = {
      val lc = listadoConsultoraInicial.alias("lc")
      val vt = venta6AC.alias("vt")

      lc.join(vt, makeEquiJoin("lc", "vt", Seq(
        "CodEbelista", "CodTactica", "CodProducto")))
        .select(
          $"lc.CodEbelista", $"lc.CodTactica", $"lc.CodProducto",
          $"vt.VentaAcumulada".as("VentaAcumU6C"))
    }

    // Se actualiza la Venta Acumulada de las PU6C
    val listadoVentaAcumPU6C = {
      val lc = listadoConsultoraInicial.alias("lc")
      val vt = venta6ACPenultimas.alias("vt")

      lc.join(vt, makeEquiJoin("lc", "vt", Seq(
        "CodEbelista", "CodTactica", "CodProducto")))
        .select(
          $"lc.CodEbelista", $"lc.CodTactica", $"lc.CodProducto",
          $"vt.VentaAcumulada".as("VentaAcumPU6C"))
    }

    // Se actualiza la Venta Acumulada de las U6C AA
    val listadoVentaAcumU6C_AA = {
      val lc = listadoConsultoraInicial.alias("lc")
      val vt = venta6ACAnioAnterior.alias("vt")

      lc.join(vt, makeEquiJoin("lc", "vt", Seq(
        "CodEbelista", "CodTactica", "CodProducto")))
        .select(
          $"lc.CodEbelista", $"lc.CodTactica", $"lc.CodProducto",
          $"vt.VentaAcumulada".as("VentaAcumU6C_AA"))
    }

    // Se aplica las atualizaciones
    val listadoConsultorasVentas = {
      val lc = listadoConsultoraInicial.alias("lc")
      val vu = listadoVentaAcumU6C.alias("vu")
      val vp = listadoVentaAcumPU6C.alias("vp")
      val va = listadoVentaAcumU6C_AA.alias("va")

      lc
        .updateFrom(vu,
          Seq("CodEbelista", "CodTactica", "CodProducto"), "VentaAcumU6C")
        .updateFrom(vp,
          Seq("CodEbelista", "CodTactica", "CodProducto"), "VentaAcumPU6C")
        .updateFrom(va,
          Seq("CodEbelista", "CodTactica", "CodProducto"), "VentaAcumU6C_AA")
    }

    // Se actualiza la información
    //  (VtaAcum, VtaProm, Gatillador, Recencia, Frecuencia y Precio Óptimo) de las U24C
    val listadoVariablesIndividual = {
      val lc = listadoConsultorasVentas.alias("lc")
      val vt = venta24ACPrecio.alias("vt")

      val updateListado = lc
        .join(vt, makeEquiJoin("lc", "vt", Seq(
          "CodEbelista", "CodTactica", "CodProducto")))
        .select(
          $"lc.CodEbelista", $"lc.CodTactica", $"lc.CodProducto",
          $"vt.VentaAcumulada".as("VentaAcumU24C"),
          $"vt.VentaPromedio".as("VentaPromU24C"),
          $"vt.Gatillador",
          $"vt.Recencia".as("RecenciaU24C"),
          $"vt.Frecuencia".as("FrecuenciaU24C"),
          lit(1).as("FlagCompra"),
          $"vt.PrecioOptimo")

      lc.updateFrom(updateListado, Seq("CodEbelista", "CodTactica", "CodProducto"),
        "VentaAcumU24C", "VentaPromU24C", "Gatillador", "RecenciaU24C",
        "FrecuenciaU24C", "FlagCompra", "PrecioOptimo"
      )
    }

    /** 1.2. Táctica Bundle **/

    /** 1.2.0. Productos Bundle **/

    // A nivel de Item
    val productosBundle = productos
      .where($"TipoTactica" === "Bundle")
      .select("*")

    // A nivel CUC
    val productosCUCBundle = productosBundle
      .where($"DesProducto".isNotNull)
      .groupBy(
        $"CodTactica", $"TipoTactica", $"CodProducto",
        $"Unidades", $"PrecioOferta", $"FlagTop")
      .agg(max($"DesProducto").as("DesProducto"))

    /** 1.2.1. Información de las U24C (Últimas 24 campañas) **/
    val tmpFVtaProEbeCam24ACBundleProducto = {
      val fv = tmpFVtaProEbeCam24AC.alias("fv")
      val pi = productosBundle.alias("pi")

      fv.join(pi, makeEquiJoin("fv", "pi", Seq("CodSAP")))
        .select($"fv.*", $"pi.CodTactica", $"pi.CodProducto")
    }

    // Recencia, Venta Acumulada U24C
    val venta24ACBundle = {
      tmpFVtaProEbeCam24ACBundleProducto
        .groupBy($"CodEbelista", $"CodTactica", $"CodProducto")
        .agg(
          countDistinct($"AnioCampana").as('Frecuencia),
          AnioCampana.Spark.delta(lit(acProceso.toString), max($"AnioCampana"))
            .as("Recencia"),
          sum($"RealVtaMNNeto").as("VentaAcumulada"),
          lit(0d).as("PrecioOptimo"),
          lit(0d).as("CicloRecompraPotencial"))
        .select("*")
        .where($"VentaAcumulada" > 0)
    }

    /** 1.2.2. Información de las U6C (Últimas 6 campañas) **/
    val venta6ACBundle = tmpFVtaProEbeCam24ACBundleProducto
      .where($"AnioCampana".between(
        (acProceso - 5).toString, acProceso.toString))
      .groupBy($"CodEbelista", $"CodTactica", $"CodProducto")
      .agg(sum($"RealVtaMNNeto").as("VentaAcumulada"))
      .select("*")
      .where($"VentaAcumulada" > 0)

    /** 1.2.3. Información de las U6C AA (Últimas 6 campañas del año anterior) **/
    val venta6ACAnioAnteriorBundle = tmpFVtaProEbeCam24ACBundleProducto
      .where($"AnioCampana".between(
        (acProceso - 23).toString, (acProceso - 18).toString))
      .groupBy($"CodEbelista", $"CodTactica", $"CodProducto")
      .agg(sum($"RealVtaMNNeto").as("VentaAcumulada"))
      .select("*")
      .where($"VentaAcumulada" > 0)

    /** 1.2.4. Información de las PU6C (Penúltimas 6 campañas) **/
    val venta6ACPenultimasBundle = tmpFVtaProEbeCam24ACBundleProducto
      .where($"AnioCampana".between(
        (acProceso - 11).toString, (acProceso - 6).toString))
      .groupBy($"CodEbelista", $"CodTactica", $"CodProducto")
      .agg(sum($"RealVtaMNNeto").as("VentaAcumulada"))
      .select("*")
      .where($"VentaAcumulada" > 0)

    val venta24ACBundleConCiclo = {
      val vt = venta24ACBundle.alias("vt")
      val fv = tmpFVtaProEbeCam24ACBundleProducto.alias("fv")

      val updateVenta = fv.groupBy($"CodEbelista", $"CodTactica", $"CodProducto")
        .agg(
          ((lit(23)
            - AnioCampana.Spark.delta(
                min($"AnioCampana"), lit((acProceso - 23).toString))
            - AnioCampana.Spark.delta(
                lit(acProceso.toString), max($"AnioCampana"))
          ).cast("double") / (count($"AnioCampana") - lit(1)))
            .as("CicloRecompraPotencial"),
          sum($"RealVTAMNNeto").as("VentaAcumulada"),
          count($"AnioCampana").as("CampanasValidas"))
        .select("*")
        .where($"VentaAcumulada" > 0 && $"CampanasValidas" > 1)

      vt.updateFrom(updateVenta,
        Seq("CodEbelista", "CodTactica", "CodProducto"),
        "CicloRecompraPotencial")
    }

    /** 1.2.6. Gatillador **/

    // Se guardan las campañas en las que se compraron las ofertas
    // (Campañas en que se compraron todos los productos de la táctica)
    // Se cuentan los productos por táctica
    val totalProductosBundle = productosCUCBundle
      .groupBy($"CodTactica", $"TipoTactica")
      .agg(count($"CodProducto").as("TotalProductos"))

    // Se guardan las campañas donde se compraron todos los productos de la táctica
    // (FVTAPROEBECAMU24C_TacticaBundle)
    val campanasTacticaBundle = {
      val fv = tmpFVtaProEbeCam24ACBundleProducto.alias("fv")
      val tb = totalProductosBundle.alias("tb")

      fv.join(tb, makeEquiJoin("fv", "tb", Seq("CodTactica")))
        .groupBy(
          $"fv.CodEbelista", $"fv.CodTactica", $"fv.AnioCampana", $"tb.TotalProductos")
        .agg(
          sum($"fv.RealVTAMNNeto").as("VentaAcumulada"),
          countDistinct($"fv.CodProducto").as("ProductosVenta"))
        .select("*")
        .where($"VentaAcumulada" > 0 && $"ProductosVenta" === $"TotalProductos")
    }

    val ventaPotencialMinima = campanasTacticaBundle
      .where($"AnioCampana" >= (acProceso - 5).toString)
      .groupBy($"CodEbelista", $"CodTactica")
      .agg(min($"VentaAcumulada").as("VentaMinima"))

    val venta24ACTacticaBundle = {
      val ventaTacticaBundle = campanasTacticaBundle
        .groupBy($"CodEbelista", $"CodTactica")
        .agg(
          lit(0d).as("Gatillador"),
          sum($"VentaAcumulada").as("VentaAcumulada"),
          AnioCampana.Spark.delta(lit(acProceso.toString), max($"AnioCampana"))
            .as("Recencia"),
          countDistinct($"AnioCampana").as("Frecuencia"))

      // Se calculan los pedidos para dos productos
      val pedidosGatillador = {
        val fv = tmpFVtaProEbeCam24ACBundleProducto.alias("fv")
        val pc = productosCUCBundle.alias("pc")
        val tb = totalProductosBundle.alias("tb")

        val venta24ACPedidosBundle1 = fv
          .join(pc, makeEquiJoin("fv", "pc", Seq("CodTactica", "CodProducto")))
          .where($"fv.RealUUVendidas" >= $"Unidades")
          .groupBy(
            $"fv.CodEbelista", $"fv.CodTactica", $"fv.CodProducto",
            $"fv.CodSAP", $"fv.AnioCampana")
          .agg(sum($"fv.RealVTAMNNeto").as("VentaAcumulada"))
          .select("*")
          .where($"VentaAcumulada" > 0)

        val vtmp = venta24ACPedidosBundle1.alias("vtmp")
        val venta24ACPedidosBundle2 = vtmp
          .join(tb, makeEquiJoin("vtmp", "tb", Seq("CodTactica")))
          .groupBy(
            $"vtmp.CodEbelista", $"vtmp.CodTactica", $"vtmp.AnioCampana",
            $"tb.TotalProductos")
          .agg(countDistinct($"vtmp.CodProducto").as("NumProducto"))
          .select("*")
          .where($"NumProducto" === $"TotalProductos")

        venta24ACPedidosBundle2
          .groupBy($"CodEbelista", $"CodTactica")
          .agg(count($"AnioCampana").as("PedidosGatillador"))
          .orderBy($"CodEbelista", $"CodTactica")
      }

      // Se calcula el número de campañas en la que se aspearon
      // los productos juntos de cada táctica
      val aspeosBundle = {
        val pb = productosBundle.alias("pb")
        val dm = dwhDMatrizCampana.alias("dm")
        val tb = totalProductosBundle.alias("tb")

        val campanasAspeosBundle = pb
          .join(dm, makeEquiJoin("pb", "dm", Seq("CodSAP")))
          .join(tb, makeEquiJoin("pb", "tb", Seq("CodTactica")))
          .where($"dm.CodPais" === params.codPais())
          .groupBy($"pb.CodTactica", $"dm.AnioCampana", $"tb.TotalProductos")
          .agg(countDistinct($"pb.CodProducto").as("NumProducto"))
          .select("*")
          .where($"NumProducto" === $"TotalProductos")
          .select($"CodTactica", $"AnioCampana")
          .distinct()

        val ca = campanasAspeosBundle.alias("ca")
        val vt = ventaTacticaBundle.alias("vt")
        val bc = baseEstadoConsultoras.alias("bc")

        vt.join(ca, makeEquiJoin("vt", "ca", Seq("CodTactica")))
          .join(bc, $"vt.CodEbelista" === $"bc.CodEbelista" &&
            $"ca.AnioCampana" === $"bc.AnioCampana")
          .where($"bc.FlagPasoPedido" === 1)
          .groupBy($"vt.CodEbelista", $"vt.CodTactica")
          .agg(countDistinct($"ca.AnioCampana").as("CampaniasAspeadas"))
      }

      val ventaTacticaBundleUpdate = {
        val vt = ventaTacticaBundle.as("vt")
        val as = aspeosBundle.as("as")
        val pg = pedidosGatillador.as("pg")

        vt.join(as, makeEquiJoin("vt", "as", Seq("CodEbelista", "CodTactica")))
          .join(pg, makeEquiJoin("vt", "pg", Seq("CodEbelista", "CodTactica")))
          .select(
            $"vt.CodEbelista", $"vt.CodTactica",
            ($"pg.PedidosGatillador".cast("double") / $"as.CampaniasAspeadas")
              .as("Gatillador"))
      }

      ventaTacticaBundle
        .updateFrom(ventaTacticaBundleUpdate,
          Seq("CodEbelista", "CodTactica"), "Gatillador")
    }


    /** 1.2.6. Precio Óptimo **/

    // Obtengo la matriz de venta de los productos propuestos
    val poCatalogoBundle = {
      val dm = dwhDMatrizCampana.alias("dm")
      val pb = productosBundle.alias("pb")
      val dt = dwhDTipoOferta.alias("dt")

      dm.join(pb, makeEquiJoin("dm", "pb", Seq("CodSAP")))
        .join(dt, makeEquiJoin("dm", "dt", Seq("CodTipoOferta")))
        .where(
          $"dm.AnioCampana".between((acProceso - 23).toString, acProceso.toString) &&
            $"dm.PrecioOferta" > 0 && $"dt.CodTipoProfit" === "01" &&
            not($"dt.CodTipoOferta".isin("030", "040", "051")) &&
            $"dt.CodPais" === params.codPais() && $"dm.CodPais" === params.codPais())
        .select(
          $"pb.CodTactica", $"pb.CodProducto", $"dm.*")
    }

    // Obtengo los productos vendidos en las últimas 24 campañas
    val poVentaBundle = tmpFVtaProEbeCam24ACBundleProducto.alias("poVentaBundle")
    val poProdPorConsultoraBundle = {
      val bm = poVentaBundle
        .select($"CodEbelista", $"CodTactica", $"CodProducto", $"CodSAP")
        .distinct()
        .alias("bm")

      val pc = poCatalogoBundle.alias("pc")
      bm.join(pc, makeEquiJoin("bm", "pc", Seq("CodSAP")))
        .select(
          $"bm.CodEbelista", $"bm.CodTactica", $"bm.CodProducto",
          $"pc.PrecioOferta", $"pc.AnioCampana")
    }

    // Se arma la tabla de Precio Óptimo
    // Se cuentan las campañas en las que se muestra el producto a
    // dicho precio y la consultora ha pasado pedido
    val precioOptimoBundle = {
      val pp = poProdPorConsultoraBundle.alias("pp")
      val bc = baseEstadoConsultoras.alias("bc")

      pp.join(bc, makeEquiJoin("pp", "bc", Seq("CodEbelista", "AnioCampana")))
        .where($"bc.FlagPasoPedido" === 1)
        .groupBy(
          $"pp.CodEbelista", $"pp.CodTactica", $"pp.CodProducto", $"pp.PrecioOferta")
        .agg(
          lit(0).as("PedidosPuros"), lit(0d).as("Probabilidad"),
          countDistinct($"pp.AnioCampana").as("NumAspeos"))
    }

    // Pedidos puros
    val poPedidosPurosBundle = {
      val pv = poVenta.alias("pv")
      val pc = poCatalogo.alias("pc")

      pv.join(pc, makeEquiJoin("pv", "pc", Seq("AnioCampana", "CodSAP", "CodTipoOferta", "CodVenta")))
        .groupBy(
          $"pv.CodEbelista", $"pv.CodTactica", $"pv.CodProducto", $"pc.PrecioOferta")
        .agg(countDistinct($"pc.AnioCampana").as("PedidosPuros"))
    }

    // Actualiza precio optimo
    val precioOptimoBundlePedidos = {
      val po = precioOptimoBundle.alias("po")

      val updatePo = {
        val pp = poPedidosPurosBundle.alias("pp")

        po
          .join(pp, makeEquiJoin("po", "pp", Seq(
            "CodEbelista", "CodTactica", "CodProducto", "PrecioOferta")))
          .select(
            $"po.CodEbelista", $"po.CodTactica", $"po.CodProducto", $"po.PrecioOferta",
            $"pp.PedidosPuros", ($"pp.PedidosPuros".cast("double") / $"po.NumAspeos")
              .as("Probabilidad"))
      }

      po.updateFrom(updatePo,
        Seq("CodEbelista", "CodTactica", "CodProducto", "PrecioOferta"),
        "PedidosPuros", "Probabilidad")
    }

    val precioOptimoBundleFinal = precioOptimoBundlePedidos
      .select(
        $"CodEbelista", $"CodTactica", $"CodProducto", $"PrecioOferta",
        row_number().over(
          Window.partitionBy($"CodEbelista", $"CodTactica", $"CodProducto")
            .orderBy($"Probabilidad".desc, $"PedidosPuros".desc, $"PrecioOferta".asc)
        ).as("Posicion"))
      .orderBy($"CodEbelista", $"CodTactica", $"CodProducto", $"PrecioOferta")

    // Actualizo el precio óptimo
    val venta24ACBundleCompleto = {
      val vt = venta24ACBundleConCiclo.alias("vt")
      val po = precioOptimoBundleFinal.alias("po")

      val ventaUpdate = vt.join(po, makeEquiJoin("vt", "po", Seq("CodEbelista", "CodTactica", "CodProducto")))
        .where($"po.Posicion" === 1)
        .select(
          $"vt.CodEbelista", $"vt.CodTactica", $"vt.CodProducto",
          $"po.PrecioOferta".as("PrecioOptimo"))

      vt.updateFrom(ventaUpdate,
        Seq("CodEbelista", "CodTactica", "CodProducto"),
        "PrecioOptimo")
    }

    val poPrecioMinimoBundle = precioOptimoBundlePedidos
      .groupBy($"CodTactica", $"CodProducto")
      .agg(min($"PrecioOferta").as("PrecioMinimo"))

    // SELECT CodTactica, CodProducto,
    //        MIN(PrecioOferta) PrecioMinimo
    // INTO #PO_PrecioMinimo_Bundle
    // FROM #PrecioOptimo_Bundle
    // GROUP BY CodTactica, CodProducto

    /** 1.2.7. Tabla final de Bundle **/

    // Creación de la tabla de variables por producto y Consultora
    val listadoConsultoraBundleInicial = {
      val lc = baseConsultorasFiltrada.alias("lc")
      val pc = productosCUCBundle.alias("pc")

      val listadoBase = lc.crossJoin(pc)
        .select(
          $"lc.*",$"pc.CodTactica", $"pc.TipoTactica",$"pc.CodProducto", $"pc.DesProducto",
          lit(0d).as("VentaAcumU6C"),
          lit(0d).as("VentaAcumPU6C"),
          lit(0d).as("VentaAcumU6C_AA"),
          lit(0d).as("VentaAcumU24C"),
          lit(0d).as("VentaPromU24C"),
          lit(0d).as("FrecuenciaU24C"),
          lit(0d).as("RecenciaU24C"),
          lit(0).as("FrecuenciaU6C"),

          lit(0d).as("CicloRecompraPotencial"),
          lit(0d).as("BrechaRecompraPotencial"),
          lit(0d).as("VentaPotencialMinU6C"),
          lit(0d).as("GAP"),
          lit(0d).as("BrechaVenta"),
          lit(0d).as("BrechaVenta_MC"),
          lit(0).as("FlagCompra"),
          lit(0d).as("PrecioOptimo"),

          lit(0d).as("GAPPrecioOptimo"),
          lit(0).as("NumAspeos"),
          lit(0d).as("Gatillador"),
          lit(0d).as("FrecuenciaNor"),
          lit(0d).as("RecenciaNor"),
          lit(0d).as("BrechaVentaNor"),
          lit(0d).as("BrechaVenta_MCNor"),
          lit(0d).as("GAPPrecioOptimoNor"),

          lit(0d).as("GatilladorNor"),
          lit(0d).as("Oportunidad"),
          lit(0d).as("OportunidadNor"),
          lit(0d).as("Score"),
          lit(0).as("UnidadesTactica"),
          lit(0d).as("Score_UU"),
          lit(0d).as("Score_MC_UU"),
          lit("A").as("PerfilOficial"),
          lit(0).as("FlagBRP"),
          lit(0).as("FlagVentaU6CMenosAA"),
          lit(0).as("FlagVentaU6CMenosPP"),
          $"pc.FlagTop")
        .distinct()

      // Actualiza precio minimo
      val lb = listadoBase.alias("lb")
      val pm = poPrecioMinimoBundle.alias("pm")

      lb.join(pm,
        $"lb.CodTactica" === $"pm.CodTactica" &&
          $"lb.CodProducto" === $"pm.CodProducto", "left")
        .select(
          $"lb.*",
          coalesce($"pm.PrecioMinimo", lit(0d)).as("PrecioMinimo"))
    }

    // Se actualiza la Venta Acumulada de las U6C
    val listadoVentaAcumU6CBundle = {
      val lc = listadoConsultoraBundleInicial.alias("lc")
      val vt = venta6ACBundle.alias("vt")

      lc.join(vt, makeEquiJoin("lc", "vt", Seq("CodEbelista", "CodTactica", "CodProducto")))
        .select(
          $"lc.CodEbelista", $"lc.CodTactica", $"lc.CodProducto",
          $"vt.VentaAcumulada".as("VentaAcumU6C"))
    }

    // Se actualiza la Venta Acumulada de las PU6C
    val listadoVentaAcumPU6CBundle = {
      val lc = listadoConsultoraBundleInicial.alias("lc")
      val vt = venta6ACPenultimasBundle.alias("vt")

      lc.join(vt, makeEquiJoin("lc", "vt", Seq("CodEbelista", "CodTactica", "CodProducto")))
        .select(
          $"lc.CodEbelista", $"lc.CodTactica", $"lc.CodProducto",
          $"vt.VentaAcumulada".as("VentaAcumPU6C"))
    }

    // Se actualiza la Venta Acumulada de las U6C AA
    val listadoVentaAcumU6C_AABundle = {
      val lc = listadoConsultoraBundleInicial.alias("lc")
      val vt = venta6ACAnioAnteriorBundle.alias("vt")

      lc.join(vt, makeEquiJoin("lc", "vt", Seq("CodEbelista", "CodTactica", "CodProducto")))
        .select(
          $"lc.CodEbelista", $"lc.CodTactica", $"lc.CodProducto",
          $"vt.VentaAcumulada".as("VentaAcumU6C_AA"))
    }

    // Se aplica las atualizaciones
    val listadoConsultorasBundleVentasPrevias = {
      val lc = listadoConsultoraBundleInicial.alias("lc")
      val vu = listadoVentaAcumU6CBundle.alias("vu")
      val vp = listadoVentaAcumPU6CBundle.alias("vp")
      val va = listadoVentaAcumU6C_AABundle.alias("va")

      lc
        .updateFrom(vu,
          Seq("CodEbelista", "CodTactica", "CodProducto"), "VentaAcumU6C")
        .updateFrom(vp,
          Seq("CodEbelista", "CodTactica", "CodProducto"), "VentaAcumPU6C")
        .updateFrom(va,
          Seq("CodEbelista", "CodTactica", "CodProducto"), "VentaAcumU6C_AA")
    }

    // Se actualiza la información de las U24C
    val listadoConsultorasBundleVentas24AC = {
      val lc = listadoConsultorasBundleVentasPrevias.alias("lc")
      val vt = venta24ACBundleCompleto.alias("vt")

      val updateListado = lc.join(vt, makeEquiJoin("lc", "vt", Seq("CodEbelista", "CodTactica", "CodProducto")))
        .select(
          $"lc.CodEbelista", $"lc.CodTactica", $"lc.CodProducto",
          $"vt.VentaAcumulada".as("VentaAcumU24C"),
          $"vt.CicloRecompraPotencial",
          ($"vt.CicloRecompraPotencial" - $"vt.Recencia")
            .as("BrechaRecompraPotencial"),
          $"vt.PrecioOptimo")

      lc.updateFrom(updateListado,
        Seq("CodEbelista", "CodTactica", "CodProducto"),
        "VentaAcumU24C", "CicloRecompraPotencial",
          "BrechaRecompraPotencial","PrecioOptimo")
    }

    // Se actualiza la Venta Potencial Mínima de las U6C
    val listadoConsultorasBundleVentaMinima = {
      val lc = listadoConsultorasBundleVentas24AC.alias("lc")
      val vm = ventaPotencialMinima.alias("vm")

      val updateListado = lc.join(vm, makeEquiJoin("lc", "vm", Seq("CodEbelista", "CodTactica")))
        .select(
          $"lc.CodEbelista", $"lc.CodTactica",
          $"vm.VentaMinima".as("VentaPotencialMinU6C"))

      lc.updateFrom(updateListado,
        Seq("CodEbelista", "CodTactica"), "VentaPotencialMinU6C")
    }

    // Se actualiza la Venta Acumulada de las U24C a nivel de Táctica
    val listadoVariablesBundle = {
      val lc = listadoConsultorasBundleVentaMinima.alias("lc")
      val vt = venta24ACTacticaBundle.alias("vt")

      val updateListado = lc.join(vt, makeEquiJoin("lc", "vt", Seq("CodEbelista", "CodTactica")))
        .select(
          $"lc.CodEbelista", $"lc.CodTactica",
          ($"vt.VentaAcumulada"/$"vt.Frecuencia").as("VentaPromU24C"),
          $"vt.Gatillador".as("Gatillador"),
          $"vt.Recencia".as("RecenciaU24C"),
          $"vt.Frecuencia".as("FrecuenciaU24C"),
          lit(1).as("FlagCompra"))

      lc.updateFrom(updateListado,
        Seq("CodEbelista", "CodTactica"),
        "VentaPromU24C", "Gatillador", "RecenciaU24C",
        "FrecuenciaU24C", "FlagCompra")
    }

    (
      Some(listadoVariablesIndividual),
      Some(listadoVariablesBundle),
      None,
      None
    )
  }

  def processNew(spark: SparkSession): Results = {
    val (_, llavesConsultoras, productos, productosCUC) =
      commonTables(spark)

    /** 2. RFM para Nuevas **/

    // Se obtienen las consultoras que hayan comprado los productos
    // listados en las últimas 18 campañas y que estos hayan sido aspeados

    val tmpFVtaProEbeCamNuevas = {
      val fv = dwhFVtaProEbeCam.alias("fv")
      val dm = dwhDMatrizCampana.alias("dm")
      val dt = dwhDTipoOferta.alias("dt")
      val pd = productos.alias("pd")

      fv.join(pd, makeEquiJoin("fv", "pd", Seq("CodSAP")))
        .join(dm, makeEquiJoin("fv", "dm", Seq("AnioCampana", "CodSAP")))
        .join(dt, makeEquiJoin("fv", "dt", Seq("CodTipoOferta")))
        .where(
          $"fv.CodPais" === $"dm.CodPais" && $"fv.CodPais" === $"dt.CodPais" &&
            $"fv.CodPais" === params.codPais() &&
            $"fv.AnioCampana".between((acProceso - 17).toString, acProceso.toString) &&
            $"fv.AnioCampana" === $"fv.AnioCampanaRef" &&
            $"dt.CodTipoProfit" === "01" &&
            not($"dt.CodTipoOferta".isin("030", "040", "051")) &&
            $"fv.RealVtaMNNeto" > 0)
        .select($"fv.AnioCampana", $"fv.CodEbelista", $"pd.CodTactica", $"pd.CodProducto")
        .distinct()
    }

    /** 2.1. Cálculo de variables **/

    /** 2.1.1. Penetración Constantes y Penetración Inconstantes **/

    // Se busca el comportamiento de las consultoras que
    // ingresaron en las últimas18 campañas
    val temporalCI = {
      val fs = dwhFStaEbeCam.alias("fs")
      val de = dwhDebelista.alias("de")

      fs.join(de, makeEquiJoin("fs", "de", Seq("CodPais", "CodEbelista")))
        .where(
          $"fs.CodPais" === params.codPais() &&
            $"de.AnioCampanaIngreso"
              .between((acProceso - 17).toString, acProceso.toString))
        .select(
          $"fs.CodEbelista", $"fs.AnioCampana", $"fs.CodComportamientoRolling",
          $"fs.DescripcionRolling".as("DesNivelComportamiento"),
          $"de.AnioCampanaIngreso")
    }

    // Se halla la última campaña en la que la consultora fue Nueva
    val anioCampanaNuevas = temporalCI
      .where($"DesNivelComportamiento" === "Nuevas")
      .groupBy($"CodEbelista")
      .agg(AnioCampana.Spark.calc(max($"AnioCampana"), lit(1))
        .as("AnioCampanaMasUno"))

    // Se guardan las consultoras que en la campaña consecutiva han sido Constantes 1 o 2
    val consultorasNuevasConstantes = {
      val ci = temporalCI.alias("ci")
      val ac = anioCampanaNuevas.alias("ac")

      ac.join(ci,
        $"ac.CodEbelista" === $"ci.CodEbelista"
          && $"ac.AnioCampanaMasUno" === $"ci.AnioCampana", "inner")
        .where($"ci.DesNivelComportamiento".isin("Constantes 1", "Constantes 2"))
        .select($"ac.CodEbelista", $"ci.AnioCampana")
        .distinct()
    }

    // Se guardan las consultoras que en la campaña consecutiva han sido Inconstantes
    val consultorasNuevasInconstantes = {
      val ci = temporalCI.alias("ci")
      val ac = anioCampanaNuevas.alias("ac")

      ac.join(ci,
        $"ac.CodEbelista" === $"ci.CodEbelista"
          && $"ac.AnioCampanaMasUno" === $"ci.AnioCampana", "inner")
        .where($"ci.DesNivelComportamiento".isin("Inconstantes"))
        .select($"ac.CodEbelista", $"ci.AnioCampana")
        .distinct()
    }

    // Se guardan los totales para utilizarlos en la penetración
    val totalesPenetracion = {
      val cc = consultorasNuevasConstantes
        .withColumn("TipoComportamiento", lit("Constantes"))
      val ci = consultorasNuevasInconstantes
        .withColumn("TipoComportamiento", lit("Inconstantes"))

      cc.unionByName(ci)
        .groupBy($"TipoComportamiento")
        .agg(countDistinct($"CodEbelista").as("TotalConsultoras"))
    }

    // Se guarda la Penetración de las Constantes a nivel de cada táctica, producto
    val penetracionCtes = {
      val cc = consultorasNuevasConstantes.alias("cc")
      val fv = tmpFVtaProEbeCamNuevas.alias("fv")
      val tp = totalesPenetracion.alias("tp")

      cc.join(fv, makeEquiJoin("cc", "fv", Seq("CodEbelista")))
        .join(tp, $"tp.TipoComportamiento" === "Constantes", "inner")
        .where($"fv.AnioCampana".between(
          $"cc.AnioCampana", AnioCampana.Spark.calc($"cc.AnioCampana", lit(2))))
        .groupBy($"fv.CodTactica", $"fv.CodProducto")
        .agg((
          countDistinct($"cc.CodEbelista").cast("double") /
            first($"tp.TotalConsultoras", ignoreNulls = true)).as("Porcentaje"))
    }

    // Se guarda la Penetración de las Inconstantes a nivel de cada táctica, producto
    val penetracionInCtes = {
      val ci = consultorasNuevasInconstantes.alias("ci")
      val fv = tmpFVtaProEbeCamNuevas.alias("fv")
      val tp = totalesPenetracion.alias("tp")

      ci.join(fv, makeEquiJoin("ci", "fv", Seq("CodEbelista")))
        .join(tp, $"tp.TipoComportamiento" === "Inconstantes", "inner")
        .where($"fv.AnioCampana".between(
          $"ci.AnioCampana", AnioCampana.Spark.calc($"ci.AnioCampana", lit(2))))
        .groupBy($"fv.CodTactica", $"fv.CodProducto")
        .agg((
          countDistinct($"ci.CodEbelista").cast("double") /
            first($"tp.TotalConsultoras", ignoreNulls = true)).as("Porcentaje"))
    }

    // Se calcula la frecuencia
    val frecuencia = {
      val lc = llavesConsultoras.alias("lc")
      val fv = tmpFVtaProEbeCamNuevas.alias("fv")

      lc.join(fv, makeEquiJoin("lc", "fv", Seq("CodEbelista")))
        .where($"fv.AnioCampana".between((acProceso - 3).toString, acProceso.toString))
        .groupBy($"lc.CodEbelista", $"fv.CodTactica", $"fv.CodProducto")
        .agg(countDistinct($"fv.AnioCampana").as("Frecuencia"))
    }

    var listadoVariables = {
      val lc = llavesConsultoras.alias("lc")
      val pc = productosCUC.alias("pc")

      lc.crossJoin(pc)
        .select(
          $"lc.CodEbelista",
          $"pc.TipoTactica",
          $"pc.CodTactica",
          $"pc.FlagTop",
          lit(0d).as("Frecuencia"),
          lit(0d).as("PenetracionCtes"),
          lit(0d).as("PenetracionInCtes"),
          $"pc.PrecioOferta",
          lit(0d).as("Penetracion"),
          lit(0d).as("FrecuenciaNor"),
          lit(0d).as("PrecioOfertaNor"),
          lit(0d).as("PenetracionNor"),
          lit(0d).as("Score"),
          lit(0d).as("ScoreNor"),
          $"pc.Unidades".as("UnidadesOferta"),
          lit(0d).as("ProbabilidadCompra"))
    }

    // Actualización de Frecuencia
    listadoVariables = {
      val lv = listadoVariables.alias("lv")
      val fc = frecuencia.alias("fc")

      val updateListado = lv.join(fc, makeEquiJoin("lv", "fc", Seq("CodEbelista", "CodTactica")))
        .select($"lv.CodEbelista", $"lv.CodTactica", $"fc.Frecuencia")

      lv.updateFrom(updateListado,
        Seq("CodEbelista", "CodTactica"), "Frecuencia")
    }

    // Actualización del % Penetración Constantes
    listadoVariables = {
      val lv = listadoVariables.alias("lv")
      val pt = penetracionCtes.alias("pt")

      val updateListado = lv.join(pt, makeEquiJoin("lv", "pt", Seq("CodTactica")))
        .select($"lv.CodTactica", $"pt.Porcentaje".as("PenetracionCtes"))

      lv.updateFrom(updateListado, Seq("CodTactica"), "PenetracionCtes")
    }

    // Actualización del % Penetración Inconstantes
    listadoVariables = {
      val lv = listadoVariables.alias("lv")
      val pt = penetracionInCtes.alias("pt")

      val updateListado = lv.join(pt, makeEquiJoin("lv", "pt", Seq("CodTactica")))
        .select($"lv.CodTactica", $"pt.Porcentaje".as("PenetracionInCtes"))

      lv.updateFrom(updateListado, Seq("CodTactica"), "PenetracionInCtes")
    }

    /** 2.2. Cálculo del Score **/

    // Se calcula la fórmula de la penetracion
    listadoVariables = listadoVariables
      .withColumn("Penetracion",
        when($"PenetracionInCtes" === 0, 0)
          .otherwise($"PenetracionCtes".cast("double") / $"PenetracionInCtes") *
          ($"PenetracionCtes" + $"PenetracionInCtes") / 2)

    // Se calcula el promedio y desviacion estándar
    val listadoPromDS = listadoVariables
      .agg(
        round(sum($"Frecuencia").as("double") / count($"CodEbelista"), 4)
          .as("PromFrecuencia"),
        round(stddev_pop($"Frecuencia"), 4).as("DSFrecuencia"),
        round(sum($"Penetracion").as("double") / count($"CodEbelista"), 4)
          .as("PromPenetracion"),
        round(stddev_pop($"Penetracion"), 4).as("DSPenetracion"))

    // Se calcula los datos normalizados
    listadoVariables = {
      val lv = listadoVariables.alias("lv")
      val lp = listadoPromDS.alias("lp")

      lv.crossJoin(lp)
        .updateWith(Seq($"lv.*"), Map(
          "FrecuenciaNor" ->
            when($"lp.DSFrecuencia" === 0, 0).otherwise(
              round(($"lv.Frecuencia" - $"lp.PromFrecuencia") / $"lp.DSFrecuencia", 4)
            ),
          "PenetracionNor" ->
            when($"lp.DSPenetracion" === 0, 0).otherwise(
              round(($"lv.Penetracion" - $"lp.PromPenetracion") / $"lp.DSPenetracion", 4)
            )
        ))
    }

    // Se calcula el Score
    listadoVariables = listadoVariables
      .withColumn("Score", $"PenetracionNor" - $"FrecuenciaNor")

    // Se calcula el promedio y desviación estándar del Score
    val listadoPromDSScore = listadoVariables
      .agg(
        round(sum($"Score").as("double") / count($"CodEbelista"), 4)
          .as("PromScore"),
        round(stddev_pop($"Score"), 4).as("DSScore"))

    // Se calcula los datos normalizados
    listadoVariables = {
      val lv = listadoVariables.alias("lv")
      val lp = listadoPromDSScore.alias("lp")

      lv.crossJoin(lp)
        .updateWith(Seq($"lv.*"), Map(
          "ScoreNor" ->
            when($"lp.DSScore" === 0, 0).otherwise(
              round(($"lv.Score" - $"lp.PromScore") / $"lp.DSScore", 4)
            )
        ))
    }

    // Se calcula el promedio y desviación estándar del Precio de Oferta
    val listadoPromDSPrecio = listadoVariables
      .agg(
        round(sum($"PrecioOferta").as("double") / count($"CodEbelista"), 4)
          .as("PromPrecioOferta"),
        round(stddev_pop($"PrecioOferta"), 4).as("DSPrecioOferta"))

    // Se normaliza la variable Precio de Oferta
    listadoVariables = {
      val lv = listadoVariables.alias("lv")
      val lp = listadoPromDSPrecio.alias("lp")

      lv.crossJoin(lp)
        .updateWith(Seq($"lv.*"), Map(
          "PrecioOfertaNor" ->
            when($"lp.DSPrecioOferta" === 0, 0).otherwise(
              round(($"lv.PrecioOferta" - $"lp.PromPrecioOferta")
                / $"lp.DSPrecioOferta", 4)
            )
        ))
    }

    // Se calcula el Score en base a la Oportunidad
    listadoVariables = {
      val expScore = exp($"ScoreNor" + $"PrecioOfertaNor")

      listadoVariables
        .withColumn("ProbabilidadCompra",
          (expScore / (expScore + lit(1))) * $"UnidadesOferta")
    }

    // Carga los registros a la tabla ListadoProbabilidades
    val listadoProbabilidades = listadoVariables
      .select(
        lit(params.codPais()).as("CodPais"),
        lit(params.anioCampanaProceso()).as("AnioCampanaProceso"),
        lit(params.anioCampanaExpo()).as("AnioCampanaExpo"),
        $"CodEbelista", $"CodTactica", $"FlagTop",
        $"ProbabilidadCompra".as("Probabilidad"),
        lit(params.tipoARP()).as("TipoARP"),
        lit(params.tipoPersonalizacion()).as("TipoPersonalizacion"),
        lit(0).as("FlagMC"),
        lit(params.perfil()).as("Perfil"))


    /** 2.4. Carga tabla de productos para el complemento **/

    // Creacion de la tabla
    var listadoVariablesProductos = productosCUC
      .select(
        $"TipoTactica",
        $"CodTactica",
        $"FlagTop",
        lit(0d).as("PenetracionCtes"),
        lit(0d).as("PenetracionInCtes"),
        $"PrecioOferta",
        lit(0d).as("Penetracion"),
        lit(0d).as("STD_PrecioOferta"),
        lit(0d).as("STD_Penetracion"),
        lit(0d).as("Score"),
        lit(0d).as("STD_Score"),
        $"Unidades".as("UnidadesOferta"),
        lit(0d).as("ProbabilidadCompra"))

    // Actualización del % Penetración Constantes
    listadoVariablesProductos = {
      val lv = listadoVariablesProductos.alias("lv")
      val pt = penetracionCtes.alias("pt")

      val updateListado = lv.join(pt, makeEquiJoin("lv", "pt", Seq("CodTactica")))
        .select($"lv.CodTactica", $"pt.Porcentaje".as("PenetracionCtes"))

      lv.updateFrom(updateListado, Seq("CodTactica"), "PenetracionCtes")
    }

    // Actualización del % Penetración Inconstantes
    listadoVariablesProductos = {
      val lv = listadoVariablesProductos.alias("lv")
      val pt = penetracionInCtes.alias("pt")

      val updateListado = lv.join(pt, makeEquiJoin("lv", "pt", Seq("CodTactica")))
        .select($"lv.CodTactica", $"pt.Porcentaje".as("PenetracionInCtes"))

      lv.updateFrom(updateListado, Seq("CodTactica"), "PenetracionInCtes")
    }

    // Se calcula la fórmula de la penetracion
    listadoVariablesProductos = listadoVariablesProductos
      .withColumn("Penetracion",
        when($"PenetracionInCtes" === 0, 0)
          .otherwise($"PenetracionCtes".cast("double") / $"PenetracionInCtes") *
          ($"PenetracionCtes" + $"PenetracionInCtes") / 2)

    // Se calcula el promedio y desviacion estándar
    val listadoPromDSProd = listadoVariablesProductos
      .agg(
        round(sum($"PrecioOferta").as("double") / count($"CodTactica"), 4)
          .as("PromPrecioOferta"),
        round(stddev_pop($"PrecioOferta"), 4).as("DSPrecioOferta"),
        round(sum($"Penetracion").as("double") / count($"CodTactica"), 4)
          .as("PromPenetracion"),
        round(stddev_pop($"Penetracion"), 4).as("DSPenetracion"))

    // Se calcula los datos normalizados
    listadoVariablesProductos = {
      val lv = listadoVariablesProductos.alias("lv")
      val lp = listadoPromDSProd.alias("lp")

      lv.crossJoin(lp)
        .updateWith(Seq($"lv.*"), Map(
          "STD_PrecioOferta" ->
            when($"lp.DSPrecioOferta" === 0, 0).otherwise(
              round(($"lv.PrecioOferta" - $"lp.PromPrecioOferta") / $"lp.DSPrecioOferta", 4)
            ),
          "STD_Penetracion" ->
            when($"lp.DSPenetracion" === 0, 0).otherwise(
              round(($"lv.Penetracion" - $"lp.PromPenetracion") / $"lp.DSPenetracion", 4)
            )
        ))
    }

    // Se calcula el Score
    listadoVariablesProductos = listadoVariablesProductos
      .withColumn("Score", $"STD_Penetracion" + $"STD_PrecioOferta")

    // Se calcula el promedio y desviación estándar del Score
    val listadoPromDSScoreProd = listadoVariablesProductos
      .agg(
        round(sum($"Score").as("double") / count($"CodTactica"), 4)
          .as("PromScore"),
        round(stddev_pop($"Score"), 4).as("DSScore"))

    // Se calcula los datos normalizados
    listadoVariablesProductos = {
      val lv = listadoVariablesProductos.alias("lv")
      val lp = listadoPromDSScoreProd.alias("lp")

      lv.crossJoin(lp)
        .updateWith(Seq($"lv.*"), Map(
          "STD_Score" ->
            when($"lp.DSScore" === 0, 0).otherwise(
              round(($"lv.Score" - $"lp.PromScore") / $"lp.DSScore", 4)
            )
        ))
    }

    // Se calcula el Score en base a la Oportunidad
    listadoVariablesProductos = {
      val expScore = exp($"STD_SCORE")

      listadoVariablesProductos
        .withColumn("ProbabilidadCompra",
          (expScore / (expScore + lit(1))) * $"UnidadesOferta")
    }

    // Carga los registros a la tabla ListadoVariablesProductos
    val listadoVariablesProductosFinal = listadoVariablesProductos
      .select(
        lit(params.codPais()).as("CodPais"),
        lit(params.anioCampanaProceso()).as("AnioCampanaProceso"),
        lit(params.anioCampanaExpo()).as("AnioCampanaExpo"),
        lit(params.tipoARP()).as("TipoARP"),
        lit(params.tipoPersonalizacion()).as("TipoPersonalizacion"),
        $"TipoTactica", $"CodTactica", $"FlagTop", $"PenetracionCtes",
        $"PenetracionInCtes", $"Penetracion", $"STD_PrecioOferta",
        $"STD_Penetracion", $"Score", $"STD_Score", $"UnidadesOferta",
        $"ProbabilidadCompra")

    (None, None, Some(listadoProbabilidades), Some(listadoVariablesProductosFinal))
  }


  private type Commons = (DataFrame, DataFrame, DataFrame, DataFrame)
  private def commonTables(spark: SparkSession): Commons = {
    // Leo la Base de Consultoras
    val baseConsultorasFiltrada = baseConsultoras
      .where($"CodPais" === params.codPais() &&
        $"TipoARP" === params.tipoARP() &&
        $"Perfil" === params.perfil())
      .select("*")

    val llavesConsultoras = new LlavesConsultoras(baseConsultorasFiltrada).get(spark)

    // Se obtienen los productos a nivel de Item (CODSAP?)
    val productos = new ProductosItem(params, listadoProductos, dwhDProducto).get(spark)

    // Se obtienen los productos a nivel CUC
    val productosCUC = {
      val pc = new ProductosCUC(params, listadoProductos, dwhDProducto)
        .get(spark).alias("pc")
      val dm = dwhDMatrizCampana.alias("dm")
      val dp = dwhDProducto.alias("dp")

      // Se Actualiza el código SAP desde la Matriz de Facturación
      val codSap = dm.join(pc, makeEquiJoin("dm", "pc", Seq("CodVenta")))
        .join(dp, makeEquiJoin("dp", "dm", Seq("CodSAP")))
        .where(dm("AnioCampana") === params.anioCampanaExpo())
        .select($"pc.CodProducto", $"dp.CodSAP")

      pc.updateFrom(codSap, Seq("CodProducto"), "CodSAP")
    }

    (
      baseConsultorasFiltrada.persist(StorageLevel.MEMORY_AND_DISK_SER),
      llavesConsultoras.persist(StorageLevel.MEMORY_AND_DISK_SER),
      productos.persist(StorageLevel.MEMORY_AND_DISK_SER),
      productosCUC.persist(StorageLevel.MEMORY_AND_DISK_SER)
    )
  }
}
