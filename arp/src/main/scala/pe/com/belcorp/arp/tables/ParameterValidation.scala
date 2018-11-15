package pe.com.belcorp.arp.tables

import org.apache.spark.sql._
import org.apache.spark.sql.functions._
import pe.com.belcorp.arp.utils.ProcessParams
import pe.com.belcorp.arp.utils.Extensions._

import scala.collection.mutable

/**
  * Generate DataFrame with validation messages
  * @param processParams current process parameters
  * @param arpParams ARP parameter table for personalization
  * @param arpParamsEst ARP parameter table for estimation
  */
class ParameterValidation(
   val processParams: ProcessParams,
   val dproducto: DataFrame,
   val arpParamsPer: Option[DataFrame] = None,
   val arpParamsEst: Option[DataFrame] = None) extends Table {

  override def get(spark: SparkSession): DataFrame = {
    import spark.implicits._

    val messages = processParams.flagCarga() match {
      case Constants.CARGA_PERSONALIZACION => validatePersonalizationParameters(spark)
      case Constants.CARGA_ESTIMACION => validateEstimateParameters(spark)
    }

    messages.toDF()
  }

  import ParameterValidation._

  private val arpParams = processParams.flagCarga() match {
    case Constants.CARGA_PERSONALIZACION => arpParamsPer.get
    case Constants.CARGA_ESTIMACION => arpParamsEst.get
  }

  private val filteredArpParams = {
    arpParams.where(
      $"CodPais" === processParams.codPais() &&
        $"AnioCampanaProceso" === processParams.anioCampanaProceso() &&
        $"AnioCampanaExpo" === processParams.anioCampanaExpo() &&
        $"TipoArp" === processParams.tipoARP() &&
        $"TipoPersonalizacion" === processParams.tipoPersonalizacion() &&
        $"Perfil" === processParams.perfil())
  }

  private def emptyParameters(): Boolean = {
    filteredArpParams
      .select($"CodCUC")
      .distinct()
      .exists
  }

  private def getMissingProducts(spark: SparkSession): Array[String] = {
    import spark.implicits._

    val neededProducts = filteredArpParams
      .select($"CodCUC".as("CUC"))
      .distinct()

    val missingProducts = neededProducts
      .join(dproducto, Seq("CUC"), "left_anti")
      .select($"CUC".as[String])

    missingProducts.collect()
  }

  private def validatePersonalizationParameters(spark: SparkSession): Seq[Occurence] = {
    import spark.implicits._

    val result = mutable.MutableList.empty[Occurence]

    /** 1. Validación para Carga **/
    /** 1.1. Valida la existencia de información en ARP_Parametros **/
    if (emptyParameters()) {
      result += Occurence("No hay parametros")
    }

    /** 1.2. Valida que todos los CUCs existan en la tabla DPRODUCTO **/
    val missing = getMissingProducts(spark)
    if(missing.nonEmpty) {
      result += Occurence("Los Cuc No Existen en DWH_DPRODUCTO :: " + missing.mkString(","))
    }

    /** 1.3. Valida que las Tacticas tengan un solo Indicador Padre en ARP_Parametros **/
    val multipleIndicators = filteredArpParams
      .groupBy($"CodTactica")
      .agg(sum($"IndicadorPadre").as("NumIndPadre"))
      .select("*")
      .where($"NumIndPadre" > 1)
      .exists

    if (multipleIndicators) {
      result += Occurence("Error Ind.Padre")
    }

    /** 1.4. Valida que los CUVs no se repitan en ARP_Parametros **/
    val multipleCUV = filteredArpParams
      .where($"IndicadorPadre" > 0)
      .groupBy($"CODVENTA")
      .agg(count("*").as("Repeticiones"))
      .select("*")
      .where($"Repeticiones" > 1)
      .exists

    if (multipleCUV) {
      result += Occurence("CUV repetido")
    }

    /** 1.5. Valida Vinculos sin Indicador Padre en ARP_Parametros LMHM **/
    val noLink = arpParams
      .where(
        $"CodPais" === processParams.codPais() &&
          $"AnioCampanaProceso" === processParams.anioCampanaProceso() &&
          $"AnioCampanaExpo" === processParams.anioCampanaExpo() &&
          $"TipoArp" === processParams.tipoARP() &&
          $"TipoPersonalizacion" === processParams.tipoPersonalizacion())
      .groupBy($"Perfil", $"CodTactica")
      .agg(
        sum(when($"IndicadorPadre" === 1, 1).otherwise(0))
          .as("Valor"))
      .select("*")
      .where($"Valor" === 0)
      .exists

    if (noLink) {
      result += Occurence("Vinculos sin Indicador Padre")
    }

    result
  }


  private def validateEstimateParameters(spark: SparkSession): Seq[Occurence] = {
    val result = mutable.MutableList.empty[Occurence]

    /** 2. Validación para Estimación **/
    /** 2.1. Valida la existencia de información en ARP_Parametros_Est **/
    if (emptyParameters()) {
      result += Occurence("No hay parametros")
    }

    /** 2.2. Valida que todos los CUCs existan en la tabla DPRODUCTO **/
    val missing = getMissingProducts(spark)
    if(missing.nonEmpty) {
      result += Occurence("Los Cuc No Existen en DWH_DPRODUCTO :: " + missing.mkString(","))
    }

    result
  }
}

object ParameterValidation {
  case class Occurence(message: String)
}