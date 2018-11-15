package pe.com.belcorp.arp.tables

import java.time.Instant
import java.util.Date

import org.apache.spark.sql.{SaveMode, SparkSession}
import pe.com.belcorp.arp.utils.ProcessParams

import scala.collection.mutable

class ProcessHistory(val id: String, val params: ProcessParams) {
  import ProcessHistory._

  private val buffer = mutable.MutableList.empty[Entry]

  def pushOK(msg: String): Unit = push(msg, Instant.now(), status = true)
  def pushError(msg: String): Unit = push(msg, Instant.now(), status = false)

  private def push(msg: String, instant: Instant, status: Boolean): Unit = {
    buffer += Entry(
      params.codPais(), params.anioCampanaExpo(), params.anioCampanaExpo(),
      params.tipoARP(), params.flagCarga(), params.flagMC(), params.perfil(),
      params.flagOF(), params.tipoGP(), params.tipoPersonalizacion(),
      id, msg, instant.toString, status
    )
  }

  def flush(spark: SparkSession, target: String): Unit = {
    import spark.implicits._

    buffer.toDF()
      .coalesce(1)
      .write
      .mode(SaveMode.Append)
      .saveAsTable(target)

    buffer.clear()
  }
}

object ProcessHistory {
  case class Entry(
    codPais: String, anioCampanaProceso: String, anioCampanaExpo: String,
    tipoARP: String, flagCarga: Int, flagMC: String, perfil: String,
    flagOF: String, flagGP: String, tipoPersonalizacion: String,
    id: String, mensaje: String, fecha: String, status: Boolean)
}
