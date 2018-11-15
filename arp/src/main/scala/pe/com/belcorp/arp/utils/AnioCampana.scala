package pe.com.belcorp.arp.utils

import org.apache.spark.sql.Column
import pe.com.belcorp.arp.utils

case class AnioCampana(anio: Int, campana: Int) extends Ordered[AnioCampana] {
  import AnioCampana._

  def +(periodos: Int): AnioCampana = {
    val value = campana + periodos

    if(RANGE contains value) AnioCampana(anio, value)
    else if(value < 0) AnioCampana(anio - 1, (BASE - value) % BASE)
    else AnioCampana(anio + 1, (BASE + value) % BASE)
  }

  def -(periodos: Int): AnioCampana = this.+(-periodos)

  def delta(that: AnioCampana): Int = {
    if(that > this) return that.delta(this)

    val periodosAnio = (this.anio - that.anio) * BASE
    val periodosBase = (this.campana - that.campana).abs

    if (this.campana > that.campana) periodosAnio + periodosBase
    else periodosAnio - periodosBase
  }

  override def toString: String = f"${anio}${campana + 1}%02d"
  override def compare(that: AnioCampana): Int = {
    val diffAnio = this.anio - that.anio
    if(diffAnio != 0) return diffAnio

    this.campana - that.campana
  }
}

object AnioCampana {
  val BASE = 18
  val RANGE = 0.to(17)
  private final val __DEBUG = false

  def parse(string: String): AnioCampana = {
    try {
      val (anio, campana) = string.splitAt(4)
      AnioCampana(anio.toInt, campana.stripPrefix("0").toInt - 1)
    } catch {
      case e: Exception =>
        if(__DEBUG) e.printStackTrace()
        AnioCampana(0, 0)
    }
  }

  @inline
  def calc(ac: String, delta: Int): String = (parse(ac) + delta).toString

  @inline
  def delta(ac1: String, ac2: String): Int = parse(ac1).delta(parse(ac2))

  object Spark {
    import org.apache.spark.sql.functions.udf

    private val _udfDelta = udf { (ac1: String, ac2: String) => AnioCampana.delta(ac1, ac2) }
    private val _udfCalc = udf { (ac: String, delta: Int) => AnioCampana.calc(ac, delta) }

    def delta(ac1: Column, ac2: Column): Column = _udfDelta(ac1, ac2)
    def calc(ac: Column, delta: Column): Column = _udfCalc(ac, delta)
  }
}
