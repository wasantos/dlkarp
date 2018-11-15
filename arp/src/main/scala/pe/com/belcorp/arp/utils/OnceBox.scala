package pe.com.belcorp.arp.utils

class OnceBox[T] {
  private var value: Option[T] = None

  def this(newValue: T = null) {
    this()
    value = Option(newValue)
  }

  def get: T = value.get
  def set(newValue: => T): this.type = {
    if(this.value.isEmpty) {
      value = Option(newValue)
    }

    this
  }
}

object OnceBox {
  def empty[T]: OnceBox[T] = new OnceBox[T]
}
