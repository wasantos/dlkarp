package pe.com.belcorp.arp.utils

import java.time.Instant

object Goodies {
  def logIt(msg: Any): Unit = {
    val now = Instant.now
    val fullMsg = s"[INFO][$now] $msg"

    System.err.println(s"[NOTERR]$fullMsg")
    System.out.println(fullMsg)
  }
}
