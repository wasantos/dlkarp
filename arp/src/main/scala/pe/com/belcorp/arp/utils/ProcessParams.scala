package pe.com.belcorp.arp.utils

import org.rogach.scallop.ScallopConf

/**
  * codPais
  * anioCampanaProceso
  * anioCampanaExpo
  * tipoARP
  * flagCarga
  * flagMC
  * perfil
  * flagOF
  * tipoGP
  * tipoPersonalizacion
  *
  */
class ProcessParams(arguments: Seq[String]) extends ScallopConf(arguments) {
  //ARP Parameters
  val codPais = opt[String]()
  val anioCampanaProceso = opt[String]()
  val anioCampanaExpo = opt[String]()
  val tipoARP = opt[String](name = "tipo-arp")
  val flagCarga = opt[Int]()
  val flagMC = opt[String](name = "flag-mc")
  val perfil = opt[String]()
  val flagOF = opt[String](name = "flag-of")
  val tipoGP = opt[String](name = "tipo-gp")
  val tipoPersonalizacion = opt[String]()
  val carpetaParametros = opt[String]()
  val schemaDatalake = opt[String](default = Some("belcorpdb"))
  val schemaARP = opt[String](name = "schema-arp", default = Some("belcorpdb"))

  // Run argument extractions
  verify()
}
