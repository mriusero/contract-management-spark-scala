package sda.main
import sda.args._
object MainBatch {
  def main(args: Array[String]): Unit = {
    Args.parseArguments(args)
    val reader = Args.readertype
    println(s"Le type de fichier a lire est un $reader")
    val conf=Args.readerConfigurationFile
    println(s"La configuration du fichier est $conf")
  }
}