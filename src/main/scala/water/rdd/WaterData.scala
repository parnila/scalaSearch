
package water.rdd

import java.io.File

object WaterData {

  /** get file */
  private[water] def filePath = {
    val resource = this.getClass.getClassLoader.getResource("water/Drinking_Water.csv")
    if (resource == null) sys.error("No file has been found")
    new File(resource.toURI).getPath
  }

  /** Load waterRaw from the given file */
  private[water] def parse(line: String): WaterRaw = {

    val words = line.split(",")
    WaterRaw(
      isoCode = words(0),
      country = words(1),
      year = words(2).toInt,
      basic = evalWord(words(3)),
      limited = evalWord(words(4)),
      unimproved = evalWord(words(5)),
      surface = evalWord(words(6)),
      basicRateChange = evalWordDouble(words(7)))
  }

  private def evalWord(word: String): Option[Int] =
     if (word == "-") None else Some(word.toInt)

  private def evalWordDouble(word: String): Option[Double] =
     if (word == "-") None else Some(word.toDouble)

}