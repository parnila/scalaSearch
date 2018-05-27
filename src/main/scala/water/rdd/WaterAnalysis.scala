package water.rdd

import org.apache.spark.SparkConf
import org.apache.spark.SparkContext
import org.apache.spark.SparkContext._

import org.apache.spark.rdd.RDD

/* a raw with water data*/
case class WaterRaw(isoCode: String, country: String, year: Int, basic: Option[Int], limited: Option[Int], unimproved: Option[Int], surface: Option[Int], basicRateChange: Option[Double]) extends Serializable

object WaterAnalysis {

  ///set up the spark configuration and create context
  val conf: SparkConf = new SparkConf().setMaster("local").setAppName("WaterAnalysis")
  val sc: SparkContext = new SparkContext(conf)
  
  //init RDD
  val waterRdd: RDD[WaterRaw] = sc.textFile(WaterData.filePath)
    .map(line => WaterData.parse(line))

  def main(args: Array[String]) {
    println("hey: " + waterRdd.count)

    //Get years where data have been collected
    val yearsCollected: List[Int] = years(waterRdd)

    //Get number countries with limited access and full access to water for each year
    yearsCollected.foreach(year =>
      {
        println("In " + year + " the number countries with limited access to water is : " + numberCountriesRestrictedAccessWater(year))
        println("In " + year + " the number countries with full access to water is : " + numberCountriesFullAccessWater(year))
      })

    //Get the smallest and the highest percentage of access to water with the corresponding countries for the first year of data
    val yearStart = yearsCollected.head;
    val resWaterAccess = indexBasicAccessPerCountries(yearStart)
    displayInfoSmallestAndHighestBasicAccessWater(resWaterAccess, yearStart)

    //Get the smallest and the highest percentage of access to water with the corresponding countries for the second year of data
    val yearDelta = yearsCollected.tail.head
    val resWaterAccessDelta = indexBasicAccessPerCountries(yearDelta)
    displayInfoSmallestAndHighestBasicAccessWater(resWaterAccessDelta, yearDelta)

    //get list countries which improves their water access
    displayInfoCountriesWithImprovement(resWaterAccess, resWaterAccessDelta)
    sc.stop()
  }

  def displayInfoCountriesWithImprovement(rddWaterAccess: RDD[(Int, Iterable[String])], rddWaterAccessDelta: RDD[(Int, Iterable[String])]): Unit = {
    println("List countries with a better access to" + diffentielCountries(rddWaterAccess.first, rddWaterAccessDelta.first))
    println("List countries with have now a full water access" + diffentielCountries(rddWaterAccess.max, rddWaterAccessDelta.max))
  }

  def diffentielCountries(waterAccess: (Int, Iterable[String]), waterAccessDelta: (Int, Iterable[String])): Set[String] = {
    val setCountriesBase = waterAccess._2.toSet;
    val setCountriesDelta = waterAccessDelta._2.toSet;
    setCountriesBase.filterNot(setCountriesDelta);
  }

  /**
   * Display the smallest and the highest percentage of access to water with the corresponding countries for the given year
   */
  def displayInfoSmallestAndHighestBasicAccessWater(rddWaterAccess: RDD[(Int, Iterable[String])], year: Int): Unit = {
    println("Smallest Basic access " + year + ":" + rddWaterAccess.first._1 + "% -Countries :" + rddWaterAccess.first._2.toList)
    println("Highest Basic access" + year + ":" + rddWaterAccess.max._1 + "% -Countries :" + rddWaterAccess.max._2.toList)
  }

  /**
   * years where data have been collected
   */
  def years(waterRdd: RDD[WaterRaw]): List[Int] =
    waterRdd.map(raw => (raw.year)).distinct().collect().toList

  /**
   * Get the basic value with the list of countries for a year given
   * Return Pair Rdds with key -> basic, values -> list countries
   * Order ascending=true
   */
  def indexBasicAccessPerCountries(yearSearch: Int): RDD[(Int, Iterable[String])] = {
    val pairRdd: RDD[(Int, String)] = waterRdd.filter(raw => raw.year == yearSearch && raw.basic.isDefined)
      .map(raw => (raw.basic.get, raw.country))
    //groupy by basic access water and sort ascending order
    //make this evaluation persistent for optimization
    pairRdd.groupByKey().sortByKey(true).persist()
  }

  /**
   *  number of countries with restricted access to water
   */
  def numberCountriesRestrictedAccessWater(yearSearch: Int): Long = {
    waterRdd.filter(raw => raw.year == yearSearch && raw.basic.isDefined && raw.basic.get < 100).count()
  }

  /**
   *  number of countries with full access to water
   */
  def numberCountriesFullAccessWater(yearSearch: Int): Long = {
    waterRdd.filter(raw => raw.year == yearSearch && raw.basic.isDefined && raw.basic.get == 100).count()
  }

  /**
   * List of countries with the smallest surface of water
   */
  def countriesSmallestSurfaceWater(yearSearch: Int): List[(Int, String)] =
    waterRdd.filter(raw => raw.year == yearSearch && raw.surface.isDefined)
      .map(raw => (raw.surface.get, raw.country))
      .sortBy(_._1, true).collect().toList

}