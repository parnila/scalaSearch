package water.rdd

import org.scalatest.{ FunSuite, BeforeAndAfterAll }
import org.junit.runner.RunWith
import org.scalatest.junit.JUnitRunner
import org.apache.spark.SparkContext._
import WaterAnalysis._

@RunWith(classOf[JUnitRunner])
class WaterAnalysisTest extends FunSuite with BeforeAndAfterAll {

  test("test can be instantiated") {
    val instantiatable = try {
      WaterAnalysis
      true
    } catch {
      case _: Throwable => false
    }
    assert(instantiatable, "Can't instantiate a WaterAnalysis object")
  }

  test("test number lines in file loaded") {
    assert(waterRdd.count == 464, "the number of line should equal to 464")
  }

  test("test years available") {
    assert(years(waterRdd) == List(2000, 2015), " years 2000 and 2015 should be available")
  }

  test("test number countries with full access water per year") {
    assert(numberCountriesFullAccessWater(2000) == 48, " 48 countries get full access to water in 2000")
    assert(numberCountriesFullAccessWater(2015) == 64, " 64 countries get full access to water in 2015")
  }

  test("test number countries with a restricted access water per year") {
    assert(numberCountriesRestrictedAccessWater(2000) == 159, " 159 countries get a restricted access water  in 2000")
    assert(numberCountriesRestrictedAccessWater(2015) == 163, " 163 countries get a restricted access water  in 2015")
  }

  test("test values access water with corresponding countries in 2000") {
    val resBasic = indexBasicAccessPerCountries(2000)
    assert(resBasic.first._1 == 17, " the smallest access of water is 17% in 2000")
    assert(resBasic.first._2.toList == List("Eritrea", "Ethiopia"), " Eritrea and Ethiopia got the smallest access of water in 2000")
    assert(resBasic.max._1 == 100, " the highest access of water is 100% in 2000")
    assert(resBasic.max._2.size == 48, "48 countries get a highest access of water in 2000")
  }

  test("test values access water with corresponding countries in 2015") {
    val resBasic = indexBasicAccessPerCountries(2015)
    assert(resBasic.first._1 == 19, " the smallest basic access is 19% in 2015")
    assert(resBasic.first._2.toList == List("Eritrea"), " Eritrea gets the smallest access of water in 2015")
    assert(resBasic.max._1 == 100, " the highest access of water is 100% in 2015")
    assert(resBasic.max._2.size == 64, "64 countries get a highest access of water in 2015")
  }

  test("test list of countries which has a better access to water between 2000 and 2015") {
    val resBasic = indexBasicAccessPerCountries(2000)
    val resBasicDelta = indexBasicAccessPerCountries(2015)
    assert(diffentielCountries(resBasic.first, resBasicDelta.first) == Set("Ethiopia"), " Ethiopia has a better access to water")
    assert(diffentielCountries(resBasic.max, resBasicDelta.max) == Set("Jordan", "Canada", "Bulgaria"), " 3 countries  have now a full water access")
  }

}

