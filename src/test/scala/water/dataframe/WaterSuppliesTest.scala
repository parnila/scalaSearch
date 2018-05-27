package water.dataframe

import org.scalatest.{ FunSuite, BeforeAndAfterAll }
import org.junit.runner.RunWith
import org.scalatest.junit.JUnitRunner
import org.apache.spark.SparkContext._
import WaterSupplies._

@RunWith(classOf[JUnitRunner])
class WaterSuppliesTest extends FunSuite with BeforeAndAfterAll {

  test("test can be instantiated") {
    val instantiatable = try {
      WaterSupplies
      true
    } catch {
      case _: Throwable => false
    }
    assert(instantiatable, "Can't instantiate a WaterSupplies object")
  }

  test("test number lines in file loaded") {
    assert(dataFrame.count == 464, "the number of line should equal to 464")
  }

  test("test value of accessOnPremises for the first row") {
    val firstRow = accessOnPremises(2000, dataFrame).head()
    assert(firstRow.getAs("accessOnPremises").asInstanceOf[Int] == 2, "In 2000 , the smallest access on premises is 2%")
    assert(firstRow.getAs("country").equals("United Republic of Tanzania"), "In 2000 , United Republic of Tanzania has the smallest access on premises")
  }
  
  test("test count of countries with 100% of accessOnPremises in 2000") {
    val numberCounty = numberCountryAccessOnPremises(accessOnPremises(2000, dataFrame))
    assert(numberCounty.head.getAs("count").asInstanceOf[Long] == 24, "In 2000 , 24 countries have an access on premises of 100%")  
  }
}