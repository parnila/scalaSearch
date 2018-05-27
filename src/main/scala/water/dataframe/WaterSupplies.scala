package water.dataframe

import org.apache.spark.sql._
import org.apache.spark.sql.types._
import scala.collection.mutable.ListBuffer
import org.apache.spark.sql.functions._

object WaterSupplies {

  // Create a SparkSession. No need to create SparkContext
  val sparkSession: SparkSession = SparkSession.builder()
    .appName("WaterSupplies")
    .config("spark.master", "local")
    .getOrCreate()

  /**
   * define the schema of a DataFrame
   * @param columnNames : Column names of the DataFrame
   */
  def dfSchema(columnNames: List[String]): StructType = {
    StructType(
      Seq(
        StructField(name = "code", dataType = StringType, nullable = false),
        StructField(name = "country", dataType = StringType, nullable = false),
        StructField(name = "year", dataType = IntegerType, nullable = false),
        StructField(name = "safelyManaged", dataType = IntegerType, nullable = true),
        StructField(name = "accessOnPremises", dataType = IntegerType, nullable = true),
        StructField(name = "availableWhenNeeded", dataType = IntegerType, nullable = true),
        StructField(name = "freeFromContamination", dataType = IntegerType, nullable = true),
        StructField(name = "piped", dataType = IntegerType, nullable = true),
        StructField(name = "nonpiped", dataType = IntegerType, nullable = true)))
  }

  /**
   * Models a row of water data set
   * @param country
   * @param year
   * @param accessOnPremises
   * @param availableWhenNeeded
   * @param freeFromContamination
   * @param piped
   * @param nonpiped
   */
  case class WaterSuppliesRow(
      country: String,
      year: Int,
      accessOnPremises: Int,
      availableWhenNeeded: Int,
      freeFromContamination: Int,
      piped: Int,
      nonpiped: Int) 
  
  
  //create dataframe from the file loaded
  val dataFrame = loadDataFrame()

  /** Main function */
  def main(args: Array[String]): Unit = {

   // Request Dataframe for access on Premises in 2000 and display number countries
    val datatAccessOnPremises = accessOnPremises(2000, dataFrame)
    val numberCountries = numberCountryAccessOnPremises(datatAccessOnPremises)
    println("res number:" + numberCountries.show())

	//Get a dataset from Dataframe
    val dataset = waterSuppliesTyped(dataFrame);
    println(dataset.show())

	//Do a request with plain SQL
    val v = waterDistributionSql(dataFrame)
    println(v.show());  
  }

  /**
   * Create dataframe from the file "Water_Supplies"
   */
  def loadDataFrame(): DataFrame = {
    //load file
    val resource = this.getClass.getClassLoader.getResource("water/Water_Supplies.csv")
    //create dataframe from the file loaded
    read(resource.toString())
  }

  /**
   *  Read file from csv file and return
   *  a DataFrame according the schema defined
   *
   */
  def read(resource: String): DataFrame = {
    //read file
    val rdd = sparkSession.sparkContext.textFile(resource)
    val headerColumns = rdd.first().split(",").to[List]
    // Compute the schema based on the first line of the CSV file
    val schema = dfSchema(headerColumns)
    val data =
      rdd
        .mapPartitionsWithIndex((i, it) => if (i == 0) it.drop(1) else it) // skip the header line
        .map(_.split(",").to[List])
        .map(createRow)

    //create schema
    sparkSession.createDataFrame(data, schema)
  }

  /**
   * Create Row for each line.
   * Each Row is a mix of String, Integer and null values according the schema
   */
  def createRow(line: List[String]): Row = {
    // get an index per line (1,line1)(2,line2)...and keep order of insertion
    val mapIndex = line.zipWithIndex.map(_.swap).toIndexedSeq

    //Build a list of Any (mix String, Integer and null)
    val myList = scala.collection.mutable.ListBuffer.empty[Any]
    mapIndex.map {
      case (index: Int, word: String) => (if (index < 2) myList.append(word)
      else if (word.equals("-")) myList.append(null)
      else myList.append(word.toInt))
    }

    //Get a unique row
    Row.fromSeq(myList)
  }

  /**
   * return a DataFrame with accessOnPremises not null for the year given
   * order by ascending accessOnPremises and country
   */
  def accessOnPremises(year: Int, df: DataFrame): DataFrame = {
    df.select("accessOnPremises", "country", "year")
      .where(df("year") === year && df("accessOnPremises").isNotNull)
      .orderBy(df("accessOnPremises").asc, df("country").asc)
  }

  /**
   * Number of country per accessOnPremises
   * order by descending accessOnPremises
   */
  def numberCountryAccessOnPremises(df: DataFrame): Dataset[Row] = {
    df.groupBy(df("accessOnPremises")).count.orderBy(df("accessOnPremises").desc)
  }

  /**
   * return a Dataset[WaterSuppliesRow] from the “untyped” DataFrame
   */
  def waterSuppliesTyped(waterSuppliesDf: DataFrame): Dataset[WaterSuppliesRow] = {
    //for conversion between Dataframe and Dataset
    import sparkSession.implicits._

    waterSuppliesDf.map(x => WaterSuppliesRow(x.getAs("country"),
      x.getAs("year"), x.getAs("accessOnPremises"), x.getAs("availableWhenNeeded"),
      x.getAs("freeFromContamination"), x.getAs("piped"), x.getAs("nonpiped")))
      
  }
  
  /**
   * Return a Dataframe of a view about water distribution by using a plain SQL query instead
   */
  def waterDistributionSql(waterSupplies: DataFrame): DataFrame = {
    val viewName = s"viewWaterDistribution"
    waterSupplies.createOrReplaceTempView(viewName)
    sparkSession.sql(waterDistributionSqlQuery(viewName))
  }

  /**
   * return SQL query equivalent to the transformation implemented in `waterDistributionTyped`
   * @param viewName : Name of the SQL view to use
   */
  def waterDistributionSqlQuery(viewName: String): String =
    {
      s"SELECT country, year, piped, nonpiped FROM $viewName WHERE piped IS NOT NULL and nonpiped IS NOT NULL ORDER BY country, year, piped, nonpiped"
    }

}