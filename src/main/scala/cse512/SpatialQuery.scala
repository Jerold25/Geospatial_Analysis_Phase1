package cse512

import org.apache.spark.sql.SparkSession
import scala.math.{pow, sqrt}

object SpatialQuery extends App{
  def stContains(queryRectangle: String, pointString: String): Boolean = {

    val rectSplit = queryRectangle.split(",")
    val rectLeftX = rectSplit(0).toDouble
    val rectBotY = rectSplit(1).toDouble
    val rectRightX = rectSplit(2).toDouble
    val rectTopY = rectSplit(3).toDouble

    val pointSplit = pointString.split(",")
    val pointX = pointSplit(0).toDouble
    val pointY = pointSplit(1).toDouble

    return pointX >= rectLeftX && pointX <= rectRightX && pointY >= rectBotY && pointY <= rectTopY
  }
  
  def stWithin(pointString1: String, pointString2: String, distance: Double): Boolean = {

    val pointString1Split = pointString1.split(",")
    val point1X = pointString1Split(0).toDouble
    val point1Y = pointString1Split(1).toDouble

    val pointString2Split = pointString2.split(",")
    val point2X = pointString2Split(0).toDouble
    val point2Y = pointString2Split(1).toDouble

    val point12Dist = sqrt(pow(point1X - point2X, 2) + pow(point1Y - point2Y, 2))
    return point12Dist <= distance
  }

  def runRangeQuery(spark: SparkSession, arg1: String, arg2: String): Long = {

    val pointDf = spark.read.format("com.databricks.spark.csv").option("delimiter","\t").option("header","false").load(arg1);
    pointDf.createOrReplaceTempView("point")

    // YOU NEED TO FILL IN THIS USER DEFINED FUNCTION
    spark.udf.register("ST_Contains",stContains _)

    val resultDf = spark.sql("select * from point where ST_Contains('"+arg2+"',point._c0)")
    resultDf.show()

    return resultDf.count()
  }

  def runRangeJoinQuery(spark: SparkSession, arg1: String, arg2: String): Long = {

    val pointDf = spark.read.format("com.databricks.spark.csv").option("delimiter","\t").option("header","false").load(arg1);
    pointDf.createOrReplaceTempView("point")

    val rectangleDf = spark.read.format("com.databricks.spark.csv").option("delimiter","\t").option("header","false").load(arg2);
    rectangleDf.createOrReplaceTempView("rectangle")

    // YOU NEED TO FILL IN THIS USER DEFINED FUNCTION
    spark.udf.register("ST_Contains",stContains _)

    val resultDf = spark.sql("select * from rectangle,point where ST_Contains(rectangle._c0,point._c0)")
    resultDf.show()

    return resultDf.count()
  }

  def runDistanceQuery(spark: SparkSession, arg1: String, arg2: String, arg3: String): Long = {

    val pointDf = spark.read.format("com.databricks.spark.csv").option("delimiter","\t").option("header","false").load(arg1);
    pointDf.createOrReplaceTempView("point")

    // YOU NEED TO FILL IN THIS USER DEFINED FUNCTION
    spark.udf.register("ST_Within",stWithin _)

    val resultDf = spark.sql("select * from point where ST_Within(point._c0,'"+arg2+"',"+arg3+")")
    resultDf.show()

    return resultDf.count()
  }

  def runDistanceJoinQuery(spark: SparkSession, arg1: String, arg2: String, arg3: String): Long = {

    val pointDf = spark.read.format("com.databricks.spark.csv").option("delimiter","\t").option("header","false").load(arg1);
    pointDf.createOrReplaceTempView("point1")

    val pointDf2 = spark.read.format("com.databricks.spark.csv").option("delimiter","\t").option("header","false").load(arg2);
    pointDf2.createOrReplaceTempView("point2")

    // YOU NEED TO FILL IN THIS USER DEFINED FUNCTION
    spark.udf.register("ST_Within",stWithin _)

    val resultDf = spark.sql("select * from point1 p1, point2 p2 where ST_Within(p1._c0, p2._c0, "+arg3+")")
    resultDf.show()

    return resultDf.count()
  }
}
