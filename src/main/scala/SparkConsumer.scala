import org.apache.spark.SparkConf
import org.apache.spark.sql.{Dataset, SparkSession}
import org.apache.spark.sql.functions.{unix_timestamp, lit, sqrt, sin, cos, radians, atan2, pow}
import org.apache.spark.sql.streaming.{GroupStateTimeout, GroupState}


import java.sql.Timestamp


case class InputBusData(
  order: String,
  line: String,
  latitude: Float,
  longitude: Float,
  datetime: Timestamp
)

case class BusLineState(
  datetime: Option[Timestamp] = null,
  latitude: Option[Float] = null,
  longitude: Option[Float] = null
)

case class OutputBusData(
  datetime: Timestamp,
  order: String,
  line: String,
  latitude: Float,
  longitude: Float,
  lastDatetime: Option[Timestamp] = null,
  lastLatitude: Option[Float] = null,
  lastLongitude: Option[Float] = null
)

object SparkConsumer extends App {

  // Run Spark

  val conf = new SparkConf()
    .setMaster("local[*]")
    .setAppName("SparkConsumer")

  val spark = SparkSession
    .builder()
    .config(conf)
    .getOrCreate()

  spark.sparkContext.setLogLevel("ERROR")

  import spark.implicits._


  // Process Data

  def unquote(str: String): String = str.replaceAll("^\"|\"$", "").replaceAll("/./0$", "")

  def readBusDataStream(spark: SparkSession, topic: String): Dataset[String] = {
    val df = spark.readStream
      .format("kafka")
      .option("kafka.bootstrap.servers", "10.129.34.90:9090")
      .option("subscribe", topic)
      .load()

    df.selectExpr("cast(value as string)").as[String]
  }

  def parseBusData(busData: Dataset[String]): Dataset[InputBusData] = {
    busData
      .map(row => row.split(","))
      .map(row =>
        InputBusData(
          unquote (row(0)),
          unquote (row(1)),
          unquote (row(2)) toFloat,
          unquote (row(3)) toFloat,
          Timestamp.valueOf(unquote(row(5).replace("T", " ").replace("Z", "")))
        )
      )
      .as[InputBusData]
  }

  def lagValues(key: (String, String), values: Iterator[InputBusData], state: GroupState[BusLineState]): OutputBusData = {
    val currentRow = values.toList.head
    val lastState = state.getOption.getOrElse(BusLineState())

    state.update(BusLineState(Some(currentRow.datetime), Some(currentRow.latitude), Some(currentRow.longitude)))

    OutputBusData(
      currentRow.datetime, currentRow.line, currentRow.order, currentRow.latitude, currentRow.longitude,
      lastState.datetime, lastState.latitude, lastState.longitude
    )
  }

  val busDataRawInput = readBusDataStream(spark, "raw-bus-data-9")

  val lagBusData =
    parseBusData(busDataRawInput)
      .withWatermark("datetime", "5 minute")
      .groupByKey(row => (row.order, row.line))
      .mapGroupsWithState(GroupStateTimeout.NoTimeout)(lagValues)

  val diffBusData =
    lagBusData
      .withColumn("datetimeDiff", unix_timestamp($"lastDatetime") - unix_timestamp($"datetime"))
      .withColumn("a", pow(sin(radians($"latitude" - $"lastLatitude") / 2), 2))
      .withColumn("b", cos(radians($"latitude")) * cos(radians($"lastLatitude")))
      .withColumn("c", pow(sin(radians($"longitude" - $"lastLongitude") / 2), 2))
      .withColumn("d", $"a" + ($"b" * $"c"))
      .withColumn("distance", atan2(sqrt($"d"), sqrt(lit(-1) * $"d" + lit(1))) * lit(6371 * 2 * 1000))
      .drop("a", "b", "c", "d")

  val query =
    diffBusData
      .selectExpr("cast(datetime as string) AS key", "to_json(struct(*)) AS value")
      .writeStream
      .outputMode("update")
//      .format("console")
      .format("kafka")
      .option("kafka.bootstrap.servers", "10.129.34.90:9090")
      .option("topic", "processed-bus-data-10")
      .option("checkpointLocation", "/tmp/checkpoint")
      .start()

  query.awaitTermination()
}

