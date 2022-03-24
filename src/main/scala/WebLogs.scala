import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions.{desc, hour, to_date, when}

object WebLogs extends App {

  val sparkSession = SparkSession
    .builder()
    .master("local[*]")
    .appName("curso")
    .config("spark.some.config.option", "some-value")
    .getOrCreate()

  import sparkSession.implicits._

  var weblogsDF = sparkSession.read
    .option("header", "true")
    .option("sep", " ")
    .option("inferSchema", "true")
    .csv("src/main/resources/weblogs/nasa_aug95.csv")


  val pro = weblogsDf.select(when($"requestProtocol".rlike("[a-zA_Z0-9]"), $"requestProtocol").as("requestProtocol"))
    .distinct()
    .na.drop()

  pro.show()


  val status = weblogsDf.select($"status")
    .groupBy($"status")
    .count()
    .orderBy(desc("count"))

  status.show()


  val code = weblogsDf.select($"requestMethod")
    .groupBy($"requestMethod")
    .count()
    .orderBy(desc("count"))

  code.show()



  val tranr = weblogsDf.groupBy($"requestResource")
    .agg(sum($"response_size").as("totalByteTransfer"))
    .na.drop()
    .orderBy(desc("totalByteTransfer"))
    .limit(1)

  tran.show()


  val regis = weblogsDf.groupBy($"requestResource")
    .count()
    .orderBy(desc("count"))

  regis.show()



  val day = weblogsDf.select(to_date($"datetime").as("Date"))
    .groupBy($"Date")
    .count()
    .orderBy(desc("count"))
    .limit(1)

  day.show()



  val hour = weblogsDf.select(hour($"datetime").as("Hour"))
    .groupBy($"Hour")
    .count()
    .orderBy(desc("count")).show(24)



  val host = weblogsDf.groupBy($"requesting_host")
    .count()
    .orderBy(desc("count"))

  host.show()



  val error = weblogsDf.select(to_date($"datetime").as("date"), $"status")
    .filter($"status".equalTo(404))
    .groupBy($"date")
    .agg(count($"date").as("404 Errors"))
    .orderBy(desc("404 Errors"))

  error.show()

}
