import java.io.{BufferedWriter, File, FileInputStream, FileWriter}
import java.nio.file.{Files, Paths}

import com.amazonaws.regions.Regions
import com.amazonaws.services.s3.AmazonS3ClientBuilder
import org.apache.hadoop.io.{LongWritable, Text}
import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}

import scala.collection.mutable.ArrayBuffer
import scala.io.Source
import scala.math.BigDecimal.RoundingMode
import scala.xml.XML.loadString

/**
  * Created by rparra on 25/2/17.
  */
object IrsRevenueApp {
  @transient lazy val log = org.apache.log4j.LogManager.getLogger("myLogger")

  def main(args: Array[String]) {
    val conf = new SparkConf().setAppName("IRS Revenue App")
      .registerKryoClasses(Array(classOf[LongWritable], classOf[Text]))
    val sc = new SparkContext(conf)
    val recordsOfInterest: RDD[(String, String, Long, Long)] = readOrFetchData(sc)


    def averageBy(rdd: RDD[(String, String, Long, Long)], keyGetter: ((String, String, Long, Long)) => String,
                  metric: ((String, String, Long, Long)) => BigDecimal) = {

      val accumulator = (BigDecimal.valueOf(0.0), 0)
      val addToAccumulator = (acc: (BigDecimal, Int), elem: BigDecimal) => ((elem + acc._1, acc._2 + 1))
      val mergeAccumulators = (a: (BigDecimal, Int), b: (BigDecimal, Int)) => ((a._1 + b._1, a._2 + b._2))

      val agg = rdd.map(r => (keyGetter(r), metric(r)))
        .aggregateByKey(accumulator)(addToAccumulator, mergeAccumulators).collect()
      agg.map(x => (x._1, x._2._1 / x._2._2))
    }

    def medianBy(rdd: RDD[(String, String, Long, Long)], keyGetter: ((String, String, Long, Long)) => String,
                 metric: ((String, String, Long, Long))  => BigDecimal) = {
      val accumulator = ArrayBuffer[BigDecimal]()
      val addToAccumulator = (acc: ArrayBuffer[BigDecimal], elem: BigDecimal) => {acc.append(elem); acc}
      val mergeAccumulators = (a: ArrayBuffer[BigDecimal], b: ArrayBuffer[BigDecimal]) => a ++ b

      val agg = rdd.map(r => (keyGetter(r), metric(r)))
        .aggregateByKey(accumulator)(addToAccumulator, mergeAccumulators).collect()
      agg.map(x => {
        val sorted = x._2.sorted
        val median = if (sorted.length % 2 == 0) {
          (sorted(sorted.length / 2) + sorted(sorted.length / 2 - 1)) / 2
        } else sorted(sorted.length / 2)
        (x._1, median)
      })
    }


    val buffer = ArrayBuffer[String]()

    recordsOfInterest.map(r =>  (r._3, r._4, r._3 - r._4, (r._3 - r._4) / BigDecimal.valueOf(Math.abs(r._4)) * 100))
      .repartition(1)
      .saveAsTextFile("absolute-relative")

    val nationalAverageGrowth = averageBy(recordsOfInterest, r => r._1, r => r._3 - r._4)
    nationalAverageGrowth.foreach(r => {
      val msg = s"Average national growth in USD: ${r._2.setScale(2, RoundingMode.HALF_UP)}"
      buffer.append(msg)
      println(msg)
    })

    val nationalAverageRelativeGrowth = averageBy(recordsOfInterest, r => r._1, r => (r._3 - r._4) / BigDecimal.valueOf(Math.abs(r._4)) * 100)
    nationalAverageRelativeGrowth.foreach(r => {
      val msg = s"Average national growth in %: ${r._2.setScale(2, RoundingMode.HALF_UP)}"
      buffer.append(msg)
      println(msg)
    })

    val stateAverageGrowth = averageBy(recordsOfInterest, r => r._2, r => r._3 - r._4)
    stateAverageGrowth.foreach(r => {
      val msg = s"Average growth in ${r._1} in USD: ${r._2.setScale(2, RoundingMode.HALF_UP)}"
      buffer.append(msg)
      println(msg)
    })

    val stateAverageRelativeGrowth = averageBy(recordsOfInterest, r => r._2, r => (r._3 - r._4) / BigDecimal.valueOf(Math.abs(r._4)) * 100)
    stateAverageRelativeGrowth.foreach(r => {
      val msg = s"Average growth in ${r._1} in %: ${r._2.setScale(2, RoundingMode.HALF_UP)}"
      buffer.append(msg)
      println(msg)
    })


    val nationalMedianGrowth = medianBy(recordsOfInterest, r => r._1, r => r._3 - r._4)
    nationalMedianGrowth.foreach(r => {
      val msg = s"Median national growth in USD: ${r._2.setScale(2, RoundingMode.HALF_UP)}"
      buffer.append(msg)
      println(msg)
    })

//  Uncomment to test median estimation via sampling
//    for (i <- 1 to 100) {
//      val recordsOfInterest: RDD[(String, String, Long, Long)] = readOrFetchData(sc)
//
//      val nationalMedianRelativeGrowth = medianBy(recordsOfInterest, r => r._1, r => (r._3 - r._4) / BigDecimal.valueOf(Math.abs(r._4)) * 100)
//      nationalMedianRelativeGrowth.foreach(r => {
//        val msg = s"Median national growth in %: ${r._2.setScale(2, RoundingMode.HALF_UP)}"
//        buffer.append(msg)
//        println(msg)
//      })
//    }


    val stateMedianGrowth = medianBy(recordsOfInterest, r => r._2, r => r._3 - r._4)
    stateMedianGrowth.foreach(r => {
      val msg = s"Median growth in ${r._1} in USD: ${r._2.setScale(2, RoundingMode.HALF_UP)}"
      buffer.append(msg)
      println(msg)
    })

    val stateMedianRelativeGrowth = medianBy(recordsOfInterest, r => r._2, r => (r._3 - r._4) / BigDecimal.valueOf(Math.abs(r._4)) * 100)
    stateMedianRelativeGrowth.foreach(r => {
      val msg = s"Median growth in ${r._1} in %: ${r._2.setScale(2, RoundingMode.HALF_UP)}"
      buffer.append(msg)
      println(msg)
    })

    val stateCount = recordsOfInterest.map(_._2).countByValue()
    stateCount.foreach(r => {
      val msg = s"Records from ${r._1}: ${r._2}"
      buffer.append(msg)
      println(msg)
    })

    val file = new File("output.txt")
    val bw = new BufferedWriter(new FileWriter(file))
    bw.write(buffer.mkString("\n"))
    bw.close()

    //perReturnType.foreach(println(_))

    //    val errorCount = records.filter(x => x._4 == -2).count()
    //    println(s"Error count was $errorCount")
    //    records.filter(x => x._4 == -2).foreach({
    //      println("===================================================")
    //      println(_)
    //    })

    //records.saveAsTextFile("output")
    //    val logData = sc.textFile(logFile, 2).cache()
    //    val numAs = logData.filter(line => line.contains("a")).count()
    //    val numBs = logData.filter(line => line.contains("b")).count()
    //    println(s"Lines with a: $numAs, Lines with b: $numBs")
    sc.stop()
  }

  def readOrFetchData(sc: SparkContext): RDD[(String, String, Long, Long)] = {
    val intermediatePath = "intermediate"
    val currentPath = System.getProperty("user.dir")
    val localPrefix = s"$currentPath/data/"

    def fetchData(isLocal: Boolean = false): RDD[(String, String, Long, Long)] = {
      def s3 = AmazonS3ClientBuilder.standard().withRegion(Regions.US_EAST_1).build()
      val bucket = "irs-form-990"
      val s3Prefix = s"s3n://$bucket/"
      val indexFile = "index_2013.csv"

      val r = scala.util.Random
      val indexPath = if (isLocal) localPrefix + indexFile else s3Prefix + indexFile
      val index = sc.textFile(indexPath).cache()
      val fileList = index.map(_.split(",").last + "_public.xml").sample(false, 0.1, r.nextLong).collect().tail

      if (isLocal) {
        println("Fetching from local drive")
      } else {
        println("Fetching from S3")
      }

      val forms = sc.parallelize(fileList)
        .map(key => {
          val inputStream = if (isLocal) {
            new FileInputStream(new File(localPrefix + key))
          } else {
            s3.getObject(bucket, key).getObjectContent
          }
          (key, Source.fromInputStream(inputStream).mkString)
        })

      val records = forms.map(f => {
        log.info(s"Processing file ${f._1}")
        val startIndex = f._2.indexOf('<')
        val xmlNode = loadString(f._2.substring(startIndex))
        val returnType = (xmlNode \\ "Return" \ "ReturnHeader" \ "ReturnType").head.text
        val state = {
          val child = (xmlNode \\ "Return" \ "ReturnHeader" \ "Filer" \ "USAddress" \ "State")
          if (child.isEmpty) "Foreign"
          else child.head.text
        }
        val totalRevenue = {
          val child = (xmlNode \\ (returnType match {
            case "990" => "TotalRevenueCurrentYear"
            case "990EZ" => "TotalRevenue"
            case "990PF" => "TotalRevenueAndExpenses"
          }))
          if (child.isEmpty) Long.MinValue
          else child.head.text.toLong
        }

        val totalRevenuePreviousYear = if (returnType == "990") {
          val child = (xmlNode \\ "TotalRevenuePriorYear")
          if (child.isEmpty) Long.MinValue
          else child.head.text.toLong
        } else Long.MinValue
        (returnType, state, totalRevenue, totalRevenuePreviousYear)
      })

      val recordsOfInterest = records
        .filter(r => r._1 == "990" && r._2 != "Foreign" && r._3 != Long.MinValue && r._4 != Long.MinValue && r._4 != 0L)
        .cache()

      recordsOfInterest.saveAsObjectFile(intermediatePath)
      recordsOfInterest
    }

    if (Files.exists(Paths.get(intermediatePath))) {
      println("reading intermediate data from disk")
      sc.objectFile[(String, String, Long, Long)](intermediatePath)
    } else {
      val isDataDownloaded = Files.exists(Paths.get(localPrefix))
      fetchData(isDataDownloaded)
    }
  }
}
