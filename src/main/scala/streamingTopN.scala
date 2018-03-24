import com.beust.jcommander.JCommander
import org.apache.kafka.common.serialization.StringDeserializer
import org.apache.kudu.spark.kudu.KuduContext
import org.apache.log4j.Logger
import org.apache.spark.sql.{SparkSession}
import org.apache.spark.streaming.{Seconds, StreamingContext}
import org.apache.spark.streaming.kafka010.ConsumerStrategies.Subscribe
import org.apache.spark.streaming.kafka010.{CanCommitOffsets, HasOffsetRanges, KafkaUtils, OffsetRange}
import org.apache.spark.streaming.kafka010.LocationStrategies.PreferConsistent


class streamingTopN {

}

case class topn(name: String, top: Int)

object streamingTopN {

  private val log = Logger.getLogger(classOf[streamingTopN])

  def main(args: Array[String]): Unit = {

    /*    if (args.length < 7) {
          System.err.println(
            s"""
               |Usage: DirectKafkaWordCount <brokers> <topics>
               |  <brokers> is a list of one or more Kafka brokers
               |  <topics> is a list of one or more kafka topics to consume from
               |  <masterList> is a list of kudu
               |  <kuduTableName> is a name of kudu
               |  <appName>  is a name of spark processing
               |  <dataProcessingMode> the function of dataProcessing logical processing mode
               |         default,common,newcommon,stds,tcdns
               |  <groupid> is the name of kafka groupname
               |  <checkpoint_path> is a address of checkpoint  hdfs:///tmp/checkpoint_
            """.stripMargin)
          System.exit(1)
        }*/

    //1.获取输入参数与定义全局变量
    log.info("获取输入变量")
    //val Array(brokers, topic, kuduMaster, kuduTableName, appName, dataProcessingMode, groupid, checkpoint_path) = args
    val argv = new Args()
    JCommander.newBuilder().addObject(argv).build().parse(args: _*)

    //2,创建source/dest context
    log.info("初始sparkcontext和kuducontext")
    val spark = SparkSession.builder().appName(argv.appName).getOrCreate()
    val ssc = new StreamingContext(spark.sparkContext, Seconds(10))
    ssc.checkpoint("/tmp/streamingtop")
    val kc = new KuduContext(argv.kuduMaster, spark.sparkContext)

    //3,创建kafka数据流
    log.info("初始化kafka数据流")
    val kafkaParams = Map[String, Object](
      "bootstrap.servers" -> argv.brokers,
      "key.deserializer" -> classOf[StringDeserializer],
      "value.deserializer" -> classOf[StringDeserializer],
      "group.id" -> argv.groupid,
      "auto.offset.reset" -> "latest",
      "enable.auto.commit" -> (false: java.lang.Boolean)
    )
    val topics = Array(argv.topic)
    val numStreams = argv.numStreams
    val kafkaStreams = (1 to numStreams).map { i =>
      KafkaUtils
        .createDirectStream[String, String](ssc, PreferConsistent,
        Subscribe[String, String](topics, kafkaParams))
        .repartition(numStreams)
    }
    val stream = ssc.union(kafkaStreams)

    //4,开始处理数据
    log.info("开始处理数据")
    import spark.implicits._

    var offsetRanges = Array[OffsetRange]()

    stream.transform(rdd => {
      offsetRanges = rdd.asInstanceOf[HasOffsetRanges].offsetRanges
      rdd
    }).map(_.value()).map(x => (x(0), 1))
      .reduceByKeyAndWindow(_ + _, _ - _, Seconds(60), Seconds(20)).foreachRDD(rdd => {
      log.info("begin write to kudu ......")
      val df = rdd.map(x => topn(x._1.toString, x._2)).toDF()

      /**
        * write to kudu ......
        */
      kc.upsertRows(df, argv.kuduTableName)

      /**
        * write to mysql
        */
      log.info("begin write to mysql .....")
      df.foreachPartition(rdd => {
        val conn = DBConnectionPool.getConn()
        rdd.foreach(x => {
          val sql = "insert into spark values(\"" + x(0) + "\"," + x(1) + ")"
          conn.prepareStatement(sql).executeUpdate()
        })
        DBConnectionPool.releaseCon(conn)
      })
      stream.asInstanceOf[CanCommitOffsets].commitAsync(offsetRanges)
    })

    ssc.start()
    ssc.awaitTermination()

  }

}
