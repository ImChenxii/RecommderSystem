import java.text.SimpleDateFormat
import java.util.Calendar

import org.apache.spark.sql.{DataFrame, Dataset, SparkSession}
import org.apache.spark.streaming.{Duration, StreamingContext}
import org.apache.spark.streaming.kafka.KafkaUtils
import kafka.serializer.StringDecoder
import org.apache.spark.mllib.recommendation.{MatrixFactorizationModel, Rating}
import org.apache.spark.streaming.dstream.InputDStream

object Kafka2SparkStructuredStreaming {
    def main(args: Array[String]): Unit = {
        // 创建SparkSession
        val warehouseLocation = "spark-warehouse"
        val spark = SparkSession
                .builder()
                .appName("SparkSQL_ALS")
                .config("spark.sql.warehouse.dir", warehouseLocation)
                .enableHiveSupport()
                .getOrCreate()
        // 绑定的kafka端口
        val kafkaParams = Map("bootstrap.servers" -> "rs03:9092")
        val batchDuration = new Duration(5000)
        val ssc = new StreamingContext(spark.sparkContext, batchDuration)
        import spark.implicits._
        import spark.sql
        // 绑定的主题
        val topics = "movie".split(",").toSet
        // 辅助函数：检查此用户是否存在于训练集中
        def exist(user : Int):Boolean = {
            val userList: Array[Int] = sql("select distinct(u_id) from movies.t_ratings").rdd.map(x => x.getInt(0)).collect()
            val userlist = userList
            // 返回是否存在此用户
            userlist.contains(user)
        }

        // 为新用户推荐电影策略：
        // 推荐观看人数较多的电影
        def recommendPopularMovies() = {
            sql("select * from movies.t_top5Rec").show
        }

        // 从Kafka中作为consumer消费数据
        val kafkaDirectStream: InputDStream[(String, String)] = KafkaUtils.createDirectStream[String, String, StringDecoder, StringDecoder](ssc, kafkaParams, topics)
        // 对数据进行处理
        // 读取模型
        var  dateFormat:SimpleDateFormat = new SimpleDateFormat("yyyy-MM-dd")
        var cal:Calendar = Calendar.getInstance()
        cal.add(Calendar.DATE, -1)
        var yesterday = dateFormat.format(cal.getTime())
        val modelpath = "/export/datas/movielen/models/" + yesterday
        val messages = kafkaDirectStream.foreachRDD { rdd =>
            rdd.foreachPartition { partition =>
                val userlist = partition.map(x => x._2.split(",")).map(x => x(1)).map(_.toInt)
                val model = MatrixFactorizationModel.load(spark.sparkContext, modelpath)
            while (userlist.hasNext) {
                val user = userlist.next()
                if (exist(user)) {
                    // 如果存在此用户,就为其推荐15个中随机5个
                    val result_15: Array[Rating] = model.recommendProducts(user, 15)
                    // 生成一个0-2随机数
                    val i = (new util.Random).nextInt(3);
                    val result: Array[Rating] = result_15.slice(5 * i, 5 * (i+1))
                } else {
                    println("below movies are recommended for you :")
                    recommendPopularMovies()
                }
            }
        }
    }

}
