import java.text.SimpleDateFormat
import java.util.{Calendar, Date, Properties}

import org.apache.spark.SparkContext
import org.apache.spark.sql.{SQLContext, SaveMode, SparkSession}
import org.apache.spark.mllib.recommendation.{ALS, MatrixFactorizationModel, Rating}
object RecommendMySQL {
    def main(args: Array[String]): Unit = {
        val warehouseLocation = "spark-warehouse"

        val spark = SparkSession
                .builder()
                .appName("SparkSQL_ALS")
                .config("spark.sql.warehouse.dir", warehouseLocation)
                .enableHiveSupport()
                .getOrCreate()
        import spark.implicits._
        import spark.sql
        // 读取所有用户
        val users = sql("select distinct(u_id) from movies.t_ratings order by u_id asc")
        val allusers = users.rdd.map(_.getInt(0)).toLocalIterator
        // 读取模型
        var  dateFormat:SimpleDateFormat = new SimpleDateFormat("yyyy-MM-dd")
        var cal:Calendar = Calendar.getInstance()
        cal.add(Calendar.DATE, -1)
        var yesterday = dateFormat.format(cal.getTime())
        val modelpath = "/export/datas/movielen/models/" + yesterday
        val model = MatrixFactorizationModel.load(spark.sparkContext, modelpath)
        // 为所有用户进行推荐
        while (allusers.hasNext) {
            val rec = model.recommendProducts(allusers.next(), 5)
        }
        // 将结果输出到MySQL
        val jdbcURL = "jdbc:mysql://rs03:3306/t_rec"
        val recResultTable = "t_rec.user_movie_recommandation"
        val mysqlusername = "root"
        val mysqlpassword = "123456"
        val prop = new Properties
        // 注：执行时通过--jars把mysql的驱动jar包所在的路径添加到classpath
        prop.put("driver", "com.mysql.jdbc.Driver")
        prop.put("user", mysqlusername)
        prop.put("password", mysqlpassword)
        def writeRecResultToMysql(uid: Array[Rating], sqlContext: SQLContext, sc: SparkContext) {
            val uidString = uid.map(x => x.user.toString() + "|" + x.product.toString() + "|" + x.rating.toString())
            import sqlContext.implicits._
            val uidDFArray = sc.parallelize(uidString)
            val uidDF = uidDFArray.map(_.split("|")).map(x => Result(x(0).trim().toInt, x(1).trim.toInt, x(2).trim().toDouble)).toDF
            uidDF.write.mode(SaveMode.Overwrite).jdbc(jdbcURL, recResultTable, prop)
        }
        spark.stop()
    }

}
