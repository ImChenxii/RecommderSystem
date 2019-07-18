import java.text.SimpleDateFormat
import java.util.{Calendar, Date}

import org.apache.spark.sql.SparkSession
import org.apache.spark.mllib.recommendation.ALS
import org.apache.spark.mllib.recommendation.Rating
object SparkSQLALSTest {
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
        // 读取数据
        val ratingTrainRDD = sql("select * from movies.t_ratings").rdd.map(x => Rating(x.getInt(0),x.getInt(1),x.getInt(2)))
        // 训练模型
        var model = ALS.train(ratingTrainRDD, 10, 10, 0)
        var  dateFormat:SimpleDateFormat = new SimpleDateFormat("yyyy-MM-dd")
        var cal:Calendar = Calendar.getInstance()
        cal.add(Calendar.DATE, -1)
        var yesterday = dateFormat.format(cal.getTime())
        // 保存模型
        val savePath = "/export/datas/movielen/models/" + yesterday
        model.save(spark.sparkContext, savePath)

        spark.stop()

    }

}
