import java.text.SimpleDateFormat
import java.util.{Calendar, Date}

import org.apache.spark.sql.SparkSession
import org.apache.spark.mllib.recommendation.ALS
import org.apache.spark.mllib.recommendation.Rating

object SparkSQLALS {
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
        // val ratingRDD = sql("select * from movies.t_ratings").rdd.map(x => Rating(x.getInt(0),x.getInt(1),x.getInt(2)))
        // rank:特征数量, iterations:迭代次数,lambda:正则因子,block:数据分割,并行计算,alpha:置信参数
        // val model = ALS.train(ratingRDD, 1, 10,0.01)
        // ratingTrainRDD从训练集取出的数据,需要进行训练的数据
        val ratingTrainRDD = sql("select * from movies.t_ratings_train").rdd.map(x => Rating(x.getInt(0),x.getInt(1),x.getInt(2)))
        val ratingTrainRDD_ = ratingTrainRDD.map {case Rating(userid, movieid, rating) => ((userid, movieid), rating)}
        // ratingTestRDD从测试集取出的数据
        val ratingTestRDD = sql("select * from movies.t_ratings_test").rdd.map(x => Rating(x.getInt(0),x.getInt(1),x.getInt(2)))
        val ratingTestRDD_ = ratingTestRDD.map {case Rating(userid, movieid, rating) => ((userid, movieid), rating)}
        // 去除标签的训练集,用来检验在训练集上误差所用
        val ratingTrainRDD_unlabel = ratingTrainRDD.map {case Rating(userid, movieid, rating) => (userid, movieid)}
        ratingTrainRDD.persist()
        ratingTestRDD.persist()

        //特征向量的大小
        val rank = 10
        //正则因子
        val lambda = List(0.001, 0.005, 0.01, 0.015, 0.02, 0.1)
        //迭代次数
        val iteration = List(5, 10, 20, 30, 40)
        var bestRMSE = Double.MaxValue
        var bestIteration = 0
        var bestLambda = 0.0
        var bestModel = ALS.train(ratingTrainRDD, rank, 10, 0)
        for (l <- lambda; i <- iteration) {
            // 训练
            val model = ALS.train(ratingTrainRDD, rank, i, l)
            // 预测在训练集上的误差
            val predict = model.predict(ratingTrainRDD_unlabel).map {case Rating(userid, movieid, rating) => ((userid, movieid), rating)}
            // 根据key组合,形成((uid,mid),(real, pred)
            val predictAndFact = predict.join(ratingTrainRDD_)
            // 计算均方误差
            val MSE = predictAndFact.map {
                case ((user, product), (r1, r2)) =>
                    val err = r1 - r2
                    err * err
            }.mean()
            val RMSE = math.sqrt(MSE)
            //RMSE越小，代表模型越精确
            if (RMSE < bestRMSE) {
                bestModel = model
                bestRMSE = RMSE
                bestIteration = i
                bestLambda = l
            }
        }
        var  dateFormat:SimpleDateFormat = new SimpleDateFormat("yyyy-MM-dd")
        var cal:Calendar = Calendar.getInstance()
        cal.add(Calendar.DATE, -1)
        var yesterday = dateFormat.format(cal.getTime())
        // 保存模型
        val savePath = "/export/datas/movielen/models/" + yesterday
        bestModel.save(spark.sparkContext, savePath)
        spark.stop()
    }
}
