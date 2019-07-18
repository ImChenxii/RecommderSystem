import org.apache.spark.sql.{SaveMode, SparkSession}

object Top5Recommend {
    //基于dataframe来构建你的SPARK MLLIB应用。
    //spark.ml是基于DATAFRAME来构建pipeline,通过pipeline来完成机器学习的流水线
    def main(args: Array[String]) {
        val warehouseLocation = "spark-warehouse"

        val spark = SparkSession
                .builder()
                .appName("SparkSQL_ALS")
                .config("spark.sql.warehouse.dir", warehouseLocation)
                .enableHiveSupport()
                .getOrCreate()
        import spark.implicits._
        import spark.sql
        //最终的表里应该是5部默认推荐的电影的名称
        val moviesRatingCount = sql("select count(*) c ,m_id from movies.t_ratings_train group by m_id order by c desc")
        val top5 = moviesRatingCount.limit(5)
        top5.createOrReplaceTempView("top5")
        val top5DF = sql("select a.m_name from movies.t_movies a join top5 b on a.,_id=b.m_id")
        top5DF.write.mode(SaveMode.Overwrite).parquet("/export/datas/movielen/logs/top5")
        sql("drop table if exists movies.t_top5Rec")
        sql("create table if not exists movies.t_top5Rec(m_name string) stored as parquet")
        sql("load data inpath '/export/datas/movielen/logs/top5' overwrite into table movies.t_top5Rec")
    }
}
