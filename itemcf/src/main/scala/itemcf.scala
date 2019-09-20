import org.apache.spark.sql.{DataFrame, SparkSession}
import org.apache.flink.api.scala._

object itemcf {
  //定义常量和表名
  val MAX_RECOMMENDATION = 10

  def main(args: Array[String]): Unit = {

    val spark: SparkSession = SparkSession.builder().appName("ItemCF")
//            .master("local[*]")
      .enableHiveSupport()
      .getOrCreate()


    //加载数据
//    val resultDF: DataFrame = spark.sql("select * from cache.rating_19 where buyer_id !='null'")
//        val ratingWithCount: DataFrame = spark.table("cache.rating_20")
        val ratingWithCount: DataFrame = spark.table("cache.rating_20_test")



    //TODO:核心算法，计算同现相似度，得到商品的相似列表

    //将评分按照用户id两两配对，统计两个商品被同一个用户评分过的次数
    val joinedDF: DataFrame = ratingWithCount.join(ratingWithCount,"buyer_id")
      .toDF("buyer_id","sku1","count1","sku2","count2")
      .select("buyer_id","sku1","count1","sku2","count2")
      .filter(x=>x.getAs[String]("sku1")!=x.getAs[String]("sku2"))
joinedDF.show()
    //创建一张临时表，用于写sql查询
    joinedDF.createTempView("joined")
    //按照sku1,sku2做group by，统计buyer_id的数量，就是对两个商品同时购买的人数
    val cooccurrenceDF: DataFrame = spark.sql(
      """
select sku1,
sku2,
count(buyer_id) as cocount,
first(count1) as count1,
first(count2) as count2
from joined
group by sku1,sku2
      """.stripMargin)

    //提取需要的数据，包装成(sku1,(sku2,score))
    import spark.implicits._
    val simDF: DataFrame = cooccurrenceDF.rdd.map {
      row =>
        val coocSim: Double = cooccurrenceSim(row.getAs[Long]("cocount"), row.getAs[Long]("count1"), row.getAs[Long]("count2"))
        (row.getString(0), (row.getString(1), coocSim))
    }
      .groupByKey()
      .map {
        case (skuId, recs) =>
          (skuId.toString, recs.toList.sortWith(_._2>_._2).take(MAX_RECOMMENDATION).map(x=>Map(x._1 -> x._2.toDouble)))
      }
      .toDF("sku_id","recs")
    simDF.show(false)


    //保存到hive

    simDF.write.mode("overwrite").format("hive").saveAsTable("cache.simScore")




    spark.stop()
  }


  def cooccurrenceSim(coCount: Long, count1: Long, count2: Long):Double = {
    coCount / math.sqrt(count1 * count2)
  }

  //定义标准推荐对象
  case class Recommendation(skuId:String,score:Double)
  //定义商品相似度列表
  case class ProductRecs(skuId:String,recs:List[Recommendation])
}
