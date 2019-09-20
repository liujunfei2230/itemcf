package model

import org.apache.spark.broadcast.Broadcast
import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}
import org.apache.spark.sql.{DataFrame}
import org.apache.spark.sql.hive.HiveContext

/**
  * Created by dengxing on 2017/7/18.
  */
object CF {

  /** 基于dt时间获取原始数据源
    *
    * @param sc    SparkContext
    * @param table 转换的hive表
    * @param day   获取当前日期的数据
    * @return 原始数据的dataFrame
    */
  def getResource(sc: SparkContext, table: String, day: String) = {
    val hiveContext = new HiveContext(sc)
    import hiveContext.sql
    val resource = sql("select "
      + "uid,"
      + "aid,"
      + "cnt"
      + " from " + table + " where dt ='" + day + "'")
    resource
  }

  /**
    * 分布式计算余弦相似度
    * --------------------------------
    *          user1     user2
    * item1  score11   score21 (X)
    * item2  score12   score22 (Y)
    * --------------------------------
    * sim(item1,item2) = XY / math.sqrt(XX) * math.sqrt(YY)
    * XY= score11 * score12 + score21 * score22
    * XX = score11 * score11 + score21 * score21
    * YY = score12 * score12 + score22 * score22
    *
    * @param resource
    * @return RDD[(item1,item2,sim)]
    */
  def getCosineSimilarity(resource: DataFrame): RDD[(String, (String, Double))] = {
    val rating = resource.map {
      row => {
        val uid = row.getString(0)
        val aid = row.getString(1)
        val score = row.getString(2).toDouble
        (uid, aid, score)
      }
    }
    //RDD[(uid,(aid,score))]
    val user_item_score = rating.map(f => (f._1, (f._2, f._3)))
    /*
     * 提取每个用户有过行为的item键值对,即
     * RDD[((aid1,aid2),(score11,score22))]
     */
    val item_score_pair = user_item_score.join(user_item_score)
      .map(f => ((f._2._1._1, f._2._2._1), (f._2._1._2, f._2._2._2)))
    /*
     * 提取同一对item，所有的用户评分向量的点积，即XY 及 XX 及 YY
     * RDD[((aid1,aid2),score11 * score12 + score21 * score22)]
     * 及 RDD[((aid1,aid1),score11 * score11 + score21 * score21)]
     * 及 RDD[((aid2,aid2),score12 * score12 + score22 * score22)]
     */
    val item_pair_ALL = item_score_pair.map(f => (f._1, f._2._1 * f._2._2)).reduceByKey(_ + _)
    /*
     * 提取每个item，所有用户的自向量的点积，即XX或YY
     * RDD[((aid1,aid1),score11 * score11 + score21 * score21)]
     * 或 RDD[((aid2,aid2),score12 * score12 + score22 * score22)]
     */
    val item_pair_XX_YY = item_pair_ALL.filter(f => f._1._1 == f._1._2)
    /*
     * 提取每个item，所有用户的非自向量的点积，即XY
     * RDD[((aid1,aid2),score11 * score12 + score21 * score22)]
     */
    val item_pair_XY = item_pair_ALL.filter(f => f._1._1 != f._1._2)
    /*
     * 提取item_pair_XX_YY中的item及XX或YY
     * RDD[(aid1,score11 * score11 + score21 * score21)]
     * 或 RDD[(aid2,score12 * score12 + score22 * score22)]
     */
    val item_XX_YY = item_pair_XX_YY.map(f => (f._1._1, f._2))
    /*
     *  转化item_pair_XY为(aid1,((aid1,aid2,XY),XX)))
     *  RDD[(aid1,((aid1,aid2,score11 * score12 + score21 * score22),score11 * score11 + score21 * score21)))]
     */
    val item_XY_XX = item_pair_XY.map(f => (f._1._1, (f._1._1, f._1._2, f._2))).join(item_XX_YY)
    /*
     *  转为item_XY_XX为(aid2,((aid1,aid2,XY,XX),YY))
     *  RDD[(aid2,((aid1,aid2,score11 * score12 + score21 * score22,score11 * score11 + score21 * score21),score12 * score12 + score22 * score22))]
     */
    val item_XY_XX_YY = item_XY_XX.map(f => (f._2._1._2, (f._2._1._1, f._2._1._2, f._2._1._3, f._2._2))).join(item_XX_YY)
    /*
     *  提取item_XY_XX_YY中的(aid1,aid2,XY,XX,YY))
     *  RDD[(aid1,aid2,score11 * score12 + score21 * score22,score11 * score11 + score21 * score21,score12 * score12 + score22 * score22)]
     */
    val item_pair_XY_XX_YY = item_XY_XX_YY.map(f => (f._2._1._1, f._2._1._2, f._2._1._3, f._2._1._4, f._2._2))
    /*
     *  转化item_pair_XY_XX_YY为(aid1,aid2,XY / math.sqrt(XX * YY))
     *  RDD[(aid1,aid2,score11 * score12 + score21 * score22 / math.sqrt((score11 * score11 + score21 * score21)*(score12 * score12 + score22 * score22))]
     */
    val item_pair_sim = item_pair_XY_XX_YY.map(f => (f._1, (f._2, f._3 / math.sqrt(f._4 * f._5))))
    item_pair_sim
  }

  /**
    * 基于item相似度矩阵为user生成topN推荐列表
    *
    * @param resource
    * @param item_sim_bd
    * @param topN
    * @return RDD[(user,List[(item,score)])]
    */
  def recommend(resource: DataFrame, item_sim_bd: Broadcast[scala.collection.Map[String, List[(String, Double)]]], topN: Int = 50) = {
    val user_item_score = resource.map(
      row => {
        val uid = row.getString(0)
        val aid = row.getString(1)
        val score = row.getString(2).toDouble
        ((uid, aid), score)
      }
    )
    /*
     * 提取item_sim_user_score为((user,item2),sim * score)
     * RDD[(user,item2),sim * score]
     */
    val user_item_simscore = user_item_score.flatMap(
      f => {
        val items_sim = item_sim_bd.value.getOrElse(f._1._2, List(("0", 0.0)))
        for (w <- items_sim) yield ((f._1._1, w._1), w._2 * f._2)
      }).filter(_._2 > 0.03)

    /*
     * 聚合user_item_simscore为 (user,（item2,sim1 * score1 + sim2 * score2）)
     * 假设user观看过两个item,评分分别为score1和score2，item2是与user观看过的两个item相似的item,相似度分别为sim1，sim2
     * RDD[(user,item2),sim1 * score1 + sim2 * score2）)]
     */
    val user_item_rank = user_item_simscore.reduceByKey(_ + _, 1000)
    /*
     * 过滤用户已看过的item,并对user_item_rank基于user聚合
     * RDD[(user,CompactBuffer((item2,rank2）,(item3,rank3)...))]
     */
    val user_items_ranks = user_item_rank.subtractByKey(user_item_score).map(f => (f._1._1, (f._1._2, f._2))).groupByKey()
    /*
     * 对user_items_ranks基于rank降序排序，并提取topN,其中包括用户已观看过的item
     * RDD[(user,ArrayBuffer((item2,rank2）,...,(itemN,rankN)))]
     */
    val user_items_ranks_desc = user_items_ranks.map(f => {
      val item_rank_list = f._2.toList
      val item_rank_desc = item_rank_list.sortWith((x, y) => x._2 > y._2)
      (f._1, item_rank_desc.take(topN))
    })
    user_items_ranks_desc
  }

  /**
    * json 编码
    *
    * @param recTopN 离线推荐结果
    */
  def encodeToJson(recTopN: (String, List[(String, Double)])) = {
    val mtype = "u2a"
    val mtype_ = "\"" + "mtype" + "\"" + ":" + "\"" + mtype + "\""
    val uid = recTopN._1
    val uid_ = "\"" + "uid" + "\"" + ":" + "\"" + uid + "\""
    val aid_score = recTopN._2
    val aids_ = new StringBuilder().append("\"" + "aids" + "\"" + ":[")
    for (v <- aid_score) {
      val aid = v._1
      val score = v._2
      val aid_score = "[" + "\"" + aid + "\"" + "," + score + "]"
      aids_.append(aid_score + ",")
    }
    aids_.deleteCharAt(aids_.length - 1).append("]")
    val result = "{" + mtype_ + "," + uid_ + "," + aids_.toString() + "}"
    result
  }

  def main(args: Array[String]): Unit = {
    val table = args(0) //要处理的表
    val day = args(1) //当前日期
    val output = args(2) //cf相似矩阵输出路径

    val sparkConf = new SparkConf().setAppName("Wireless ItemBased Collaborative Filtering")
    val sc = new SparkContext(sparkConf)

    val resource = getResource(sc, table, day).repartition(500)
    resource.cache()

    // 1.计算item相似度矩阵
    val item_sim: RDD[(String, (String, Double))] = getCosineSimilarity(resource)
    item_sim.cache()

    // 2.保存cf相似度矩阵到HDFS
    item_sim.saveAsTextFile(output)

    // 3.每个item提取最相近的40个item
    val item_sim_rdd = item_sim.filter(f => f._2._2 > 0.05).groupByKey().map(
      f => {
        val item = f._1
        val items_score = f._2.toList
        val items_score_desc = items_score.sortWith((x, y) => x._2 > y._2)
        (item, items_score_desc.take(40))
      }).collectAsMap()

    // 4.广播相似度矩阵
    val item_sim_bd: Broadcast[scala.collection.Map[String, List[(String, Double)]]] = sc.broadcast(item_sim_rdd)

    // 5.为用户生成推荐列表
    val recTopN = recommend(resource, item_sim_bd, 50)

    recTopN.map(encodeToJson(_)).take(10).foreach(println)


  }
}


