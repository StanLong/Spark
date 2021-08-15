package com.stanlong.spark.core.req

import org.apache.spark.rdd.RDD
import org.apache.spark.util.AccumulatorV2
import org.apache.spark.{SparkConf, SparkContext}

import scala.collection.mutable

/**
 * Top10热门品类
 */
object Spark02_Req_HotCategorySessionTop10 {

    def main(args: Array[String]): Unit = {
        val sparkConf = new SparkConf().setMaster("local[*]").setAppName("HotCategoryTop10")
        val sc = new SparkContext(sparkConf)

        val actionRdd = sc.textFile("datas/user_visit_action.txt")
        actionRdd.cache()


        val top10Ids = top10Category(actionRdd)

        // 过滤原始数据， 保留点击和前10品类的ID
        val filterActionRdd = actionRdd.filter(
            action => {
                val datas = action.split("_")
                if (datas(6) != -1) {
                    top10Ids.contains(datas(6))
                } else {
                    false
                }
            }
        )


        // 根据品类ID和sessionID进行点击量的统计
        val reduceRdd = filterActionRdd.map(
            action => {
                val datas = action.split("_")
                ((datas(6), datas(2)), 1)
            }
        ).reduceByKey(_ + _)


        // 将统计的结果进行结构的转换
        // ((品类ID， sessionID), sum) =》(品类ID， (sessionID, sum))
        val mapRdd = reduceRdd.map {
            case ((cid, sid), sum) => {
                (cid, (sid, sum))
            }
        }

        // 相同的品类进行分组
        val groupRdd = mapRdd.groupByKey()

        // 分组后的数据进行点击量的排序，取前10名
        val resultRdd = groupRdd.mapValues(
            iter => {
                iter.toList.sortBy(_._2)(Ordering.Int.reverse).take(10)
            }
        )

        resultRdd.collect().foreach(println)




        sc.stop()
    }

    def top10Category(actionRdd: RDD[String]): Array[(String)] ={

        val flatRdd = actionRdd.flatMap(
            action => {
                val datas = action.split("_")
                if (datas(6) != "-1") {
                    // 点击的场合
                    List((datas(6), (1, 0, 0)))
                } else if (datas(8) != "null") {
                    // 下单的场合
                    val ids = datas(8).split(",")
                    ids.map(id => (id, (0, 1, 0)))
                } else if (datas(10) != "null") {
                    // 支付的场合
                    val ids = datas(10).split(",")
                    ids.map(id => (id, (0, 0, 1)))
                } else {
                    Nil
                }
            }
        )

        val analysisRdd = flatRdd.reduceByKey(
            (t1, t2) => {
                (t1._1 + t2._1, t1._2 + t2._2, t1._3 + t2._3)
            }
        )

        analysisRdd.sortBy(_._2, false).take(10).map(_._1)
    }

}
