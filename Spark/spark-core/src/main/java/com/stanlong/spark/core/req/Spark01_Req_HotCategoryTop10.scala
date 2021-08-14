package com.stanlong.spark.core.req

import org.apache.spark.util.AccumulatorV2
import org.apache.spark.{SparkConf, SparkContext}

import scala.collection.mutable

/**
 * Top10热门品类
 */
object Spark01_Req_HotCategoryTop10 {

    def main(args: Array[String]): Unit = {
        val sparkConf = new SparkConf().setMaster("local[*]").setAppName("HotCategoryTop10")
        val sc = new SparkContext(sparkConf)

        // 读取原始日志数据
        val actionRdd = sc.textFile("datas/user_visit_action.txt")
        // 2019-07-17_95_26070e87-1ad7-49a3-8fb3-cc741facaddf_37_2019-07-17 00:00:02_手机_-1_-1_null_null_null_null_3
        // 2019-07-17_63_837f7970-3f77-4337-812c-c1e790ca05b7_39_2019-07-17 00:05:24_null_-1_-1_13,17,14,11_1,88,94_null_null_14

        // 声明累加器
        val acc = new HotCategoryAccumulator
        sc.register(acc, "hotCategory")

        actionRdd.foreach(
            action => {
                val datas = action.split("_")
                if (datas(6) != "-1") {
                    // 点击的场合
                    acc.add((datas(6), "click"))
                } else if (datas(8) != "null") {
                    // 下单的场合
                    val ids = datas(8).split(",")
                    ids.foreach(
                        id => {
                            acc.add((id, "order"))
                        }
                    )
                } else if (datas(10) != "null") {
                    // 支付的场合
                    val ids = datas(10).split(",")
                    ids.foreach(
                        id => {
                            acc.add((id, "pay"))
                        }
                    )
                } else {
                    Nil
                }
            }
        )


        // 将相同品类ID的数据进行分组聚合
        //  (品类ID ,(点击数量，下单数量，支付数量))
        val accVal = acc.value
        val categories = accVal.map((_._2))
        val sort = categories.toList.sortWith(
            (left, right) => {
                if (left.clickCnt > right.clickCnt) {
                    true
                } else if (left.clickCnt == right.clickCnt) {
                    if (left.orderCnt > right.orderCnt) {
                        true
                    } else if (left.orderCnt == right.orderCnt) {
                        left.payCnt > right.payCnt
                    } else {
                        false
                    }
                } else {
                    false
                }
            }
        )

        // 将结果打印到控制台
        sort.take(10).foreach(println)

        sc.stop()
    }

    case class HotCategory(cid:String, var clickCnt:Int, var orderCnt:Int, var payCnt:Int)

    /**
     * 自定义累加器
     * 1. 继承 AccumulatorV2
     *      IN:(品类ID， 行为类型)
     *      OUT:mutable.Map[String, HotCategory]
     * 2. 重写方法
     */
    class HotCategoryAccumulator extends AccumulatorV2[(String, String), mutable.Map[String, HotCategory]]{

        private val hcMap = mutable.Map[String, HotCategory]()

        override def isZero: Boolean = {
            hcMap.isEmpty
        }

        override def copy(): AccumulatorV2[(String, String), mutable.Map[String, HotCategory]] = {
            new HotCategoryAccumulator()
        }

        override def reset(): Unit = {
            hcMap.clear()
        }

        override def add(v: (String, String)): Unit = {
            val cid = v._1
            val actionType = v._2
            val category = hcMap.getOrElse(cid, HotCategory(cid, 0, 0, 0))
            if(actionType == "click"){
                category.clickCnt += 1
            }else if(actionType == "order"){
                category.orderCnt += 1
            }else if(actionType == "pay"){
                category.payCnt += 1
            }
            hcMap.update(cid, category)
        }

        override def merge(other: AccumulatorV2[(String, String), mutable.Map[String, HotCategory]]): Unit = {
            val map1 = this.hcMap
            val map2 = other.value
            map2.foreach{
                case(cid, hc) => {
                    val category = map1.getOrElse(cid, HotCategory(cid, 0, 0, 0))
                    category.clickCnt += hc.clickCnt
                    category.orderCnt += hc.orderCnt
                    category.payCnt += hc.payCnt
                    map1.update(cid, category)
                }
            }
        }

        override def value: mutable.Map[String, HotCategory] = hcMap
    }

}
