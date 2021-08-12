package com.stanlong.spark.core.req

import org.apache.spark.{SparkConf, SparkContext}

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

        // 统计品类的点击数量
        val clickActionRdd = actionRdd.filter(
            action => {
                val datas = action.split("_")
                datas(6) != "-1"
            }
        )

        val clickCountRdd = clickActionRdd.map(
            action => {
                val datas = action.split("_")
                (datas(6), 1)
            }
        ).reduceByKey(_+_)

        // 统计品类的下单数量
        val orderActionRdd = actionRdd.filter(
            action => {
                var datas = action.split("_")
                datas(8) != "null"
            }
        )

        val orderCountRdd = orderActionRdd.flatMap(
            action => {
                val datas = action.split("_")
                val cid = datas(8)
                val cids = cid.split(",")
                cids.map(id => (id, 1))
            }
        ).reduceByKey(_+_)

        // 统计品类的支付数量
        val payActionRdd = actionRdd.filter(
            action => {
                var datas = action.split("_")
                datas(10) != "null"
            }
        )

        val payCountRdd = payActionRdd.flatMap(
            action => {
                val datas = action.split("_")
                val cid = datas(10)
                val cids = cid.split(",")
                cids.map(id => (id, 1))
            }
        ).reduceByKey(_+_)


        // 将品类进行排序，并取前十
        // 先按点击量排序，再按下单量排序，最后按支付数量排序
        val cogroupRdd = clickCountRdd.cogroup(orderCountRdd, payCountRdd)
        val analysisRdd = cogroupRdd.mapValues {
            case (clickIter, orderIter, payIter) => {
                var clickCnt = 0
                val iter1 = clickIter.iterator
                if (iter1.hasNext) {
                    clickCnt = iter1.next()
                }

                var orderCnt = 0
                val iter2 = orderIter.iterator
                if (iter2.hasNext) {
                    orderCnt = iter2.next()

                }

                var payCnt = 0
                val iter3 = payIter.iterator
                if (iter3.hasNext) {
                    payCnt = iter3.next()
                }
                (clickCnt, orderCnt, payCnt)
            }
        }

        val resultRdd = analysisRdd.sortBy(_._2, false).take(10)

        // 将结果打印到控制台
        resultRdd.foreach(println)

        sc.stop()
    }

}
