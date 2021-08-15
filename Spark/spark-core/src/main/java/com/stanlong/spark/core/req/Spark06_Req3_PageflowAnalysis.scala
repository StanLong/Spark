package com.stanlong.spark.core.req

import org.apache.spark.{SparkConf, SparkContext}

/**
 * 页面单跳转换率统计
 */
object Spark06_Req3_PageflowAnalysis {

    def main(args: Array[String]): Unit = {
        val sparkConf = new SparkConf().setMaster("local[*]").setAppName("HotCategoryTop10")
        val sc = new SparkContext(sparkConf)

        val actionRdd = sc.textFile("datas/user_visit_action.txt")

        val actionDataRdd = actionRdd.map(
            action => {
                val datas = action.split("_")
                UserVisitAction(
                    datas(0),
                    datas(1).toLong,
                    datas(2),
                    datas(3).toLong,
                    datas(4),
                    datas(5),
                    datas(6).toLong,
                    datas(7).toLong,
                    datas(8),
                    datas(9),
                    datas(10),
                    datas(11),
                    datas(12).toLong
                )
            }
        )

        actionDataRdd.cache()


        // 对指定的页面连续跳转进行统计
        val ids = List(1,2,3,4,5,6,7)

        val okflowIds = ids.zip(ids.tail)

        // 计算分母
        val pageIdToCountMap = actionDataRdd.filter(
            action => {
                ids.init.contains(action.page_id) // ids.init 不统计列表的最后一个
            }
        ).map(
            action => {
                (action.page_id, 1L)
            }
        ).reduceByKey(_+_).collect().toMap

        // 计算分子
        // 根据Session进行分组
        val sessionRdd = actionDataRdd.groupBy(_.session_id)
        // 分组后根据访问时间排序（升序）
        val mvRdd = sessionRdd.mapValues(
            iter => {
                val sortList = iter.toList.sortBy(_.action_time)

                // 原始 [1,2,3,4]
                // 想要得到[1-2, 2-3, 3-4]
                // [1,2,3,4]
                // [2,3,4]
                // 使用zip
                val flowIds = sortList.map(_.page_id)
                val pageFlowIds = flowIds.zip(flowIds.tail)

                // 将不合法的跳转页面进行过滤
                pageFlowIds.filter(
                    t => {
                        okflowIds.contains(t)
                    }
                ).map(
                    t => {
                        (t, 1)
                    }
                )

            }
        )

        val flatRdd = mvRdd.map(_._2).flatMap(list => list)
        val dataRdd = flatRdd.reduceByKey(_ + _)

        // 计算单跳转换率
        dataRdd.foreach{
            case ((pageId1, pageId2), sum) => {
                val lon = pageIdToCountMap.getOrElse(pageId1, 0L)
                println(s"页面${pageId1}跳转到页面${pageId2}的单跳转换率为: " + (sum.toDouble / lon))
            }
        }



        sc.stop()
    }

    //用户访问动作表
    case class UserVisitAction(
        date: String,//用户点击行为的日期
        user_id: Long,// 用 户 的 ID
        session_id: String,//Session 的 ID
        page_id: Long,// 某 个 页 面 的 ID
        action_time: String,//动作的时间点
        search_keyword: String,//用户搜索的关键词
        click_category_id: Long,// 某 一 个 商 品 品 类 的 ID
        click_product_id: Long,// 某 一 个 商 品 的 ID
        order_category_ids: String,//一次订单中所有品类的 ID 集合
        order_product_ids: String,//一次订单中所有商品的 ID 集合
        pay_category_ids: String,//一次支付中所有品类的 ID 集合
        pay_product_ids: String,//一次支付中所有商品的 ID 集合
        city_id: Long //城市 id
    )
}
