# **Spark** 案例实操

## 数据准备

![](../doc/61.png)

上面的数据图是从数据文件中截取的一部分内容，表示为电商网站的用户行为数据，主要包含用户的 4 种行为：搜索，点击，下单，支付。数据规则如下：

Ø 数据文件中每行数据采用下划线分隔数据

Ø 每一行数据表示用户的一次行为，这个行为只能是 4 种行为的一种

Ø  如果搜索关键字为 null,表示数据不是搜索数据

Ø  如果点击的品类 ID 和产品 ID 为-1，表示数据不是点击数据

Ø  针对于下单行为，一次可以下单多个商品，所以品类 ID 和产品 ID 可以是多个，id 之间采用逗号分隔，如果本次不是下单行为，则数据采用 null 表示

Ø 支付行为和下单行为类似 

详细字段说明：

| 编号 | 字段名称           | 字段类型 | 字段含义                     |
| ---- | ------------------ | -------- | ---------------------------- |
| 1    | date               | String   | 用户点击行为的日期           |
| 2    | user_id            | Long     | 用户的 ID                    |
| 3    | session_id         | String   | Session 的 ID                |
| 4    | page_id            | Long     | 某个页面的 ID                |
| 5    | action_time        | String   | 动作的时间点                 |
| 6    | search_keyword     | String   | 用户搜索的关键词             |
| 7    | click_category_id  | Long     | 某一个商品品类的 ID          |
| 8    | click_product_id   | Long     | 某一个商品的 ID              |
| 9    | order_category_ids | String   | 一次订单中所有品类的 ID 集合 |
| 10   | order_product_ids  | String   | 一次订单中所有商品的 ID 集合 |
| 11   | pay_category_ids   | String   | 一次支付中所有品类的 ID 集合 |
| 12   | pay_product_ids    | String   | 一次支付中所有商品的 ID 集合 |
| 13   | city_id            | Long     | 城市 id                      |

## 需求 1：Top10 热门品类

```scala
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
```



