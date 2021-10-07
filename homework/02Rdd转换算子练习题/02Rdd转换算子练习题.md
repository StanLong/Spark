1. 从服务器日志apache.txt中获取每个时间段访问量

   样例数据

   ```
   83.149.9.216 - - 17/05/2015:10:05:03 +0000 GET /presentations/logstash-monitorama-2013/images/kibana-search.png
   83.149.9.216 - - 17/05/2015:10:05:43 +0000 GET /presentations/logstash-monitorama-2013/images/kibana-dashboard3.png
   83.149.9.216 - - 17/05/2015:10:05:47 +0000 GET /presentations/logstash-monitorama-2013/plugin/highlight/highlight.js
   ```

   ```scala
   package com.stanlong.spark.core.rdd.operator.transform
   
   import java.text.SimpleDateFormat
   
   import org.apache.spark.{HashPartitioner, SparkConf, SparkContext}
   
   object Spark01_RDD_Operator_Transform {
   
       def main(args: Array[String]): Unit = {
           val sparkConf = new SparkConf().setMaster("local[*]").setAppName("RDD")
           val sc = new SparkContext(sparkConf)
   
   
           val rdd = sc.textFile("datas/apache.txt")
           val timeRdd = rdd.map(
               line => {
                   val datas = line.split(" ")
                   val time = datas(3)
                   val sdf = new SimpleDateFormat("dd/MM/yyyy:HH:mm:ss")
                   val date = sdf.parse(time)
                   val sdf1 = new SimpleDateFormat("HH")
                   val hour = sdf1.format(date)
                   (hour, 1)
               }
           ).groupBy(_._1)
           timeRdd.map{
               case (hour, iter) => {
                   (hour, iter.size)
               }
           }.collect().foreach(println)
           sc.stop()
       }
   }
   ```

2. 案例实操

   1)    数据准备

   ​	agent.txt：时间戳，省份，城市，用户，广告，中间字段使用空格分隔。

   样例数据

   ```scala
   1516609143867 6 7 64 16
   1516609143869 9 4 75 18
   1516609143869 1 7 87 12
   ```

   2)    需求描述

   ​	统计出每一个省份每个广告被点击数量排行的 Top3

   3)    需求分析

   ​	缺什么补什么，多什么删什么

   4)    功能实现

   ```scala
   package com.stanlong.spark.core.rdd.operator.transform
   
   import java.text.SimpleDateFormat
   
   import org.apache.spark.{HashPartitioner, SparkConf, SparkContext}
   
   object Spark01_RDD_Operator_Transform {
   
       def main(args: Array[String]): Unit = {
           val sparkConf = new SparkConf().setMaster("local[*]").setAppName("RDD")
           val sc = new SparkContext(sparkConf)
   
           // 1. 获取原始数据： 时间戳 省份 城市 用户 广告
           val dataRdd = sc.textFile("datas/agent.txt")
   
           // 2. 将原始数据进行结构转换
           // 时间戳 省份 城市 用户 广告 =》 ((省份, 广告), 1)
           val mapRdd = dataRdd.map(
               line => {
                   val datas = line.split(" ")
                   ((datas(1), datas(4)), 1)
               }
           )
   
           // 3. 将转换结构后的数据进行分组聚合
           // ((省份, 广告), 1) => ((省份, 广告), sum)
           val reduceRdd = mapRdd.reduceByKey(_ + _)
   
           // 4. 将聚合的结果进行结构转换
           // (省份,( 广告, sum))
           val newMapRdd = reduceRdd.map {
               case ((prv, ad), sum) => {
                   (prv, (ad, sum))
               }
           }
   
           // 5. 将转换结构后的数据按照省份进行分组
           // (省份,[(广告A, sum), (广告B, sum)])
           val groupRdd = newMapRdd.groupByKey()
   
           // 6. 分组后的数据按照组内排序（降序）, 取前三
           val resultRdd = groupRdd.mapValues(
               iter => {
                   iter.toList.sortBy(_._2)(Ordering.Int.reverse).take(3)
               }
           )
   
           // 7.采集数据打印到控制台
           resultRdd.collect().foreach(println)
           
           sc.stop()
       }
   }
   ```
   
   

