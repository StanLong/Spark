package com.stanlong.spark.core

import org.apache.spark.{SparkConf, SparkContext}

object Test {
    def main(args: Array[String]): Unit = {
        // 建立和Spark框架的连接
        val sparkConf = new SparkConf().setMaster("local").setAppName("WordCount")
        val sc = new SparkContext(sparkConf)

        // 执行业务操作
        val lines = sc.textFile("datas/1.txt") // 读取文件

        val words = lines.flatMap(_.split(" ")) // 获取一行一行的数据,扁平化操作：将数据按空格隔开

        val wordGroup = words.groupBy(word => word) // 根据单词进行分组， 便于统计

        // 方法一
        // val wordToCount =  wordGroup.map( // 对分组后的数据进行转换
        //         word => {
        //         (word._1, word._2.size)
        //     }
        // )

        // 方法二 case替代map(word=>)的写法 不在使用._1 ._2  上一个父rdd的类型值可直接命名为变量使用
        val wordToCount = wordGroup.map{
            case(word, list) =>{
                (word, list.size)
            }
        }



        val array = wordToCount.collect()
        array.foreach(println) // 将转换结果采集到控制台打印出来

        // 关闭连接
        sc.stop()
    }

}
