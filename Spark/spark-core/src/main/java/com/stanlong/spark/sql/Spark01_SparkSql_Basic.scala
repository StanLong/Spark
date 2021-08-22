package com.stanlong.spark.sql

import org.apache.spark.SparkConf
import org.apache.spark.sql.{Encoder, Encoders, SparkSession, functions}
import org.apache.spark.sql.expressions.Aggregator

object Spark01_SparkSql_Basic {

    def main(args: Array[String]): Unit = {
        // 创建SparkSQl的运行环境
        val sparkSQLConf = new SparkConf().setMaster("local[*]").setAppName("sparkSQL")
        val spark = SparkSession.builder().config(sparkSQLConf).getOrCreate()
        // 在使用DataFrame时，如果涉及到转换操作，需要引入转换规则
        import spark.implicits._

        val df = spark.read.json("datas/user.json")
        df.createOrReplaceTempView("user")

        // 用户自定义UDAF函数
        spark.udf.register("ageAvg", functions.udaf(new MyAvgUDAF()))

        spark.sql("select ageAvg(age) from user").show()
        // 关闭环境
        spark.close()
    }


    /**
     * 自定义聚合函数类: 计算年龄的平均值
     * 1. 继承 org.apache.spark.sql.expressions.Aggregator 定义泛型
     *  IN: 输入的数据类型
     *  BUF: 缓冲取的数据类型
     *  OUT: 输出的数据类型
     *
     * 2 .重写方法
     */
    case class Buff(var total:Long, var count:Long)
    class MyAvgUDAF extends  Aggregator[Long, Buff, Long] {
        // 初始化
        override def zero: Buff = {
            Buff(0L, 0L)
        }

        // 根据输入的数据更新缓冲取的数据
        override def reduce(buff: Buff, in: Long): Buff = {
            buff.total = buff.total + in
            buff.count = buff.count + 1
            buff
        }

        // 合并缓冲区
        override def merge(buff1: Buff, buff2: Buff): Buff = {
            buff1.total = buff1.total + buff2.total
            buff1.count = buff1.count + buff2.count
            buff1
        }

        // 计算结果
        override def finish(buff: Buff): Long = {
            buff.total / buff.count
        }

        // 缓冲区的编码操作
        override def bufferEncoder: Encoder[Buff] = Encoders.product

        // 输出的编码操作
        override def outputEncoder: Encoder[Long] = Encoders.scalaLong
    }
}
