package com.stanlong.spark.sql

import org.apache.spark.SparkConf
import org.apache.spark.sql.SparkSession

object Spark03_SparkSql_Hive {

    def main(args: Array[String]): Unit = {
        // 创建SparkSQl的运行环境
        val sparkSQLConf = new SparkConf().setMaster("local[*]").setAppName("sparkSQL")
        val spark = SparkSession.builder().enableHiveSupport().config(sparkSQLConf).getOrCreate() // enableHiveSupport() 启用hive支持
        // 在使用DataFrame时，如果涉及到转换操作，需要引入转换规则
        import spark.implicits._

        // 使用SparkSQl 连接外置的hive
        // 1.拷贝hive-site到resource目录下
        // 2.启动hive支持
        // 3.导入依赖包

        System.setProperty("HADOOP_USER_NAME", "root") // 防止访问数据库时报权限错误
        spark.sql("show tables").show()



        // 关闭环境
        spark.close()
    }

}
