package com.stanlong.spark.sql

import org.apache.spark.SparkConf
import org.apache.spark.sql.{SaveMode, SparkSession}

object Spark02_SparkSql_JDBC {

    def main(args: Array[String]): Unit = {
        // 创建SparkSQl的运行环境
        val sparkSQLConf = new SparkConf().setMaster("local[*]").setAppName("sparkSQL")
        val spark = SparkSession.builder().config(sparkSQLConf).getOrCreate()
        // 在使用DataFrame时，如果涉及到转换操作，需要引入转换规则
        import spark.implicits._

        // 读取mysql数据
        val df = spark.read.format("jdbc")
          .option("url", "jdbc:mysql://127.0.0.1:3306/mysql")
          .option("driver", "com.mysql.jdbc.Driver")
          .option("user", "root")
          .option("password", "root")
          .option("dbtable", "user")
          .load()

        df.show()

        // 保存数据到mysql
        df.write.format("jdbc")
          .option("url", "jdbc:mysql://127.0.0.1:3306/mysql")
          .option("driver", "com.mysql.jdbc.Driver")
          .option("user", "root")
          .option("password", "root")
          .option("dbtable", "scoot") // 把df中的内容保存到一张新表，表明 scoot
          .mode(SaveMode.Append)
          .save()


        // 关闭环境
        spark.close()
    }

}
