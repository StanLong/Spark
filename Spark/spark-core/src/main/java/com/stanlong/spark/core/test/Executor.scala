package com.stanlong.spark.core.test

import java.io.ObjectInputStream
import java.net.ServerSocket

object Executor {
    def main(args: Array[String]): Unit = {
        val server = new ServerSocket(9999)
        println("9999服务器启动，等待接收数据")

        val client = server.accept()
        val in = client.getInputStream
        val objIn = new ObjectInputStream(in)
        val subTask = objIn.readObject().asInstanceOf[SubTask]
        val ints = subTask.compute()
        println("9999节点计算结果为: " + ints)
        objIn.close()
        client.close()
        server.close()
    }
}
