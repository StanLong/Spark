package com.stanlong.spark.core.test

import java.io.ObjectInputStream
import java.net.ServerSocket

object Executor2 {
    def main(args: Array[String]): Unit = {
        val server = new ServerSocket(8888)
        println("8888服务器启动，等待接收数据")

        val client = server.accept()
        val in = client.getInputStream
        val objIn = new ObjectInputStream(in)
        val subTask = objIn.readObject().asInstanceOf[SubTask]
        val ints = subTask.compute()
        println("8888节点计算结果为: " + ints)
        objIn.close()
        client.close()
        server.close()
    }
}
