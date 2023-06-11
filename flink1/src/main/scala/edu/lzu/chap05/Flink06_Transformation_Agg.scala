package edu.lzu.chap05

import org.apache.flink.api.java.functions.KeySelector
import org.apache.flink.streaming.api.scala._

object Flink06_Transformation_Agg {
    def main(args: Array[String]): Unit = {
        val env: StreamExecutionEnvironment = StreamExecutionEnvironment.getExecutionEnvironment
        env.setParallelism(1)


        val data: DataStream[Event] = env.fromElements(
            Event("zhang1", "xx.xx.xxx11", 500L),
            Event("zhang2", "xx.xx.xxx21", 200L),
            Event("zhang3", "xx.xx.xxx31", 300L),
            Event("zhang1", "xx.xx.xxx12", 100L),
            Event("zhang1", "xx.xx.xxx13", 600L)
        )

        val data1: DataStream[(String, Int)] = env.fromCollection(List(
            ("a", 1), ("b", 2), ("c", 3), ("a", 4), ("a", 5), ("b", 6), ("b", 7)
        ))

        // todo 聚合操作其实分两步；第一步：keyBy，第二步：聚合函数

        // 演示KeyBy
        // data.print()
        // data.keyBy( new MyKeySelector() ).print()

        // 简单聚合
        // 聚合函数中，可以传入位置，也可以传入字段名
//        data.keyBy( new MyKeySelector() )
//          .maxBy("timestamp")
//          .print()

        println("------------------")

        data1.keyBy(0)
          .sum(1)
          .print()


        env.execute()
    }

    class MyKeySelector() extends KeySelector[Event, String]{
        override def getKey(in: Event): String = in.user
    }

    case class Event(user: String, url: String, timestamp: Long)

}
