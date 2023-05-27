package edu.lzu.chap05

import org.apache.flink.api.common.functions.MapFunction
import org.apache.flink.streaming.api.scala._

object Flink03_Transformation_Map {
    def main(args: Array[String]): Unit = {
        val env: StreamExecutionEnvironment = StreamExecutionEnvironment.getExecutionEnvironment
        env.setParallelism(1)

        val data: DataStream[Int] = env.fromElements(1, 2, 3, 4, 5)
        val data1: DataStream[Event] = env.fromElements(
            Event("zhang1", "xx.xx.x", 123456L),
            Event("zhang2", "xx.xx.xx", 123457L),
            Event("zhang3", "xx.xx.xxx", 123458L)
        )

        // Map
        // 使用匿名函数
        val dataMap: DataStream[Int] = data.map(_ + 12)
        val data1Map: DataStream[String] = data1.map(
            event => {
                event.user
            }
        )

        // 实现MapFunction
        data1.map(new UserExtractor).print("u")


        data1Map.print()


        env.execute()
    }
    case class Event(user: String, url: String, timestamp: Long)

    // 实现MapFunction
    class UserExtractor extends MapFunction[Event, String]{
        override def map(t: Event): String = t.user
    }
}
