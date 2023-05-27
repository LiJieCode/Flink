package edu.lzu.chap05

import org.apache.flink.api.common.functions.MapFunction
import org.apache.flink.streaming.api.scala._

object Flink05_Transformation_FlatMap {
    def main(args: Array[String]): Unit = {
        val env: StreamExecutionEnvironment = StreamExecutionEnvironment.getExecutionEnvironment
        env.setParallelism(1)

        val data: DataStream[Int] = env.fromElements(1, 2, 3, 4, 5)
        val data1: DataStream[Event] = env.fromElements(
            Event("zhang1", "xx.xx.x", 123456L),
            Event("zhang2", "xx.xx.xx", 123457L),
            Event("zhang3", "xx.xx.xxx", 123458L)
        )

        // flatMap
        // 使用匿名函数




        env.execute()
    }
    case class Event(user: String, url: String, timestamp: Long)

}
