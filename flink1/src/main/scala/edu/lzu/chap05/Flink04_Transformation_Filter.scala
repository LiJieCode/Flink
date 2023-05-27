package edu.lzu.chap05

import org.apache.flink.api.common.functions.{FilterFunction, MapFunction}
import org.apache.flink.streaming.api.scala._

object Flink04_Transformation_Filter {
    def main(args: Array[String]): Unit = {
        val env: StreamExecutionEnvironment = StreamExecutionEnvironment.getExecutionEnvironment
        env.setParallelism(1)

        val data: DataStream[Int] = env.fromElements(1, 2, 3, 4, 5)
        val data1: DataStream[Event] = env.fromElements(
            Event("zhang1", "xx.xx.x", 123456L),
            Event("zhang2", "xx.xx.xx", 123457L),
            Event("zhang3", "xx.xx.xxx", 123458L)
        )

        // Filter  过滤
        // 使用匿名函数
         data.filter(
             num => {
                 num % 2 == 0
             }
         ).print("filter")

        // 实现 FilterMap
        data1.filter(new UserExtractor).print("u")



        env.execute()
    }
    case class Event(user: String, url: String, timestamp: Long)

    class UserExtractor extends FilterFunction[Event]{
        override def filter(t: Event): Boolean = t.user == "zhangsan1"
    }

}
