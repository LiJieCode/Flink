package edu.lzu.chap05

import org.apache.flink.streaming.api.scala._


object Flink01_SourceBoundedTest {
    def main(args: Array[String]): Unit = {
        // 创建环境
        val env: StreamExecutionEnvironment = StreamExecutionEnvironment.getExecutionEnvironment
        env.setParallelism(1)  // 设置并行度为1

        // 1、从元素中读取数据
        val data: DataStream[Int] = env.fromElements(1, 2, 3, 4)
        val data1: DataStream[Event] = env.fromElements(
            Event("zhang1", "xx.xx.x", 123456L),
            Event("zhang2", "xx.xx.xx", 123457L),
            Event("zhang3", "xx.xx.xxx", 123458L)
        )

        // 2、从集合中读取数据
        val events: List[Event] = List(
            Event("zhang1", "xx.xx.x", 123456L),
            Event("zhang2", "xx.xx.xx", 123457L),
            Event("zhang3", "xx.xx.xxx", 123458L)
        )
        val data2: DataStream[Event] = env.fromCollection(events)


        // 3、从文件读取数据
        val data3: DataStream[String] = env.readTextFile("data/clicks.txt")



        // 打印数据
        // data.print("num")
        // data1.print()
        // data2.print()
        data3.print()



        env.execute()
    }


    case class Event(user:String, url:String, timestamp: Long)
}
