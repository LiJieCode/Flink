package edu.lzu.chap05

import org.apache.flink.streaming.api.scala._

object Flink06_Transformation_KeyBy {
    def main(args: Array[String]): Unit = {
        val env: StreamExecutionEnvironment = StreamExecutionEnvironment.getExecutionEnvironment
        env.setParallelism(1)




        env.execute()
    }
    case class Event(user: String, url: String, timestamp: Long)

}
