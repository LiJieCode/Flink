package edu.lzu.chap05

import org.apache.flink.streaming.api.scala._

object Demo01 {
    def main(args: Array[String]): Unit = {
        // createLocalEnvironment   创建本地环境
        // createRemoteEnvironment  创建远程环境
        // getExecutionEnvironment  智能化创建环境
        val env: StreamExecutionEnvironment = StreamExecutionEnvironment.getExecutionEnvironment


        env.execute()

    }
}
