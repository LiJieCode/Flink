package edu.lzu.chap02

import org.apache.flink.api.java.utils.ParameterTool
import org.apache.flink.streaming.api.scala._

/**
 *
 * 无界流
 *
 * DataStreaming API
 *
 */
object Demo04 {
    def main(args: Array[String]): Unit = {

        // 流式处理环境
        val env: StreamExecutionEnvironment = StreamExecutionEnvironment.getExecutionEnvironment
        env.disableOperatorChaining()

        // 这里用的是socketTextStream
        // 要给出  主机名  端口号
        val parameterTool = ParameterTool.fromArgs(args)
        val hostname = parameterTool.get("host")
        val port = parameterTool.getInt("port")
        val lineDataStream: DataStream[String] = env.socketTextStream(hostname, port)

        val mapDS: DataStream[(String, Int)] = lineDataStream.flatMap(_.split(" ")).map((_, 1))

        val keyDS: KeyedStream[(String, Int), String] = mapDS.keyBy(_._1)

        val wc: DataStream[(String, Int)] = keyDS.sum(1)

        wc.print()

        // 执行任务
        env.execute()
    }
}