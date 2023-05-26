package edu.lzu.chap02

// import org.apache.flink.streaming.api.scala.StreamExecutionEnvironment
import org.apache.flink.streaming.api.scala._


/**
 *
 * 有界流
 *
 * DataStreaming API
 *
 */
object Demo02 {
    def main(args: Array[String]): Unit = {

        val env: StreamExecutionEnvironment = StreamExecutionEnvironment.getExecutionEnvironment

        // 读一条，打印一条
        val lineDataStream: DataStream[String] = env.readTextFile("data/words.txt")

        val mapDS: DataStream[(String, Int)] = lineDataStream.flatMap(_.split(" ")).map((_, 1))

        val keyDS: KeyedStream[(String, Int), String] = mapDS.keyBy(_._1)

        val wc: DataStream[(String, Int)] = keyDS.sum(1)

        wc.print()

        // 执行当前的任务
        // 流式处理和批处理最大的区别
        env.execute()

        // 3> (hello,1)
        // 1> (scala,1)
        // 1> (spark,1)
        // 3> (hello,2)
        // 3> (hello,3)
        // 7> (flink,1)
        // 这个结果是：多线程并行执行的，前面那个数字是编号
    }
}
