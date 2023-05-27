package edu.lzu.chap05

import org.apache.flink.api.common.serialization.SimpleStringSchema
import org.apache.flink.streaming.api.scala._
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaConsumer

import java.util.Properties

/**
 * 读取无界数据源 kafka数据
 * 生产中，最多的就是读取kafka的数据
 */
object Flink02_SourceKafkaTest {
    def main(args: Array[String]): Unit = {
        // 创建环境
        val env: StreamExecutionEnvironment = StreamExecutionEnvironment.getExecutionEnvironment
        env.setParallelism(1)  // 设置并行度为1

        // 1、从kafka读取数据
        val properties: Properties = new Properties()

        properties.put("bootstrap.servers", "l9z102:9092")
        properties.setProperty("group.id", "li")

        //
        val data: DataStream[String] = env.addSource(new FlinkKafkaConsumer[String]("clicks", new SimpleStringSchema(), properties))


        // 打印数据
        data.print()

        env.execute()
    }

}
