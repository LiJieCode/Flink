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

        // 增加数据源
        // addSource(new FlinkKafkaConsumer[String]())  这是flink1.13里面的实现
        // [String]这里的泛型就是，将读取的数据解析成什么样子。
        // Flink 1.14 之后的版本，new KafkaSource
        val data: DataStream[String] = env.addSource(new FlinkKafkaConsumer[String]("clicks", new SimpleStringSchema(), properties))


        // 打印数据
        data.print()

        env.execute()
    }

}
