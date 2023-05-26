package edu.lzu.chap02

// import org.apache.flink.api.scala.{DataSet, ExecutionEnvironment}
import org.apache.flink.api.scala._

/**
 *
 * 批处理
 *
 * DataSet API
 *
 */
object Demo01 {
    def main(args: Array[String]): Unit = {
        val env: ExecutionEnvironment = ExecutionEnvironment.getExecutionEnvironment

        val lineData: DataSet[String] = env.readTextFile("data/words.txt")
        // lineData 的数据是这样的：
        // hello spark
        // hello flink
        // hello scala

        // val flatDS: DataSet[String] = lineData.flatMap(_.split(" "))
        val flatDS: DataSet[String] = lineData.flatMap(line => line.split(" "))
        // flatDS 的数据是这样的：
        // hello
        // spark
        // hello
        // flink
        // hello
        // scala

        val mapDS: DataSet[(String, Int)] = flatDS.map((_, 1))
        // mapDS 的数据是这样的:
        // (hello,1)
        // (spark,1)
        // (hello,1)
        // (scala,1)
        // (hello,1)
        // (flink,1)

        val groupDS: GroupedDataSet[(String, Int)] = mapDS.groupBy(0)

        val wc: AggregateDataSet[(String, Int)] = groupDS.sum(1)

        wc.print()

    }
}
