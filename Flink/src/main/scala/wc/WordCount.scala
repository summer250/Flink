package wc

import org.apache.flink.streaming.api.scala._

object WordCount {
  def main(args: Array[String]): Unit = {
    //创建流处理的执行环境
    val env: StreamExecutionEnvironment = StreamExecutionEnvironment.getExecutionEnvironment

    //接收一个socket文本流
    val inputDataStream: DataStream[String] = env.socketTextStream("localhost", 7777)

    //进行转化处理统计
    val resultDataStream = inputDataStream
      .flatMap(_.split(" "))
      .map((_,1))
      .keyBy(0)
      .sum(1)

    resultDataStream.print()

    //启动任务执行;/.
    env.execute("stream word cunt")
  }
}
