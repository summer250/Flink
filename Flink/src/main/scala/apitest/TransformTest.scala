package apitest

import org.apache.flink.streaming.api.scala._

object TransformTest {
  def main(args: Array[String]): Unit = {
    val env = StreamExecutionEnvironment.getExecutionEnvironment
    //0.读取数据
    val inputpath ="src/main/resources/1.txt"
    val inputStream = env.readTextFile(inputpath)

    //1.先转换成样例类类型(简单转换操作)
    val dataStream = inputStream
      .map(data =>{
        val arr = data.split(",")
        SensorReading(arr(0),arr(1).toLong,arr(2).toDouble)
      })
    //2.分组聚合,输出每个传感器当前最小值
    val aggStream = dataStream
      .keyBy(_.id) //根据id进行分组
      .minBy("temperature")
//    aggStream.print()

    //3.需要输出当前最小的温度值,以及最近的时间戳,要用reduce
//    val resultStream: DataStream[SensorReading] = dataStream
//      .keyBy(_.id)
//      .reduce((curState, newData) =>
//        SensorReading(curState.id, newData.timestamp, curState.temperature.min(newData.temperature)))
//    resultStream.print()

    //4.多流转换操作
    //4.1 分流,将传感器温度数据分成低温,高温两条流
    val splitStream: SplitStream[SensorReading] = dataStream.split(data => {
      if (data.temperature > 30.0) Seq("高温") else Seq("低温")
    })

    val highTempStream: DataStream[SensorReading] = splitStream.select("高温")
    val lowTempStream: DataStream[SensorReading] = splitStream.select("低温")
    val allTempStream: DataStream[SensorReading] = splitStream.select("高温", "低温")

//    highTempStream.print("高温")
//    lowTempStream.print("低温")
//    allTempStream.print("全部")

    //4.2 合流,connect
    val warningStream: DataStream[(String, Double)] = highTempStream.map(data => (data.id, data.temperature))
    val connectedStream: ConnectedStreams[(String, Double), SensorReading] = warningStream.connect(lowTempStream)

    //4.3 union合流
    val unionSream = highTempStream.union(lowTempStream)

    //用comap对数据进行分别处理
    val coMapResultStream = connectedStream
      .map(warningData => (warningData._1, warningData._2, "危险"), lowTemData => (lowTemData.id, "健康"))

    coMapResultStream.print("comap")


    env.execute("transform test")
  }
}
