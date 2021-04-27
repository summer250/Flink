package apitest

import org.apache.flink.streaming.api.TimeCharacteristic
import org.apache.flink.streaming.api.functions.timestamps.BoundedOutOfOrdernessTimestampExtractor
import org.apache.flink.streaming.api.scala._
import org.apache.flink.streaming.api.windowing.time.Time

object WindowTest {
  def main(args: Array[String]): Unit = {
    val env = StreamExecutionEnvironment.getExecutionEnvironment
    env.setParallelism(1)
    env.setStreamTimeCharacteristic(TimeCharacteristic.EventTime)
    env.getConfig.setAutoWatermarkInterval(50)
//    val inputpath ="src/main/resources/1.txt"
//    val inputStream = env.readTextFile(inputpath)

    val inputStream = env.socketTextStream("localhost",7777)

    val dataStream = inputStream
      .map(data =>{
        val arr = data.split(",")
        SensorReading(arr(0),arr(1).toLong,arr(2).toDouble)
      })
      .assignTimestampsAndWatermarks(new BoundedOutOfOrdernessTimestampExtractor[SensorReading](Time.seconds(3)) {
        override def extractTimestamp(t: SensorReading):Long=t.timestamp*1000L
      })
//      .assignAscendingTimestamps(_.timestamp*1000L)   //升序数据提取时间戳

    val latetag = new OutputTag[(String, Double, Long)]("late")
    //每15秒统计一次,窗口内各传感器所有温度的最小值,以及最新的时间戳
    val resultStream = dataStream
      .map(data =>(data.id,data.temperature,data.timestamp))
      .keyBy(0) //按照二元组的第一个元素(id)分组
      .timeWindow(Time.seconds(15))
      .allowedLateness(Time.minutes(1))
      .sideOutputLateData(latetag)
//      .countWindow(10)
      .reduce((curRes,newData)=>(curRes._1,curRes._2.min(newData._2),newData._3))

    resultStream.getSideOutput(latetag).print("late")
    resultStream.print("result")

    env.execute("window test")
  }
}
