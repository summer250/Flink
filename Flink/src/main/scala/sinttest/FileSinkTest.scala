package sinttest

import apitest.SensorReading
import org.apache.flink.api.common.serialization.SimpleStringEncoder
import org.apache.flink.core.fs.Path
import org.apache.flink.streaming.api.functions.sink.filesystem.StreamingFileSink
import org.apache.flink.streaming.api.scala._

object FileSinkTest {
  def main(args: Array[String]): Unit = {
    val env = StreamExecutionEnvironment.getExecutionEnvironment

    val inputpath ="src/main/resources/1.txt"
    val inputStream = env.readTextFile(inputpath)


    val dataStream = inputStream
      .map(data =>{
        val arr = data.split(",")
        SensorReading(arr(0),arr(1).toLong,arr(2).toDouble)
      })

    dataStream.print()
//    dataStream.writeAsCsv("/Volumes/数据/Flink/src/main/resources/2.txt")
    dataStream.addSink(
      StreamingFileSink.forRowFormat(new Path("/Volumes/数据/Flink/src/main/resources/2.txt"),
        new SimpleStringEncoder[SensorReading]()).build()
    )

    env.execute("file sink test")
  }
}
