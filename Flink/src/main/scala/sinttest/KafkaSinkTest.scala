package sinttest

import apitest.SensorReading
import org.apache.flink.api.common.serialization.SimpleStringSchema
import org.apache.flink.streaming.api.scala._
import org.apache.flink.streaming.connectors.kafka.{FlinkKafkaConsumer011, FlinkKafkaProducer011}

import java.util.Properties

object KafkaSinkTest {
  def main(args: Array[String]): Unit = {
    val env = StreamExecutionEnvironment.getExecutionEnvironment

    val inputpath ="src/main/resources/1.txt"
    val inputStream = env.readTextFile(inputpath)

    //从kafka读取数据
    val properties = new Properties()
    properties.setProperty("bootstrap.servers","localhost:9092")
    properties.setProperty("group.id","consumer-group")
    val steam = env.addSource(new FlinkKafkaConsumer011[String]("sensor",new SimpleStringSchema(),properties))


    val dataStream = steam
      .map(data =>{
        val arr = data.split(",")
        SensorReading(arr(0),arr(1).toLong,arr(2).toDouble).toString
      })
    dataStream.addSink(new FlinkKafkaProducer011[String]("localhost:9092","sinktest",new SimpleStringSchema()))

    env.execute("kafka sink test")
  }
}

