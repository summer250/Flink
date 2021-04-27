package apitest

import org.apache.flink.streaming.api.functions.source.SourceFunction
import org.apache.flink.streaming.api.scala._

import scala.util.Random

case class SensorReading(id: String,timestamp: Long,temperature: Double)

object SourceTest {
  def main(args: Array[String]): Unit = {
    //创建执行环境
    val env = StreamExecutionEnvironment.getExecutionEnvironment

    //1.从集合中读取数据
//    val dataList =List(
//      SensorReading("sensor_1",1547718199,35.8),
//      SensorReading("sensor_6",1547718201,15.4),
//      SensorReading("sensor_7",1547718202,6.7),
//      SensorReading("sensor_10",1547718205,38.1),
//    )
//    val stream1 = env.fromCollection(dataList)
//    stream1.print()

    //2.从文件中读取数据
//    val inputpath = "src/main/resources/1.txt"
//    val stream2 = env.readTextFile(inputpath)
//    stream2.print()

    //3.从kafka中读取数据
//    val properties = new Properties()
//    properties.setProperty("bootstrap.servers","localhost:9092")
//    properties.setProperty("group.id","consumer-group")
//    val steam3 = env.addSource(new FlinkKafkaConsumer011[String]("sensor",new SimpleStringSchema(),properties))
//
//    steam3.print()

    //4.自定义source
    val stream4 = env.addSource(new MySensorSource())
    stream4.print()

    //执行
    env.execute("source test")
  }
}

//自定义SourceFunction
class MySensorSource()extends SourceFunction[SensorReading]{
  //定义一个标识位flag,用来表示数据源是否正常运行发出数据
  var running:Boolean= true

  override def run(ctx:SourceFunction.SourceContext[SensorReading]): Unit = {
    //定义一个随机数生成器
    val rand = new Random()

    //随机生成一组(10个)传感器的初始温度(id,temp)
    var curTemp =1.to(10).map(i=>("sensor_"+i,rand.nextDouble()*100))

    //定义无限循环,不停地产生数据,除非被cancel
    while(running){
      //在上次数据基础上微调,更新温度值
      curTemp = curTemp.map(
        data =>(data._1,data._2+rand.nextGaussian())
      )
      //获取当前时间戳,加入到数据中,调用ctx.collect发出数据
      val curTime = System.currentTimeMillis()
      curTemp.foreach(
        data=> ctx.collect(SensorReading(data._1,curTime,data._2))
      )
      // 间隔500ms
      Thread.sleep(100)
    }
  }

  override def cancel(): Unit = running = false

}