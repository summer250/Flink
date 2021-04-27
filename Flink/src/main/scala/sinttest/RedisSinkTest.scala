package sinttest

import apitest.SensorReading
import org.apache.flink.streaming.api.scala._
import org.apache.flink.streaming.connectors.redis.RedisSink
import org.apache.flink.streaming.connectors.redis.common.config.FlinkJedisPoolConfig
import org.apache.flink.streaming.connectors.redis.common.mapper.{RedisCommand, RedisCommandDescription, RedisMapper}

object RedisSinkTest {
  def main(args: Array[String]): Unit = {
    val env = StreamExecutionEnvironment.getExecutionEnvironment

    val inputpath ="src/main/resources/1.txt"
    val inputStream = env.readTextFile(inputpath)

    val dataStream = inputStream
      .map(data =>{
        val arr = data.split(",")
        SensorReading(arr(0),arr(1).toLong,arr(2).toDouble)
      })

    //定义一个FlinkJedisConfigBase
    val conf = new FlinkJedisPoolConfig.Builder()
      .setHost("localhost")
      .setPort(6379)
      .build()

    dataStream.addSink(new RedisSink[SensorReading](conf,new MyRedisMapper))

    env.execute("redis sink test")
  }
}
//定义一个RedisMapper
class MyRedisMapper extends RedisMapper[SensorReading]{
  //定义保存数据写入redis的命令,HEST表名key value
  override def getCommandDescription: RedisCommandDescription = {
    new RedisCommandDescription(RedisCommand.HSET,"sensor_temp")
  }
  //将id指定为key
  override def getKeyFromData(t: SensorReading): String = t.id

  //将温度值指定为value
  override def getValueFromData(t: SensorReading): String = t.temperature.toString
}