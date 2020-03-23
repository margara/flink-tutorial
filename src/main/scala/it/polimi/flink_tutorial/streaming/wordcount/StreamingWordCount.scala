package it.polimi.flink_tutorial.streaming.wordcount

import org.apache.flink.api.java.utils.ParameterTool
import org.apache.flink.streaming.api.scala._
import org.apache.flink.streaming.api.windowing.time.Time
import java.util.concurrent.TimeUnit.SECONDS

object StreamingWordCount {
  def main(args: Array[String]): Unit = {
    val params = ParameterTool.fromArgs(args)
    val serverAddr = params.get("serverAddr", "localhost")
    val serverPort = params.getInt("serverPort", 2345)
    val windowSize = params.getInt("windowSize", 4)

    val env = StreamExecutionEnvironment.getExecutionEnvironment

    val source = env.socketTextStream(serverAddr, serverPort)

    val count = source.flatMap { _.toLowerCase.split("\\W+") }
      .map { (_, 1) }
      .keyBy(0)
      .timeWindow(Time.of(10, SECONDS), Time.of(5, SECONDS))
      .sum(1)

    count.print()
    env.execute()
  }
}
