package it.polimi.flink_tutorial.batch.wordcount

import org.apache.flink.api.java.utils.ParameterTool
import org.apache.flink.api.scala._

object WordCount {
  def main(args: Array[String]): Unit = {
    val params = ParameterTool.fromArgs(args)
    val filename = params.get("filename", "files/wordcount/text.txt")

    val env = ExecutionEnvironment.getExecutionEnvironment

    val source = env.readTextFile(filename)
    val counts = source.flatMap { _.toLowerCase.split("\\W+") }
      .map { (_, 1) }
      .groupBy(0)
      .sum(1)

    counts.print()
  }
}
