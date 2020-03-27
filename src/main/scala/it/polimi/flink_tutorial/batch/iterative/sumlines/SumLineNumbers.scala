package it.polimi.flink_tutorial.batch.iterative.sumlines

import org.apache.flink.api.java.utils.ParameterTool
import org.apache.flink.api.scala._
import org.apache.flink.api.scala.extensions._
import org.apache.flink.api.java.operators.DeltaIteration

object SumLineNumbers {
  def main(args: Array[String]): Unit = {
    val params = ParameterTool.fromArgs(args)
    val filename = params.get("filename", "files/sumlines/text.txt")
    val maxIterations = params.getInt("maxIterations", 1000)

    val env = ExecutionEnvironment.getExecutionEnvironment

    // Line, index of the next word to sum, current result
    val lines = env.readTextFile(filename)
      .map(line => (line.split("\\W+"), 0, 0))

    val result = lines.iterateWithTermination(maxIterations) { iterationInput =>
      val iterationResult = iterationInput.
        mapWith {
          case (words, index, count) if index < words.length => (words, index+1, count+words(index).length)
          case (words, index, count) => (words, index, count)
        }

      val terminationCondition = iterationResult.
        filter(t => t._2 < t._1.length)

      (iterationResult, terminationCondition)
    }

    result.
      mapWith {
        case (words, _, count) => (words.mkString(" "), count)
      }
      .print()
  }

}
