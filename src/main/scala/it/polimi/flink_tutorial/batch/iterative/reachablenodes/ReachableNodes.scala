package it.polimi.flink_tutorial.batch.iterative.reachablenodes

import org.apache.flink.api.java.utils.ParameterTool
import org.apache.flink.api.scala._
import org.apache.flink.api.scala.extensions._
import org.apache.flink.api.java.operators.DeltaIteration

object ReachableNodes {
  def main(args: Array[String]): Unit = {
    val params = ParameterTool.fromArgs(args)
    val filename = params.get("filename", "files/reachablenodes/nodes.txt")
    val initialNode = params.get("initialNode", "1")
    val maxIterations = params.getInt("maxIterations", 1000)

    val env = ExecutionEnvironment.getExecutionEnvironment

    // Read links, represented as a pair (source, destination)
    val links = env.readTextFile(filename)
        .map(line => (line.split("\\W+")(0), line.split("\\W+")(1)))

    // The initial solution set contains only the initial node
    val initialSolutionSet = env.fromElements(Tuple1(initialNode))

    // The initial workset contains only the initial node
    val initialDeltaSet = initialSolutionSet

    val keyPosition = 0
    val result = initialSolutionSet
      .iterateDelta(initialDeltaSet, maxIterations, Array(keyPosition)) {
        (solution, workset) =>
          // Compute reachable nodes
          val reachableNodes = workset
            .join(links)
            .where(0)
            .equalTo(0) {
              (left, right) => Tuple1(right._2)
            }

          // Compute the new workset (only new reachable nodes)
          val newReachableNodes = reachableNodes
            .join(solution)
            .where(0)
            .equalTo(0)
            .filter(t => t._2 == null)
            .map(t => t._1)

          // Return the new solutions and the workset for the next iteration
          (newReachableNodes, newReachableNodes)
      }

    result.print()
  }

}
