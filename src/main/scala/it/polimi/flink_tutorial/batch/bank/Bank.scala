package it.polimi.flink_tutorial.batch.bank

import org.apache.flink.api.java.utils.ParameterTool
import org.apache.flink.api.scala._
import org.apache.flink.api.scala.extensions._

object Bank {
  def main(args: Array[String]): Unit = {
    val params = ParameterTool.fromArgs(args)
    val depositsFile = params.get("depositsFile", "files/bank/deposits.csv")
    val withdrawalsFile = params.get("withdrawalsFile", "files/bank/withdrawals.csv")

    val env = ExecutionEnvironment.getExecutionEnvironment

    val deposits = env.readCsvFile[(String, String, Int)](depositsFile)
    val withdrawals = env.readCsvFile[(String, String, Int)](withdrawalsFile)

    /**
     * Person with the maximum total amount of withdrawals
     */
    withdrawals
      .groupBy(0)
      .sum(2)
      .max(2)
      .mapWith {
        case (person, _, _) => person
      }
      .print()

    /**
     * Accounts with a negative balance
     */
    val totalWithdrawalsPerAccount = withdrawals
      .groupBy(1)
      .sum(2)
      .mapWith {
        case (_, account, amount) => (account, amount)
      }

    val totalDepositsPerAccount = deposits
      .groupBy(1)
      .sum(2)
      .mapWith {
        case (_, account, amount) => (account, amount)
      }

    totalWithdrawalsPerAccount
      .leftOuterJoin(totalDepositsPerAccount)
      .where(0)
      .equalTo(0) {
        (totWithdrawals, totDeposits) =>
          if (totDeposits == null) (totWithdrawals._1, -totWithdrawals._2)
          else (totWithdrawals._1, totDeposits._2-totWithdrawals._2)
      }
      .filter(_._2 < 0)
      .print();

  }
}
