package TableAPI

import org.apache.flink.api.common.typeinfo.{TypeInformation, Types}
import org.apache.flink.streaming.api.scala.{DataStream, StreamExecutionEnvironment}
import org.apache.flink.table.api.{Table, TableEnvironment, Types}
import org.apache.flink.table.api.scala.StreamTableEnvironment
import org.apache.flink.table.descriptors.FileSystem
import org.apache.flink.types.Row

object TableAPILeftJoin {
  def main(args: Array[String]): Unit = {
    val env: StreamExecutionEnvironment = StreamExecutionEnvironment.getExecutionEnvironment
    val tableEnv: StreamTableEnvironment = TableEnvironment.getTableEnvironment(env)
    val kdaStream: DataStream[String] = env.readTextFile("kdatest.txt")

    val kda_table: Table = tableEnv.fromDataStream(kdaStream)

//    val path="kdatest.txt"
//    tableEnv.registerDataStream("kda_table",kdaStream)
//    val kda_table: Table = tableEnv.scan("x")
//    tableEnv.registerTable("kda_table",kda_table)

    val resultTable: Table = kda_table
      .select("*")
    resultTable.printSchema()

    import org.apache.flink.table.api.scala._
    implicitly var  colType: TypeInformation[_] = Types.STRING
    resultTable.toAppendStream





//     new CsvTableSource()
    env.execute("table>>>>>")



  }

}
