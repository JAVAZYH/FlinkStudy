package DataStreamAPI

import org.apache.flink.streaming.api.scala.StreamExecutionEnvironment

object tlog_ETL {
  def main(args: Array[String]): Unit = {
    val env: StreamExecutionEnvironment = StreamExecutionEnvironment.getExecutionEnvironment
    val input_Dstream1 = env.readTextFile("F:\\FlinkStudy\\src\\main\\resources\\tlogtest.txt")

//    input_Dstream1
//      .map(line => {
//        val data_src = line.split('|')
//        val head = data_src(0)
//        val body = data_src(1)
//        var body_list = body.split(',')
//        body_list = body_list.map(_.trim)
//        val wordid = head.split("worldid=")(1).split("&")(0)
//        val data_list = wordid +: body_list
//        val data = data_list.mkString("|")
//        data
//      })
  }

}
