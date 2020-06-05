import org.apache.flink.streaming.api.scala.{DataStream, StreamExecutionEnvironment}

object TlogETL {
  def main(args: Array[String]): Unit = {
   val env: StreamExecutionEnvironment = StreamExecutionEnvironment.getExecutionEnvironment
    val input_ds: DataStream[String] = env.readTextFile("kda.txt")
//    val input_ds: DataStream[String] = env.readTextFile("F:\\FlinkStudy\\src\\main\\resources\\kda.txt")
    input_ds.print()
    env.execute()
  }


}
