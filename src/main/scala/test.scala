import java.text.SimpleDateFormat
import java.util
import java.util.Date

import org.apache.flink.api.common.state.{ListState, ListStateDescriptor, ValueState, ValueStateDescriptor}
import org.apache.flink.api.java.utils.ParameterTool
import org.apache.flink.configuration.Configuration
import org.apache.flink.streaming.api.datastream.DataStream
import org.apache.flink.streaming.api.functions.co.CoProcessFunction
import org.apache.flink.streaming.api.scala
import org.apache.flink.streaming.api.scala.{KeyedStream, OutputTag, StreamExecutionEnvironment}
import org.apache.flink.util.Collector

object test {
  def main(args: Array[String]): Unit = {

    // 从外部命令中获取参数
//    val params: ParameterTool =  ParameterTool.fromArgs(args)
    val host: String = "9.134.217.5"
    val port: Int = 9999
    val port2=8888



    //第一个流和第二个流的位置偏移
    val  mainKeyPos1=9
    val  mainKeyPos2=4
    val timePos1=5-1
    val timePos2=10-1
    val  timeValid=3 * 1000L

    // 创建流处理环境
    val env = StreamExecutionEnvironment.getExecutionEnvironment

    // 接收socket文本流
    /**
      * input_Dstream1 :tm=1590992503467&t=2020060114&__addcol1__worldid=11&id=100410188&m=10&agentip=10.140.140.35|746,138141503126,1302717737,2020-06-01 14:21:42,2020-06-01 14:00:21,11,,1281,,,0,1883277289,1,29,0,692,0,7509,1808,0,196659,43280,18576,2960,3,1,33727,23053,149,40,161371,19173,153,7,4,6, 126, 75,0.5, false, 1, 18, 130, 1460, 0, 0, 1, 4050,900,0,0,0,0,1590990864948,1590990877053,1590990995528|null
      * input_Dstream2 :124293513,12,1996352160,12,12,10,0,200,2319625883
    **/
//    val input_Dstream1 = env.socketTextStream(host, port)
//    val input_Dstream2 = env.socketTextStream(host, port2)
    val input_Dstream1 = env.readTextFile("F:\\FlinkStudy\\src\\main\\resources\\tlog.txt")
    val input_Dstream2 = env.readTextFile("F:\\FlinkStudy\\src\\main\\resources\\kda.txt")


    import org.apache.flink.api.scala._
//    val dataStream: DataStream[(String, Int)] = textDstream.flatMap(_.split("\\s")).filter(_.nonEmpty).map((_, 1)).keyBy(0).sum(1)

    //对局结算数据流
    val keyed_DStream1: KeyedStream[String, String] = input_Dstream1
      .map(line => {
        val data_src = line.split('|')
        val head = data_src(0)
        val body = data_src(1)
        var body_list = body.split(',')
        body_list = body_list.map(_.trim)
        val wordid = head.split("worldid=")(1).split("&")(0)
        val data_list = wordid +: body_list
        val data = data_list.mkString("|")
        data
      })
      .keyBy(str => str.split('|')(mainKeyPos1 - 1))

    //kda结合谩骂数据流,对每条数据的末尾都加上时间字段
    val keyed_DStream2: KeyedStream[String, String] = input_Dstream2.filter(str => str.split('|')(0) != "null")
      .map(_+","+new SimpleDateFormat("yyyy-MM-dd HH:mm:ss").format(new Date))
      .keyBy(str => str.split('|')(mainKeyPos2 - 1))

//    //创建两个侧输出流用来存储没有关联到数据的情况
//    val noin1OutputTag = new OutputTag[String]("noin1OutputTag")
//    val noin2OutputTag = new OutputTag[String]("noin2OutputTag")


 //连接两条流
    val resultDS: scala.DataStream[String] = keyed_DStream1.connect(keyed_DStream2).process(new CoProcessFunction[String, String, String] {

      private var inputData1: ValueState[String] = _
      private var inputData2: ValueState[String] = _
      private var ListData: ListState[String] = _
      private var in1Timer: ValueState[Long] = _
      private var in2Timer: ValueState[Long] = _

      //输入时间字符串，转换为毫秒时间戳
      def parseTime(timeStr: String) = {
        val format = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss")
        val timeStamp: Long = format.parse(timeStr).getTime
        timeStamp
      }

      override def open(parameters: Configuration): Unit = {
        inputData1 = getRuntimeContext.getState(
          new ValueStateDescriptor[String]("in1", classOf[String]))
        inputData2 = getRuntimeContext.getState(
          new ValueStateDescriptor[String]("in2", classOf[String]))
        ListData = getRuntimeContext.getListState(
          new ListStateDescriptor[String]("ListData", classOf[String])
        )
        in1Timer = getRuntimeContext.getState(
          new ValueStateDescriptor[Long]("in1Timer", classOf[Long])
        )
        in2Timer = getRuntimeContext.getState(
          new ValueStateDescriptor[Long]("in1Timer", classOf[Long])
        )

      }


      override def processElement1(in1: String, ctx: CoProcessFunction[String, String, String]#Context, out: Collector[String]): Unit = {
        val in2: String = inputData2.value()
        //如果第二个流的数据为空,注册定时器等待第二个元素
        if (in2 == null) {
          val timer = parseTime(in1.split(',')(timePos1) + timeValid)
          ctx.timerService().registerEventTimeTimer(timer)
          ListData.add(in1)
          //把定时器的值保存下来
          in1Timer.update(timer)

        } else {
          //如果第二条流的数据到达
          //将两条数据成功组合
          out.collect(in1 + in2)
          ctx.timerService().deleteEventTimeTimer(in1Timer.value())
          inputData1.clear()
          inputData2.clear()
          in1Timer.clear()
          in2Timer.clear()
        }
        //更新第一条流的数据
        inputData1.update(in1)

      }

      override def processElement2(in2: String, ctx: CoProcessFunction[String, String, String]#Context, out: Collector[String]): Unit = {
        val in1: String = inputData1.value()
        //如果第一个流的数据为空
        if (in1 == null) {
          val timer = parseTime(in2.split(',')(timePos2) + timeValid)
          //把第二条流的注册到定时器
          ctx.timerService().registerEventTimeTimer(timer)
          in2Timer.update(timer)
        } else {
          //与第一个流的数据进行关联，然后清空所有状态,并且把定时器删除
          out.collect(in1 + in2)
          ctx.timerService().deleteEventTimeTimer(in2Timer.value())
          inputData1.clear()
          inputData2.clear()
          in1Timer.clear()
          in2Timer.clear()
        }
        //更新第二条流的数据
        inputData2.update(in2)

      }


      override def onTimer(timestamp: Long, ctx: CoProcessFunction[String, String, String]#OnTimerContext, out: Collector[String]): Unit = {

        //如果第一条流的状态不为空，表示第二条流的数据还没到
        if (inputData1.value() != null) {
          val it: util.Iterator[String] = ListData.get().iterator()
          while (it.hasNext) {
            out.collect(it.next())
            //            ctx.output(noin1OutputTag,it.next())
          }
          ListData.clear()
          //          ctx.output(noin2OutputTag,inputData1.value())
        }

        //如果第二条流的状态不为空，表示第一条流的数据还没到
        if (inputData2.value() != null) {
          out.collect(inputData2.value())
          inputData2.clear()
          //          ctx.output(noin1OutputTag,inputData2.value())
        }

        //        //如果第二条流的状态不为空，表示第一条流的数据还没到
        //      if(inputData2.value()!=null){
        //        ctx.output(noin1OutputTag,inputData2.value())
        //      }
        //        //如果第一条流的状态不为空，表示第二条流的数据还没到
        //       if (inputData1.value()!=null){
        //         ctx.output(noin2OutputTag,inputData1.value())
        //       }
        //        inputData1.clear()
        //        inputData2.clear()
        in1Timer.clear()
        in2Timer.clear()
      }

    })
resultDS.print("resultDS")





//    textDstream.print().setParallelism(1)

    // 启动executor，执行任务
    env.execute()


  }

}
