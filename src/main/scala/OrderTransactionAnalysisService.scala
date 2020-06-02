//import org.apache.flink.api.common.state.{ValueState, ValueStateDescriptor}
//import org.apache.flink.configuration.Configuration
//import org.apache.flink.streaming.api.functions.co.CoProcessFunction
//import org.apache.flink.streaming.api.scala._
//import org.apache.flink.util.Collector
//
//class OrderTransactionAnalysisService {
//
//    private val orderTransactionAnalysisDao = new OrderTransactionAnalysisDao
//
//    def getOrderTransactionDatas(source1: String, source2: String) = {
//
//        // TODO 1. 获取订单日志数据
//        val fileDS1: DataStream[String] = orderTransactionAnalysisDao.readTextFile(source1)
//        // TODO 2. 获取交易日志数据
//        val fileDS2: DataStream[String] = orderTransactionAnalysisDao.readTextFile(source2)
//
//        // TODO 3. 将日志数据转换成对应的样例类数据
//        val orderDS = fileDS1.map(
//            line => {
//                val datas = line.split(",")
//                OrderLogData( datas(0).toLong, datas(1), datas(2), datas(3).toLong)
//            }
//        )
//
//        val orderKS = orderDS
//            .assignAscendingTimestamps(_.timestamp * 1000L).keyBy(_.txId)
//
//        val txDS = fileDS2.map(
//            line => {
//                val datas = line.split(",")
//                TXLogData( datas(0), datas(1), datas(2).toLong)
//            }
//        )
//
//        val txKS = txDS.assignAscendingTimestamps(_.timestamp * 1000L).keyBy(_.txId)
//
//        // TODO 4. 将两个流数据连接在一起
//        val connectCS: ConnectedStreams[OrderLogData, TXLogData] = orderKS.connect(txKS)
//
//        // TODO 5. 将连接后的数据进行处理
//        val noorderOutputTag = new OutputTag[String]("noorderOutputTag")
//        val notxOutputTag = new OutputTag[String]("notxOutputTag")
//
//        val resultDS: DataStream[String] = connectCS.process(
//            new CoProcessFunction[OrderLogData, TXLogData, String] {
//
//                private var orderData: ValueState[OrderLogData] = _
//                private var txData: ValueState[TXLogData] = _
//
//                private var orderTimer: ValueState[Long] = _
//                private var txTimer: ValueState[Long] = _
//
//                override def open(parameters: Configuration): Unit = {
//                    orderData = getRuntimeContext.getState(
//                        new ValueStateDescriptor[OrderLogData]("orderData", classOf[OrderLogData])
//                    )
//
//                    txData = getRuntimeContext.getState(
//                        new ValueStateDescriptor[TXLogData]("txData", classOf[TXLogData])
//                    )
//
//                    orderTimer = getRuntimeContext.getState(
//                        new ValueStateDescriptor[Long]("orderTimer", classOf[Long])
//                    )
//
//                    txTimer = getRuntimeContext.getState(
//                        new ValueStateDescriptor[Long]("txTimer", classOf[Long])
//                    )
//                }
//
//                override def onTimer(timestamp: Long, ctx: CoProcessFunction[OrderLogData, TXLogData, String]#OnTimerContext, out: Collector[String]): Unit = {
//                    if (orderData.value() != null) {
//                        // 交易无数据
//                        ctx.output(notxOutputTag, "交易[" + orderData.value.txId + "]无对账信息")
//                    }
//                    if (txData.value() != null) {
//                        // 支付无数据
//                        ctx.output(noorderOutputTag, "交易[" + txData.value.txId + "]交易无支付信息")
//                    }
//                    orderData.clear()
//                    txData.clear()
//                    orderTimer.clear()
//                    txTimer.clear()
//                }
//
//                override def processElement1(value: OrderLogData, ctx: CoProcessFunction[OrderLogData, TXLogData, String]#Context, out: Collector[String]): Unit = {
//                    val tx: TXLogData = txData.value()
//                    if (tx == null) {
//                        val timer = value.timestamp * 1000L + 3 * 60 * 1000L
//                        ctx.timerService().registerEventTimeTimer(timer)
//                        txTimer.update(timer)
//                    } else {
//                        // 订单数据存在，交易数据存在的场合
//                        out.collect(value.txId + "交易已经对账成功")
//                        ctx.timerService().deleteEventTimeTimer(txTimer.value())
//                        orderData.clear()
//                        txData.clear()
//                        orderTimer.clear()
//                        txTimer.clear()
//                    }
//
//                    orderData.update(value)
//                }
//
//                override def processElement2(value: TXLogData, ctx: CoProcessFunction[OrderLogData, TXLogData, String]#Context, out: Collector[String]): Unit = {
//                    val order: OrderLogData = orderData.value()
//                    if (order == null) {
//                        val timer = value.timestamp * 1000L + 3 * 60 * 1000L
//                        ctx.timerService().registerEventTimeTimer(timer)
//                        orderTimer.update(timer)
//                    } else {
//                        // 订单数据存在，交易数据存在的场合
//                        out.collect(value.txId + "交易已经对账成功")
//                        ctx.timerService().deleteEventTimeTimer(orderTimer.value())
//                        orderData.clear()
//                        txData.clear()
//                        orderTimer.clear()
//                        txTimer.clear()
//                    }
//
//                    txData.update(value)
//                }
//            }
//        )
//
//        resultDS.getSideOutput(notxOutputTag).print("notxt")
//        resultDS.getSideOutput(noorderOutputTag).print("noorder")
//
//        resultDS
//    }
//}
