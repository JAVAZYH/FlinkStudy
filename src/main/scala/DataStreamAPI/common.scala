package DataStreamAPI

import java.text.SimpleDateFormat
import java.util.Date

import org.apache.flink.types.Row

object common {

  //    def transRowStreamToStringStream(rowStream: DataStream[Row]): DataStream[String] = {
  //      val stringStream = rowStream.map(rows => {
  //        var string = ""
  //        for (pos <- 0 until rows.getArity) {
  //          if (pos == 0)
  //            string += rows.getField(pos)
  //          else
  //            string += "|" + rows.getField(pos)
  //        }
  //        string
  //      })
  //      stringStream
  //    }
  //
  //    def transStringStreamToRowStream(StringStream: DataStream[String]): DataStream[Row] = {
  //      StringStream.map(str=>{
  //        val arr: Array[String] = str.split('|')
  //        val rowResult=new Row(arr.length)
  //        for (pos <- arr.indices){
  //          rowResult.setField(pos,arr(pos))
  //        }
  //        rowResult
  //      })
  //    }


//  val param: String = context.params.get("test")
//  println(param)
//  dataStreams(0).map(rows=>{
//    val resultRow=new Row(rows.getArity+1)
//    for (ele <- 0 until rows.getArity){
//      resultRow.setField(ele,rows.getField(ele))
//    }
//    val currentDate=new SimpleDateFormat("yyyy-MM-dd HH:mm:ss").format(new Date)
//    resultRow.setField(rows.getArity+1,currentDate)
//    resultRow
//  })
}
