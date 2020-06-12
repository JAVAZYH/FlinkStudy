package DataStreamAPI

import org.apache.flink.types.Row


object test {
  def main(args: Array[String]): Unit = {
//    StreamingExecution
//    println("2020-02-03 12:23:32".toLong)
    var row1=new Row(5)
    var row2=new Row(1)
    row1.setField(0,1)
    row1.setField(1,"a")
    row1.setField(2,"a")
    row1.setField(3,"a")
    row1.setField(4,"a")
    row2.setField(0,2)
//  val result: String = row1.toString+','+row2.toString
//    def stringToRow(str:String) ={
//      val arr: Array[String] = str.split(',')
//      val result=new Row(arr.length)
//      for(ele <- arr.indices){
//        result.setField(ele,arr(ele))
//      }
//      result
//    }
def resultRow(row:Row): Row ={
  val result=new Row(1)
  var str=""
  for(ele <- 0 until  row.getArity){
    if(ele==0){
      str += row.getField(ele)
    }
    else{
      str+="|"+row.getField(ele)
    }
  }
  result.setField(0,str)
  result
}

    val result = resultRow(row1)



    println(result)


  }

}
