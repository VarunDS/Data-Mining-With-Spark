import org.apache.spark.{Partitioner, SparkConf, SparkContext, rdd}
import org.json4s._
import org.json4s.jackson.JsonMethods._
import java.io._

import org.apache.spark.HashPartitioner
import org.json4s
import org.json4s.JsonAST.JInt

class PartitionByBusinessID(override val numPartitions: Int) extends Partitioner {

  def getPartition(key: Any): Int = {
      val keyString = key.toString()
      var totalSum = 0
      var mulBy = 1
      var keyStringLength = keyString.length-1
      while (keyStringLength >= 0) {
        totalSum += keyString.charAt(keyStringLength) * mulBy
        mulBy *= 31
        keyStringLength -= 1
      }
    totalSum
  }
}

object karan_maheshwari_task2 {

  class task2 (sc: SparkContext) {

    def run(inputfile: String, n: Int): String = {

      val lines = sc.textFile(inputfile).map(x => parse(x).values.asInstanceOf[Map[String, String]])
      val normal = lines.map(x => (x("business_id"), 1)).persist()
      normal.take(1)

      var start = System.currentTimeMillis()
      normal.reduceByKey(_+_).sortBy(x=>(-x._2,x._1)).take(10)
      var end = System.currentTimeMillis()

      var l = normal.glom().map(_.length).map(x => JInt(x)).collect()

      val default = json4s.JObject(("n_partition", JInt(normal.getNumPartitions)), ("n_items", JArray(l.toList)), ("exe_time", JDouble((end-start)/100)))

      val custom = normal.partitionBy(new HashPartitioner(n)).persist()
      custom.take(1)

      start = System.currentTimeMillis()
      custom.reduceByKey(_+_).sortBy(x=>(-x._2,x._1)).take(10)
      end = System.currentTimeMillis()

      l = custom.glom().map(_.length).map(x => JInt(x)).collect()

      val customized = json4s.JObject(("n_partition", JInt(custom.getNumPartitions)), ("n_items", JArray(l.toList)), ("exe_time", JDouble((end-start)/100)))

      val answers = json4s.JObject(("default", default), ("customized", customized), ("explanation", JString("Since we partition the RDD using 'business_id', we are reducing the amount of shuffling that would be required to perform the query with default partitioning. This reduction in shuffling time in turn minimizes the execution time.")))

      return compact(render(answers))
    }

  }

  def main(args: Array[String]): Unit = {
    val conf = new SparkConf()
    conf.setMaster("local")
    conf.setAppName("Task1")
    val context = new SparkContext(conf)
    val inputfile = args(0)
    val n = args(2)
    val job = new task2(context)
    val answers = job.run(inputfile, n.toInt)
    val pw = new PrintWriter(new File(args(1)))
    pw.write(answers.toString())
    pw.close
  }
}