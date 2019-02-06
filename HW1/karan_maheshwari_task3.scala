import org.apache.spark.{SparkConf, SparkContext}
import org.json4s
import org.json4s.jackson.JsonMethods._
import java.io._

import org.json4s.JsonAST.{JDouble, JObject, JString}

object karan_maheshwari_task3 {
  class task3 (sc:SparkContext) {

    def run(inputfile1: String, inputfile2: String): (JObject, Array[(String, Double)]) = {

      var review = sc.textFile(inputfile1).map(x => parse(x).values.asInstanceOf[Map[String, Any]]).map(x => (x("business_id"), x("stars").asInstanceOf[Double]))
      var business = sc.textFile(inputfile2).map(x => parse(x).values.asInstanceOf[Map[String, Any]]).map(x => (x("business_id"), x("city").asInstanceOf[String]))
      var joined = business.join(review).map(x => x._2).aggregateByKey((0.0, 0))((a, b) => (a._1 + b, a._2 + 1), (a, b) => (a._1+b._1, a._2+b._2))

      var finalRDD = joined.map(x => (x._1, x._2._1/x._2._2)).sortBy(x=>(-x._2,x._1))

      val start1 = System.currentTimeMillis()
      val answer = finalRDD.collect().take(10)
      val end1 = System.currentTimeMillis()

      review = sc.textFile(inputfile1).map(x => parse(x).values.asInstanceOf[Map[String, Any]]).map(x => (x("business_id"), x("stars").asInstanceOf[Double]))
      business = sc.textFile(inputfile2).map(x => parse(x).values.asInstanceOf[Map[String, Any]]).map(x => (x("business_id"), x("city").asInstanceOf[String]))
      joined = business.join(review).map(x => x._2).aggregateByKey((0.0, 0))((a, b) => (a._1 + b, a._2 + 1), (a, b) => (a._1+b._1, a._2+b._2))

      finalRDD = joined.map(x => (x._1, x._2._1/x._2._2)).sortBy(x=>(-x._2,x._1))

      val start2 = System.currentTimeMillis()
      val method2 = finalRDD.take(10)
      val end2 = System.currentTimeMillis()

      val answers = json4s.JObject(("m1", JDouble(end1-start1)), ("m2", JDouble(end2-start2)), ("explanation", JString("Since .collect() collects the data from all the partitions into one place and then selects 10 items, it takes more time. When we perform .take(), we are putting a cap to the number of items we collect. Hence, it takes lesser time.")))

      return (answers, finalRDD.collect())
    }

  }

  def main(args: Array[String]): Unit = {
    val conf = new SparkConf()
    conf.setMaster("local")
    conf.setAppName("Task1")
    val context = new SparkContext(conf)
    val inputfile1 = args(0)
    val inputfile2 = args(1)
    val outputfile1 = args(2)
    val outputfile2 = args(3)
    val job = new task3(context)
    val answers = job.run(inputfile1, inputfile2)
    var pw = new PrintWriter(new File(outputfile2))
    pw.write(compact(render(answers._1)).toString)
    pw.close
    pw = new PrintWriter(new File(outputfile1))
    pw.write("city,stars\n")
    for (name <- answers._2) {
      pw.write(name._1+","+name._2.toString+"\n")
    }
    pw.close
  }
}
