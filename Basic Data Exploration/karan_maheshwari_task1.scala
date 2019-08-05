import org.apache.spark.{SparkConf, SparkContext}
import org.json4s
import org.json4s.jackson.JsonMethods._
import java.io._
import org.json4s.JsonAST.{JArray, JInt, JString}

object karan_maheshwari_task1 {

  class task1 (sc:SparkContext) {

    def convert(x: (Option[String], Int)): JArray = {
      return JArray(List(JString(x._1.get), JInt(x._2)))
    }

    def convert2(x: (String, Int)): JArray = {
      return JArray(List(JString(x._1), JInt(x._2)))
    }

    def run(inputfile: String): String = {

      val lines = sc.textFile(inputfile).map(x => parse(x).values.asInstanceOf[Map[String, String]])
      val one = lines.count()
      val two = lines.filter(x => x.get("date").toString().contains("2018")).count()
      val l3 = lines.map(x => (x.get("user_id"), 1)).persist()
      val three = l3.distinct().count()
      val four = l3.reduceByKey(_+_).sortBy(x=>(-x._2,x._1)).take(10)
      val l5 = lines.map(x => (x("business_id"), 1)).persist()
      val five = l5.distinct().count()
      val six = l5.reduceByKey(_+_).sortBy(x=>(-x._2,x._1)).take(10)
      val answers = json4s.JObject(("n_review",JInt(one)), ("n_review_2018", JInt(two)), ("n_user", JInt(three)), ("top10_user", JArray(four.map(x => convert(x)).toList)),  ("n_business", JInt(five)), ("top10_business", JArray(six.map(x => convert2(x)).toList)))
      return compact(render(answers))
    }

  }

  def main(args: Array[String]): Unit = {
    val conf = new SparkConf()
    conf.setMaster("local")
    conf.setAppName("Task1")
    val context = new SparkContext(conf)
    val inputfile = args(0)
    val job = new task1(context)
    val answers = job.run(inputfile)
    val pw = new PrintWriter(new File(args(1)))
    pw.write(answers.toString())
    pw.close
  }
}