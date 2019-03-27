import java.io.FileWriter

import org.apache.spark.mllib.recommendation.{ALS, Rating}
import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}

import scala.collection.mutable.ListBuffer

object karan_maheshwari_task2 {


  def case1(sc: SparkContext, trainfile: String, testfile: String, outputfile: String): Unit ={

    val data = sc.textFile(trainfile).map(x => x.split(",")).filter(x => !(x(0) contains "user"))
    val business = collection.mutable.Map[String, Int]()
    var business_id = 0
    val user = collection.mutable.Map[String, Int]()
    var user_id = 0

    for (b <- data.map(x => x(1)).collect()) {
      if (!(business contains b)) {
        business(b) = business_id
        business_id += 1
      }
    }

    for (u <- data.map(x => x(0)).collect()) {
      if (!(user contains u)) {
        user(u) = user_id
        user_id += 1
      }
    }

    val user_averages = data.map(x => (x(0), x(2).toFloat)).groupByKey().map(x => (x._1, x._2.sum/x._2.size)).collectAsMap()

    val inverted_business = business.map(_.swap)
    val inverted_user = user.map(_.swap)
    val training_data = data.map(x => Rating(user(x(0)), business(x(1)), x(2).toFloat))

    val rank = 10
    val numIterations = 10
    val model = ALS.train(training_data, rank, numIterations, 0.1)
    val test_data = sc.textFile(testfile, 8).map(x => x.split(","))
    val exists = test_data.filter(x => !(x(0) contains "user") && (business contains x(1)))
    val does_not_exist = test_data.filter(x => !(x(0) contains "user") && !(business contains x(1))).map(x => (x(0), x(1), user_averages(x(0)), x(2).toFloat)).collect()
    val preds = model.predict(exists.map(x => (user(x(0)), business(x(1))))).map(x =>  ((x.user, x.product), x.rating)).asInstanceOf[RDD[((Int, Int), Float)]]
    val temp = exists.map(x => ((user(x(0)), business(x(1))), x(2).toFloat))
    var scores = preds.join(temp).map(x => (inverted_user(x._1._1), inverted_business(x._1._2), x._2._1, x._2._2)).collect()
    scores = scores ++ does_not_exist

    val outputFile = new FileWriter(outputfile)
    outputFile.write("user_id" + "," + "business_id" + "," + "prediction" + "\n")
    val tt = scores.map(x => x._1 +","+ x._2+"," + x._3.toString+"\n")
    for(t <- tt) {
      outputFile.write(t)
    }
    outputFile.close()
  }

  def case2(sc: SparkContext, trainfile: String, testfile: String, outputfile: String): Unit = {

    val data = sc.textFile(trainfile).map(x => x.split(",")).filter(x => !(x(0) contains "user"))

    val grouped_by_business_id = data.map(x => (x(1), x(0))).groupByKey().map(x => (x._1, x._2.toList)).collectAsMap()

    val grouped_by_user_id = data.map(x => (x(0), (x(1), x(2).toFloat))).groupByKey().map(x => (x._1, x._2.toList)).collectAsMap()

    val data1 = sc.textFile(testfile, 2).map(x => x.split(",")).filter(x => !(x(0) contains "user"))

    def keep_only_commons(common_business: Set[String], dic: Map[String, Float], target_business: String) : (scala.collection.mutable.Map[String, Float], Double) = {
      val d = scala.collection.mutable.Map[String, Float]()
      //println(common_business)
      //println(dic)
      //println(target_business)
      for (item <- common_business) {
        d(item) = dic(item)
      }
      var target_business_rating = 0.0
      if (dic contains target_business) {
        target_business_rating = dic(target_business)
      }
      return (d, target_business_rating)
    }

    def getRatings_userBased(xx: Array[String]): (String, String, Double, String) = {
      val x = (xx(0), xx(1), xx(2))
      var users_who_rated_this_business = List[String]()
      try {
        users_who_rated_this_business = grouped_by_business_id(x._2)
      } catch {
        case _: Exception => {
          val test = grouped_by_user_id(x._1).toMap
          return (x._1, x._2, test.values.sum / test.values.size, x._3)
        }
      }

      users_who_rated_this_business = grouped_by_business_id(x._2)

      val active_user = x._1
      val target_business = x._2

      val users_who_rated_this_business_ = users_who_rated_this_business.map(x => (x, grouped_by_user_id(x)))

      var business_rated_by_active_user = List[(String, Float)]()
      try {
        business_rated_by_active_user = grouped_by_user_id(active_user)
      } catch {
        case _ : Exception => return (x._1, x._2, 0, x._3)
      }

      business_rated_by_active_user = grouped_by_user_id(active_user)

      val business_rated_by_active_user_ = business_rated_by_active_user.toMap

    val overall_average_rating_of_active_user = business_rated_by_active_user_.values.sum/business_rated_by_active_user_.values.size

    if (business_rated_by_active_user_.values.sum == 0 || business_rated_by_active_user_.values.isEmpty) {
      return (x._1, x._2, 0, x._3)
    }

    val business_rated_by_active_user_keys_set = business_rated_by_active_user_.keySet

    val sum_of_rating_for_target_business = ListBuffer[(Double, Double)]()
    var sum_of_weights = 0.0

    for (ttt <- users_who_rated_this_business_){

      val business_ratings_by_other_user_list = ttt._2

      var business_rated_by_other_user_dict = business_ratings_by_other_user_list.toMap

      val overall_average_rating_of_other_user = business_rated_by_other_user_dict.values.sum / business_rated_by_other_user_dict.values.size

      val common_business = business_rated_by_active_user_keys_set.intersect(business_rated_by_other_user_dict.keySet)

      if (common_business.nonEmpty) {

        var temp = keep_only_commons(common_business, business_rated_by_active_user_, target_business)
        var business_rated_by_active_user__  = temp._1.toMap
        temp = keep_only_commons(common_business, business_rated_by_other_user_dict, target_business)
        business_rated_by_other_user_dict  = temp._1.toMap
        val target_business_rating = temp._2

        val average_rating_of_active_user = business_rated_by_active_user__.values.sum / common_business.size
        val average_rating_of_other_user = business_rated_by_other_user_dict.values.sum / common_business.size

        var numerator = 0.0
        var distance_of_active_user = 0.0
        var distance_of_other_user = 0.0

        for (item <- common_business) {
          numerator += (business_rated_by_active_user__(item) - average_rating_of_active_user) * (business_rated_by_other_user_dict(item) - average_rating_of_other_user)
          distance_of_active_user += math.pow(business_rated_by_active_user__(item) - average_rating_of_active_user, 2)
          distance_of_other_user += math.pow(business_rated_by_other_user_dict(item) - average_rating_of_other_user, 2)
        }
        distance_of_active_user = math.pow(distance_of_active_user, 0.5)
        distance_of_other_user = math.pow(distance_of_other_user, 0.5)

        if (!(numerator == 0)) {
          val weight = numerator / (distance_of_active_user * distance_of_other_user)
          sum_of_rating_for_target_business.+:((weight, target_business_rating - overall_average_rating_of_other_user))
        }
      }
    }

    var sum_of_rating_for_target_business_ = 0.0
    for (t <- sum_of_rating_for_target_business.toList) {
        sum_of_weights += math.abs(t._1)
        sum_of_rating_for_target_business_ += t._2 * t._1
    }

    var final_rating = 0.0

    if (sum_of_weights == 0)
      return (x._1, x._2, overall_average_rating_of_active_user, x._3)
    else
      final_rating = overall_average_rating_of_active_user + sum_of_rating_for_target_business_ / sum_of_weights

    if (final_rating > 5)
      return (x._1, x._2, overall_average_rating_of_active_user, x._3)
    else
      return (x._1, x._2, final_rating, x._3)
    }

    val data1_ = data1.map(x => getRatings_userBased(x)).collect()

    val outputFile = new FileWriter(outputfile)
    outputFile.write("user_id" + "," + "business_id" + "," + "prediction" + "\n")
    var count = 0
    var mse = 0.0

    for (row <- data1_) {
      outputFile.write(row._1 + "," + row._2 + "," + row._3.toString + "\n")
      count += 1
      mse += math.pow(row._3.toFloat - row._4.toFloat, 2)
    }
  }

  def main(args: Array[String]): Unit = {
    val conf = new SparkConf()
    conf.setMaster("local")
    conf.setAppName("Task1")
    val sc = new SparkContext(conf)
    val trainfile = args(0)
    val testfile = args(1)
    val case_ = args(2).toInt
    val outputfile = args(3)
    if (case_ == 1)
      case1(sc, trainfile, testfile, outputfile)
    else if(case_ == 2)
      case2(sc, trainfile, testfile, outputfile)
  }

}
