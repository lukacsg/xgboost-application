package hu.sztaki.ilab.xgboost.sequential

import ml.dmlc.xgboost4j.scala.{DMatrix, XGBoost}

object XGBoostTest {

  def main(args: Array[String]) {
    // read trainining data, available at xgboost/demo/data
    val trainData =
      new DMatrix("/home/lukacsg/git/xgboost/demo/data/agaricus.txt.train")
//      new DMatrix("/home/client/sandbox/agaricus.txt.train")
    val testMax = new DMatrix("/home/lukacsg/git/xgboost/demo/data/agaricus.txt.test")
//    val testMax = new DMatrix("/home/client/sandbox/agaricus.txt.test")
    // define parameters
    val paramMap = List(
      "eta" -> 0.1,
      "max_depth" -> 2,
      "objective" -> "binary:logistic").toMap
    // number of iterations
    val round = 2
    // train the model
    val model = XGBoost.train(trainData, paramMap, round)
    // run prediction
    val predTrain = model.predict(testMax)
    println(predTrain.deep.mkString("[", ",", "]"))
    println(predTrain.size)
    // save model to the file.
    model.saveModel("/data/lukacsg/sandbox/xgboosttestmodel/model")
//    model.saveModel("/home/client/sandbox/model")
  }


}
