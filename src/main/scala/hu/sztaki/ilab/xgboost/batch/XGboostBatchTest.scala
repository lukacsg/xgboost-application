package hu.sztaki.ilab.xgboost.batch

import ml.dmlc.xgboost4j.scala.flink. batch.{XGboost => BatchXGboost}
import org.apache.flink.api.common.typeinfo.TypeInformation
import org.apache.flink.api.scala.ExecutionEnvironment
import org.apache.flink.ml.MLUtils

/**
  * Created by lukacsg on 2017.08.15..
  */
object XGboostBatchTest {
  def main(args: Array[String]) {
    val env: ExecutionEnvironment = ExecutionEnvironment.getExecutionEnvironment
    env.setParallelism(4)
    // read trainining data
    val trainData =
      MLUtils.readLibSVM(env, "/home/lukacsg/git/xgboost/demo/data/agaricus.txt.train")
    val testData = MLUtils.readLibSVM(env, "/home/lukacsg/git/xgboost/demo/data/agaricus.txt.test")
    // define parameters
    val paramMap = List(
      "eta" -> 0.1,
      "max_depth" -> 2,
      "objective" -> "binary:logistic").toMap
    // number of iterations
    val round = 2
    val numberOfParalelizm = 8
    // train the model
    print("##################0")
//    val model = BatchXGboost.train(trainData, paramMap, round, numberOfParalelizm = 1)
    val model = BatchXGboost.train(trainData, paramMap, round, numberOfParalelizm)
    print("##################1")
    print(model)
    implicit val typeInfo = TypeInformation.of(classOf[org.apache.flink.ml.math.Vector])
    /////////// test
//    val predictMap: Iterator[Vector] => Traversable[Array[Float]] =
//      (it: Iterator[Vector]) => {
//        if (it.isEmpty) {
//          Some(Array.empty[Float])
//        } else {
//        val mapper = (x: Vector) => {
//          val (index, value) = x.toSeq.unzip
//          LabeledPoint.fromSparseVector(0.0f,
//            index.toArray, value.map(z => z.toFloat).toArray)
//        }
//        val dataIter = for (x <- it) yield mapper(x)
//        val dmat = new DMatrix(dataIter, null)
//        model.getBooster.predict(dmat)
//        }
//      }
//    implicit val typeInfo2 = TypeInformation.of(classOf[Array[Float]])
//    val predTest = testData.map{x => x.vector}
////      .setParallelism(1)
//      .mapPartition(predictMap)
    /////////// test
    val predTest = model.predict(testData.map{x => x.vector}, numberOfParalelizm)
    val a = predTest.collect().toArray
    print(a.deep.mkString("[", ",", "]"))
    print("##################2")
    model.saveModel("/data/lukacsg/sandbox/xgboosttestmodel/model3")
    print("##################")
  }
}
