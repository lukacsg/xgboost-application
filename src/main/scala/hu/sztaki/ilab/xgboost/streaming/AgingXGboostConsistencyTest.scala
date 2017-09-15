package hu.sztaki.ilab.xgboost.streaming

import ml.dmlc.xgboost4j.scala.flink.stream.XGboost
import ml.dmlc.xgboost4j.java.IRabitTracker
import ml.dmlc.xgboost4j.scala.flink.XGBoostModel
import ml.dmlc.xgboost4j.scala.flink.utils.MLStreamUtils
import org.apache.commons.logging.LogFactory
import org.apache.flink.api.common.functions.RichFlatMapFunction
import org.apache.flink.api.java.utils.ParameterTool
import org.apache.flink.ml.common.LabeledVector
import org.apache.flink.ml.math.SparseVector
import org.apache.flink.streaming.api.functions.sink.RichSinkFunction
import org.apache.flink.streaming.api.scala._
import org.apache.flink.util.Collector

object AgingXGboostConsistencyTest {
  val logger = LogFactory.getLog(this.getClass)


  def readTestLibSVM(env: StreamExecutionEnvironment, filePath: String, dimension: Int, numberOfParallelism: Int,
                     skipBrokenLine: Boolean = false): DataStream[(LabeledVector, String)] = {
    env.readTextFile(filePath).setParallelism(numberOfParallelism)
      .flatMap {
        new RichFlatMapFunction[String, (Double, Array[(Int, Double)], String)] {


          override def flatMap(line: String, out: Collector[(Double, Array[(Int, Double)], String)]): Unit = {
            // remove all comments which start with a '#'
            val commentFreeLine = line.takeWhile(_ != '#').trim

            if (commentFreeLine.nonEmpty) {

              try {
                val splits = commentFreeLine.split(' ')
                val id = splits.head
                val sparseFeatures = splits.tail
                require(!sparseFeatures.isEmpty, "The line does not contains feature value.")
                val coos = sparseFeatures.map {
                  str =>
                    val pair = str.split(':')
                    require(pair.length == 2, "Each feature entry has to have the form <feature>:<value>")

                    // libSVM index is 1-based, but we expect it to be 0-based
                    val index = pair(0).toInt - 1
                    val value = pair(1).toDouble

                    (index, value)
                }

                out.collect((0, coos, id))

              } catch {
                case e: Exception =>
                  if (skipBrokenLine) {
                    logger.debug("the following line was skiped which breaks the format: " + line)
                  } else {
                    throw e
                  }
              }
            }
          }

        }
      }.setParallelism(numberOfParallelism)
      .map(value => (new LabeledVector(value._1, SparseVector.fromCOO(dimension, value._2)), value._3))
      .setParallelism(numberOfParallelism)
  }


  object Action extends Enumeration {
    val Train = Value(0)
    val TrainAndSave = Value(1)
    val Predict = Value(2)
    val TrainAndPredict = Value(3)
    val TrainAndSaveClose = Value(4)
  }


  def main(args: Array[String]): Unit = {

    def parameterCheck(args: Array[String]): Option[String] = {
      def outputNoParamMessage(): Unit = {
        val noParamMsg = "\tUsage:\n\n\t./run <path to parameters file>"
        println(noParamMsg)
      }

      if (args.length == 0 || !(new java.io.File(args(0)).exists)) {
        outputNoParamMessage()
        None
      } else {
        Some(args(0))
      }
    }

    def setTrainingParameter(params: ParameterTool) = {
      // parameters
      val max_depth = params.getRequired("max_depth").toInt
      val eta = params.getRequired("eta").toDouble
      val objective = params.getRequired("objective")
      logger.info(s"###PS###p;max_depth;$max_depth")
      logger.info(s"###PS###p;eta;$eta")
      logger.info(s"###PS###p;objective;$objective")
      List(
      "eta" -> eta,
      "max_depth" -> max_depth,
      "objective" -> objective).toMap
    }

    def setParameter(params: ParameterTool)(parameterName: String) = {
      val value = params.getRequired(parameterName)
      logger.info(s"###PS###p;$parameterName;$value")
      value
    }

    parameterCheck(args).foreach(propsPath => {
      val params = ParameterTool.fromPropertiesFile(propsPath)

      val getOptParam = setParameter(params) _
      def setTraining =  setTrainingParameter(params)

      //
      // parallelism
      val readParallelism = getOptParam("readParallelism").toInt
      val numberOfParallelism = getOptParam("numberOfParallelism").toInt
      val skipBrokenLine = getOptParam("skipBrokenLine").toBoolean
      // 0 - train
      // 1 - train and save model
      // 2 - predict
      // 3 - train and predict
      // 4 - train and save model + close tracker
      val action = getOptParam("action").toInt
      require(0 to 4 contains  action, "The action parameter must be contained by the interval [0, 4].")
      val act = Action(action)
      logger.info(s"###PS###p;action;$act")
      val dimension = getOptParam("dimension").toInt

      val env = StreamExecutionEnvironment.getExecutionEnvironment

      logger.info("Default parallelism: " + env.getParallelism)

      /// ????
      var tracker: Option[IRabitTracker] = None

      if (act == Action.Predict) {
        val modelPath = getOptParam("modelPath")
        val testPath = getOptParam("testPath")
        val tresholdparam = getOptParam("treshold").toDouble
        XGboost.predictWithId(
          XGBoostModel.loadModelFromFile(modelPath),
          readTestLibSVM(env, testPath, dimension, readParallelism, skipBrokenLine),
          numberOfParallelism)
          .addSink(
            new RichSinkFunction[Array[(Array[Float], String)]] {
              import scala.collection.mutable._
              val wholeModel: ArrayBuffer[(Array[Float], String)] = ArrayBuffer.empty[(Array[Float], String)]
              val treshold = tresholdparam

              override def invoke(value: Array[(Array[Float], String)]) {
                wholeModel ++= value
              }

              override def close() {
                super.close()
                logger.info("result size: " + wholeModel.size)

                val hit = wholeModel.map(_._1.head).count(_ >= treshold)
//                logger.info("result : " + wholeModel)
                logger.info("result 1: " + hit)
                logger.info("result 0: " + (wholeModel.size - hit))

                val hitIds = wholeModel.filter(_._1.head >= treshold).map(_._2).toArray
                val sortHitIds = hitIds.sorted
                val valueSortedOrder = wholeModel.filter(_._1.head >= treshold).sortWith((x, y) => x._1.head > y._1.head)
                  .map(x => (x._2, x._1.head))

                logger.info("result order: " + sortHitIds.mkString("[", ",", "]"))
                logger.info("result value order: " + valueSortedOrder.mkString("[", ",", "]"))
              }
            }
          ).setParallelism(1)
      }
      else {
        val round = getOptParam("round").toInt
        val paramMap = setTraining
        val trainPath = getOptParam("trainPath")

        act match {
          case Action.Train =>
            XGboost.train(
              MLStreamUtils.readLibSVM(env, trainPath, dimension, readParallelism, skipBrokenLine),
              paramMap, round, numberOfParallelism)
              .addSink(x => {
                logger.info("result: " + x)
              }).setParallelism(1)

          case Action.TrainAndSave =>
            val modelPath = getOptParam("modelPath")

            XGboost.trainAndSaveModelToFile(
              MLStreamUtils.readLibSVM(env, trainPath, dimension, readParallelism, skipBrokenLine),
              paramMap, round, numberOfParallelism, modelPath)
          case Action.TrainAndPredict =>
            val testPath = getOptParam("testPath")

            XGboost.trainAndPredict(MLStreamUtils.readLibSVM(env, trainPath, dimension, readParallelism, skipBrokenLine),
              MLStreamUtils.readLibSVM(env, testPath, dimension, readParallelism, skipBrokenLine),
              paramMap, round, numberOfParallelism)
              .addSink(q => logger.info("result: " + q.deep.mkString("[", ",", "]"))).setParallelism(1)

          case Action.TrainAndSaveClose =>
            val modelPath = getOptParam("modelPath")
            tracker = Some(XGboost.trainAndSaveTheModelWithClose(
              MLStreamUtils.readLibSVM(env, trainPath, dimension, readParallelism, skipBrokenLine),
              paramMap, round, numberOfParallelism, modelPath))

        }

      }

      println(env.getExecutionPlan)
      val start = System.currentTimeMillis()
      env.execute()
      logger.info(s"###PS###p;jobtime;${(System.currentTimeMillis() - start) / 1000}")


//       ==== train and close ????
      if (act == Action.TrainAndSaveClose && !tracker.isEmpty) {
        val t = tracker.get
        val trackerReturnVal = t.waitFor(0L)
        logger.info(s"Rabit returns with exit code $trackerReturnVal")
        t.stop()
      }
    })
  }

}
