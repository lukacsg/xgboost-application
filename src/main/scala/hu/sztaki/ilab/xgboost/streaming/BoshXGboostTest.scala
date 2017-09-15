package hu.sztaki.ilab.xgboost.streaming

import ml.dmlc.xgboost4j.scala.flink.stream.XGboost
import ml.dmlc.xgboost4j.java.IRabitTracker
import ml.dmlc.xgboost4j.scala.flink.XGBoostModel
import ml.dmlc.xgboost4j.scala.flink.utils.MLStreamUtils
import org.apache.commons.logging.LogFactory
import org.apache.flink.api.java.utils.ParameterTool
import org.apache.flink.streaming.api.functions.sink.RichSinkFunction
import org.apache.flink.streaming.api.scala._

object BoshXGboostTest {
  val logger = LogFactory.getLog(this.getClass)

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
        XGboost.predict(
          XGBoostModel.loadModelFromFile(modelPath),
          MLStreamUtils.readLibSVM(env, testPath, dimension, readParallelism, skipBrokenLine),
          numberOfParallelism)
          .addSink(
            new RichSinkFunction[Array[Array[Float]]] {
              import scala.collection.mutable._
              val wholeModel: ArrayBuffer[Array[Float]] = ArrayBuffer.empty[Array[Float]]
              val treshold = 0.5

              override def invoke(value: Array[Array[Float]]) {
                wholeModel ++= value
              }

              override def close() {
                super.close()
                logger.info("result size: " + wholeModel.size)

                val hit = wholeModel.map(_.head).count(_ >= treshold)
                logger.info("result : " + wholeModel)
                logger.info("result 1: " + hit)
                logger.info("result 0: " + (wholeModel.size - hit))
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
