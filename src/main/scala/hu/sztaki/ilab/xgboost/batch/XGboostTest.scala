package hu.sztaki.ilab.xgboost.batch

import ml.dmlc.xgboost4j.scala.flink.XGBoostModel
import ml.dmlc.xgboost4j.scala.flink.batch.XGboost
import org.apache.commons.logging.LogFactory
import org.apache.flink.api.java.utils.ParameterTool
import org.apache.flink.api.scala._
import org.apache.flink.ml.MLUtils

object XGboostTest {
  val logger = LogFactory.getLog(this.getClass)
  val HADOOP_FILE_PREFIX = "hdfs:"

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
      val start = System.currentTimeMillis()
      val params = ParameterTool.fromPropertiesFile(propsPath)

      val getOptParam = setParameter(params) _
      def setTraining =  setTrainingParameter(params)

      //
      // parallelism
      val numberOfParallelism = getOptParam("numberOfParallelism").toInt
      // 0 - train
      // 1 - train and save model
      // 2 - predict
      // 3 - train and predict
      val action = getOptParam("action").toInt
      require(0 to 4 contains  action, "The action parameter must be contained by the interval [0, 4].")
      val act = Action(action)
      logger.info(s"###PS###p;action;$act")

      val env = ExecutionEnvironment.getExecutionEnvironment

      logger.info("Default parallelism: " + env.getParallelism)

      if (act == Action.Predict) {
        val modelPath = getOptParam("modelPath")
        val testPath = getOptParam("testPath")
        logger.info("result: " +
        XGboost.predict(
          XGBoostModel.loadModelFromFile(modelPath, modelPath.startsWith(HADOOP_FILE_PREFIX)),
          MLUtils.readLibSVM(env, testPath),
          numberOfParallelism).deep.mkString("[", ",", "]"))
      }
      else {
        val round = getOptParam("round").toInt
        val paramMap = setTraining
        val trainPath = getOptParam("trainPath")

        act match {
          case Action.Train =>
            logger.info("result: " +
            XGboost.train(
              MLUtils.readLibSVM(env, trainPath),
              paramMap, round, numberOfParallelism)
            )

          case Action.TrainAndSave =>
            val modelPath = getOptParam("modelPath")

            XGboost.trainAndSaveModelToFile(
              MLUtils.readLibSVM(env, trainPath),
              paramMap, round, numberOfParallelism, modelPath,
              saveAsHadoopFile = modelPath.startsWith(HADOOP_FILE_PREFIX))
          case Action.TrainAndPredict =>
            val testPath = getOptParam("testPath")

            logger.info("result: " +
            XGboost.trainAndPredict(MLUtils.readLibSVM(env, trainPath),
              MLUtils.readLibSVM(env, testPath),
              paramMap, round, numberOfParallelism).deep.mkString("[", ",", "]"))
        }

      }

      logger.info(s"###PS###p;jobtime;${(System.currentTimeMillis() - start) / 1000}")

    })
  }

}
