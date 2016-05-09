package examples

import ngn.spark.SparkJob
import org.apache.spark.SparkContext
import org.apache.spark.ml.Pipeline
import org.apache.spark.ml.classification.LogisticRegression
import org.apache.spark.ml.evaluation.BinaryClassificationEvaluator
import org.apache.spark.ml.feature.{HashingTF, Tokenizer}
import org.apache.spark.ml.tuning.{CrossValidator, ParamGridBuilder}
import org.apache.spark.sql.SQLContext

/**
  * Created by gian on 08/05/16.
  */
class mlPipelinePersistence
  extends SparkJob[Unit]
    with Serializable {


    override def execute(implicit sc: SparkContext, sqlContext: SQLContext): Unit = {
      // Create the workflow"
      val tokenizer = new Tokenizer()
        .setInputCol("text")
        .setOutputCol("words")
      val hashingTF = new HashingTF()
        .setInputCol(tokenizer.getOutputCol)
        .setOutputCol("features")
      val lr = new LogisticRegression()
        .setMaxIter(3)
      val pipeline = new Pipeline()
        .setStages(Array(tokenizer, hashingTF, lr))

      val paramMaps = new ParamGridBuilder()
        .addGrid(hashingTF.numFeatures, Array(10, 20))
        .addGrid(lr.regParam, Array(0.0, 0.1))
        .build()
      // Note: In practice, you would likely want to use more features and more iterations for LogisticRegression.
      val evaluator = new BinaryClassificationEvaluator()

      val cv = new CrossValidator()
        .setEstimator(pipeline)
        .setEvaluator(evaluator)
        .setNumFolds(2)
        .setEstimatorParamMaps(paramMaps)

      //Save the workflow
      //cv.write.overwrite().save("/data/pesristed/ml/model/textClass/pipeline")
      // You can also write this, following the spark.mllib API (but this will not overwrite):
     // cv.save("/data/pesristed/ml/model/textClass/savedPipeline")

      // Load the workflow back
      //val sameCV = CrossValidator.load("/data/pesristed/ml/model/pipelinePersistence/pipeline")



      //val sqlcontext = new SQLContext(sc)
      val data = Seq(
        ("Hello hello world", 0.0),
        ("Hello how are you world", 0.0),
        ("What is the world", 1.0),
        ("There was a plant in the water", 0.0),
        ("Water was around the world", 1.0),
        ("And then hello again", 0.0))
      val df = sqlContext.createDataFrame(data).toDF("text", "label")

      // Fit the model
      val cvModel = cv.fit(df)

      // Save the fitted model
      //cvModel.write.overwrite().save("/data/pesristed/ml/model/pipelinePersistence/fittedPipeline")

      // Load the fitted model back
      //val sameCVModel = CrossValidatorModel.load("/data/pesristed/ml/model/pipelinePersistence/fittedPipeline")

      println("NOT Persisted Model succesfully loaded:"+cvModel.toString())
      println(cvModel.explainParams)
    }

  }
