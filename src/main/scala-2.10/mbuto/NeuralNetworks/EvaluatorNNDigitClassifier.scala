package mbuto.NeuralNetworks

import ngn.spark.SparkJob
import org.apache.spark.SparkContext
import org.apache.spark.ml.evaluation.MulticlassClassificationEvaluator
import org.apache.spark.ml.{Pipeline, PipelineModel}
import org.apache.spark.sql.{DataFrame, SQLContext}

/**
  * Created by gian on 08/05/16.
  */
class EvaluatorNNDigitClassifier(pipeline: Pipeline, train: DataFrame, test: DataFrame)   //(model: PipelineModel)
  extends SparkJob[Unit]
    with Serializable {

  override def execute(implicit sc: SparkContext, sqlContext: SQLContext): Unit = {
    val timestampStart: Long = System.currentTimeMillis



    val labels = Seq("0","1","2","3","4","5","6","7","8","9")



    println(s"Evaluating (and training) the Base NN digit classification pipeline: \n Params of the pipeline: \n (${pipeline.explainParams()})")

    println("Pipeline stages:")
    val stages = pipeline.getStages.map(x => println(x.getClass))



    train.persist(org.apache.spark.storage.StorageLevel.MEMORY_ONLY)
    test.persist(org.apache.spark.storage.StorageLevel.MEMORY_ONLY)

    println(s"Train Mnist DIGIT pixels loaded: ${train.count()}")
    println(s"Test count: ${test.count}")

    val model: PipelineModel = pipeline.fit(train)


    // Save and load model
    //model.save("/data/1.6/ml/model/NeuralNetworks/baseMultilayer")
    println("MLB Neural Network Model (PIPELINE): "+model.toString())
    println(s"MLBNN Stages: Vector[${model.stages.size}] = \n ${model.stages.toString}")


    val result: DataFrame = model.transform(test)

    result.show(2)
    //result.printSchema


    val evaluator = { new MulticlassClassificationEvaluator()
      .setLabelCol("indexedLabel") //StringIndexer.getOutputCol
      .setPredictionCol("prediction")
      .setMetricName("precision")
    }
    val precision = evaluator.evaluate(result)

    println(s"Precision: ${precision}")

    import org.apache.spark.sql.functions._
    val confusionMatrix = {
      result.select(col("label"), col("originalLabel"))  ///StringIndexer.getOutputCol - IndexToString.getOutputCol
        .orderBy("label")
        .groupBy("label")
        .pivot("originalLabel",labels)
        .count
    }

    println(s"Confusion Matrix (Vertical: Actual, Horizontal: Predicted):")
    confusionMatrix.show

    println(s"Duration: ${(System.currentTimeMillis - timestampStart)} ms (~${(System.currentTimeMillis - timestampStart)/1000/60} minutes)")
    println(s"Precision: ${precision}")

    train.unpersist()
    test.unpersist()
    //train
  }
}
