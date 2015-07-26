//import ngn.spark._
//import ngn.spark.ml.{SparkMl, Evaluators}
//import org.apache.spark.ml.{Pipeline}
//import org.apache.spark.ml.regression.LinearRegression
//import org.apache.spark.sql.{DataFrame}
//import org.apache.spark.{SparkContext}
//
//object LinearRegressionApp extends SparkJobApp { run(classOf[LinearRegressionJob]) }
//
//class LinearRegressionJob(sc: SparkContext) extends SparkJob(sc) with SparkMl {
//    import Evaluators._
//
//    val path = "linear-regression-libsvm.txt"
//    val dataFormat = "libsvm"
//
//    // Prepare the pipeline
//    val lir = new LinearRegression()
//      .setFeaturesCol("features")
//      .setLabelCol("label")
//      .setRegParam(0.0)
//      .setElasticNetParam(0.0)
//      .setMaxIter(500)
//      .setTol(1E-6)
//
//    val pipeline = new Pipeline().setStages(Array(lir))
//
//    // Get the data
//    val data: DataFrame = load(path, dataFormat)
//    val training :: test :: Nil = splitData(data, List(.8, .2)).map(_.cache)
//
//    val numTraining = training.count()
//    val numTest = test.count()
//    val numFeatures = training.select("features").first().size
//
//    println("Loaded data:")
//    println(s"  numTraining = $numTraining, numTest = $numTest")
//    println(s"  numFeatures = $numFeatures")
//
//    val model = pipeline.fit(training)
//
//    evaluateRegressionModel(model, training, "label")
//    evaluateRegressionModel(model, test, "label")
//}