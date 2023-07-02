package es.udc.graph.utils

import org.apache.spark.ml.feature.VectorAssembler
import org.apache.spark.ml.linalg.Vector
import org.apache.spark.ml.feature.LabeledPoint
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.functions.{col, lit, monotonically_increasing_id}
import org.apache.spark.sql.{DataFrame, Dataset, SparkSession}
import org.apache.spark.sql.types.{DoubleType, LongType, StructField, StructType}

object SourceUnit {
  def readEdgesCSV(path: String)(implicit spark: SparkSession): DataFrame = {
    spark.read.schema(StructType(Array(
      StructField("id1", LongType, true),
      StructField("id2", LongType, true),
      StructField("distance", DoubleType, true)
    ))).csv(path)
  }

  def readInputLibsvm(path: String)(implicit spark: SparkSession): DataFrame = {
    spark.read.format("libsvm")
      .load(path)
      .withColumn("id", monotonically_increasing_id())
    //           todo # in ald implementation it can have gaps, should be like for CompareGraphs?
    //            .withColumn("id", row_number().over(Window.orderBy("sq_id_gaps")))
  }

  def readInputTxt(path: String)(implicit spark: SparkSession): DataFrame = {
    val inputRaw = spark.read
      .option("delimiter", ";").option("inferSchema", true)
      .csv(path)

    val id = (inputRaw.columns.map(col).head.cast("Long") - 1).as("id")
    val vectorNameColumns = inputRaw.columns.tail
    val vectorColumns = vectorNameColumns.map(c => col(c).cast("Double").as(c)).toList
    val assembler = new VectorAssembler()
      .setInputCols(Array(vectorNameColumns: _*))
      .setOutputCol("features")

    assembler
      .transform(inputRaw.select((id :: vectorColumns): _*))
      .withColumn("label", lit(0d))
  }

  def dataFrameInputToDataset(df: DataFrame): Dataset[(Long,LabeledPoint)] = {
    import df.sparkSession.implicits._

    import org.apache.spark.mllib.linalg.Vectors
//    val mllibVec: org.apache.spark.mllib.linalg.Vector = Vectors.sparse(3, Array(1, 2, 3), Array(1, 2, 3))
//    val mlVec: org.apache.spark.ml.linalg.Vector = mllibVec.asML
//    val mllibVec2: org.apache.spark.mllib.linalg.Vector =

    df.select("id", "label", "features").as[(Long, Double, Vector)]
      .map(r => (r._1, LabeledPoint(r._2, r._3)))
  }

  def distanceRDDToDataFrame(rdd: RDD[(Long, Long, Double)])(implicit spark: SparkSession): DataFrame = {
    import spark.implicits._

    rdd.toDF("id1", "id2", "distance")
  }

  def distanceDatasetToDataFrame(rdd: Dataset[(Long, Long, Double)])(implicit spark: SparkSession): DataFrame = {
    rdd.toDF("id1", "id2", "distance")
  }
}
