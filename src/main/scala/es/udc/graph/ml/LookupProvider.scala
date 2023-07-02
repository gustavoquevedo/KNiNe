package es.udc.graph.ml

import es.udc.graph.sparkContextSingleton
import org.apache.spark.broadcast.Broadcast
import org.apache.spark.ml.feature.LabeledPoint
import org.apache.spark.ml.linalg.{DenseVector, Vector}
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.Dataset
import org.apache.spark.sql.functions.col

trait LookupProvider extends Serializable
{
  def lookup(index:Long):LabeledPoint;
}

class DummyLookupProvider() extends LookupProvider
{
  //private def lookupTable=dataset.collect()
  private def dummy:LabeledPoint=new LabeledPoint(1,new DenseVector(Array[Double](1.0, 1.0, 1.0, 1.0)))
  def lookup(index:Long):LabeledPoint=
  {
    return dummy
  }
}

class BroadcastLookupProvider(dataset: Dataset[(Long,LabeledPoint)]) extends LookupProvider
{
  /* Test to check the order of the collected items
  val test=dataset.sortBy(_._2).collect()
  for(x <- test)
    println(x)*/
  dataset.count().toInt //This should throw an exception if the dataset is too large
  
  val bData= {
    import dataset.sparkSession.implicits._
    sparkContextSingleton.getInstance().broadcast(dataset.orderBy(col("_1")).map(_._2).collect())
  }
  
  def lookup(index:Long):LabeledPoint=
  {
    return bData.value(index.toInt)
  }
}

class SplittedBroadcastLookupProvider(dataset: Dataset[(Long,LabeledPoint)]) extends LookupProvider
{
  /* Test to check the order of the collected items
  val test=dataset.sortBy(_._2).collect()
  for(x <- test)
    println(x)*/
  val NUM_PARTS=4
  import dataset.sparkSession.implicits._
  val numElems=dataset.count().toInt //This should throw an exception if the dataset is too large
  val numElemsPerPart=math.ceil(numElems.toDouble/NUM_PARTS).toInt
  
  val bDatas:Array[Broadcast[Array[LabeledPoint]]]=new Array[Broadcast[Array[LabeledPoint]]](NUM_PARTS)
  
  for (i <- 0 until NUM_PARTS)
  {
    bDatas(i)=dataset.sparkSession.sparkContext.broadcast(dataset.filter(r => (r._1>=i*numElemsPerPart) && (r._1<(i+1)*numElemsPerPart)).orderBy(col("_1")).map(_._2).collect())
  }
  
  def lookup(index:Long):LabeledPoint=
  {
    assert(index>=0)
    var i=0
    while (index>=(i+1)*numElemsPerPart)
      i+=1
    return bDatas(i).value((index-i*numElemsPerPart).toInt)
  }
}