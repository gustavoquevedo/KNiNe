package es.udc.graph.ml

import org.apache.spark.ml.feature.LabeledPoint
import org.apache.spark.ml.linalg.Vectors

trait DistanceProvider extends Serializable
{
  def getDistance(p1:LabeledPoint,p2:LabeledPoint):Double;
}

class EuclideanDistanceProvider() extends DistanceProvider
{
  def getDistance(p1:LabeledPoint,p2:LabeledPoint):Double=
  {
    return Vectors.sqdist(p1.features, p2.features)
  }
}
