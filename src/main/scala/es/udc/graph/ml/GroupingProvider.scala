package es.udc.graph.ml

import org.apache.spark.ml.feature.LabeledPoint

trait GroupingProvider extends Serializable
{
  def getGroupId(p1:LabeledPoint):Int;
  def getGroupIdList():List[Int]
  def numGroups:Int;
}

class DummyGroupingProvider() extends GroupingProvider
{
  val DEFAULT_GROUPID=0
  def getGroupId(p1:LabeledPoint):Int=
  {
    return DEFAULT_GROUPID
  }
  def getGroupIdList():List[Int]=
  {
    return List[Int](0)
  }
  def numGroups:Int=1
}
