package es.udc.graph.ml.entities

import org.apache.spark.ml.linalg.{DenseVector => BDV}
import org.apache.spark.ml.linalg.Vectors
//import breeze.linalg.{DenseVector => BDV}
import es.udc.graph.mllib.DummyGroupingProvider
import scala.collection.mutable.{HashMap, Map}

object WrappedUngroupedNeighborsForElementWithComparisonCount
{
  val wrappingGrouper=new DummyGroupingProvider()
  def wrap(n:NeighborsForElementWithComparisonCount):GroupedNeighborsForElementWithComparisonCount=
  {
    val newElem=new GroupedNeighborsForElementWithComparisonCount(new HashMap[Int,NeighborsForElement],
      wrappingGrouper.getGroupIdList(),
      n.numNeighbors,
      Vectors.zeros(wrappingGrouper.numGroups).toDense)
    newElem.addElementsOfGroup(wrappingGrouper.DEFAULT_GROUPID, n, n.comparisons)
    newElem
  }

  def unwrap(n:GroupedNeighborsForElementWithComparisonCount): NeighborsForElementWithComparisonCount = {
    n.neighborsWithComparisonCountOfGroup(WrappedUngroupedNeighborsForElementWithComparisonCount.wrappingGrouper.DEFAULT_GROUPID).get
  }
}
