package es.udc.graph.ml.entities

import org.apache.spark.ml.linalg.{Vectors, DenseVector => BDV, Vector}
//import breeze.linalg.{DenseVector => BDV}


import scala.collection.mutable.Map

case class GroupedNeighborsForElementWithComparisonCount(pNeighbors:Map[Int,NeighborsForElement], groupIdList:List[Int], numNeighbors:Int, pComparisons:Vector) extends GroupedNeighborsForElementAbstract
{
  private var _comparisons=pComparisons
  def comparisons=_comparisons

  def neighborsWithComparisonCountOfGroup(grId:Int):Option[NeighborsForElementWithComparisonCount]=
  {
    val neighs=super.neighborsOfGroup(grId)
    if (neighs.isEmpty)
      return None
    return Some(neighs.get.wrapWithCount(comparisons(grId).toInt))
  }

  def getIdsOfGroupsWithAtLeastKComparisons(k:Int):List[Int]=
    comparisons.toDense.values.zipWithIndex.filter(_._1>=k).map(_._2).toList

  def getIdsOfGroupsWithLessThanKComparisons(k:Int):List[Int]=
    comparisons.toDense.values.zipWithIndex.filter(_._1<k).map(_._2).toList

  def addElements(n:GroupedNeighborsForElementWithComparisonCount):Unit=
  {
    super.addElements(n)
    (0 to _comparisons.size - 1).foreach(i => {
      _comparisons.toArray(i) += comparisons.toArray(i)
    })
  }

  def addElementsOfGroup(groupId:Int, n:NeighborsForElementAbstract, pComparisons:Int):Unit=
  {
    super.addElementsOfGroup(groupId,n)
    _comparisons.toArray(groupId)+=pComparisons
  }

  override def addElementOfGroup(groupId:Int, index:Long, distance:Double):Unit=
  {
    super.addElementOfGroup(groupId,index,distance)
    _comparisons.toArray(groupId)+=1
  }

  def toGroupedNeighborsForElement() = {
    GroupedNeighborsForElement(pNeighbors, groupIdList, numNeighbors)
  }
}

object GroupedNeighborsForElementWithComparisonCount
{
  def newEmpty(groupIdList:List[Int], numNeighbors:Int):GroupedNeighborsForElementWithComparisonCount=
    GroupedNeighborsForElement.newEmpty(groupIdList,numNeighbors).wrapWithCounts(Vectors.zeros(groupIdList.size).toDense)
}

