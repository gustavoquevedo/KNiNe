package es.udc.graph.ml.entities

import org.apache.spark.ml.linalg.{DenseVector => BDV}
//import breeze.linalg.{DenseVector => BDV}
import scala.collection.mutable.{HashMap, Map}

abstract class GroupedNeighborsForElementAbstract
{
  def pNeighbors:Map[Int,NeighborsForElement]
  def groupIdList:List[Int]
  def numNeighbors:Int

  def neighbors=pNeighbors
  def groupedNeighborLists=neighbors.toList
  def neighborsOfGroup(i:Int)=neighbors.get(i)
  def numberOfGroupsWithElements()=neighbors.size
  def numberOfGroupsWithAtLeastKElements(k:Int):Int=neighbors.values.count(_.listNeighbors.size>=k)
  def getIdsOfIncompleteGroups():List[Int]=
  {
    groupIdList
      .filter({case grId =>
        val n=neighbors.get(grId)
        n.isEmpty || (n.get.listNeighbors.size<n.get.numNeighbors)
      })
      .toList
  }

  def addElements(n:GroupedNeighborsForElementAbstract):Unit=
  {
    for ((k,v) <- n.neighbors)
      addElementsOfGroup(k,v)
  }

  private def getOrCreateGroup(groupId:Int):NeighborsForElement=
  {
    val g=neighbors.get(groupId)
    if (g.isDefined) return g.get
    val newGroup=new NeighborsForElement(numNeighbors)
    neighbors(groupId)=newGroup
    return newGroup
  }

  def addElementsOfGroup(groupId:Int, n:NeighborsForElementAbstract):Unit=
  {

    getOrCreateGroup(groupId).addElements(n)
  }

  def addElementOfGroup(groupId:Int, index:Long, distance:Double):Unit=
  {
    getOrCreateGroup(groupId).addElement(index, distance)
  }

  def wrapWithCounts(pComparisons:BDV):GroupedNeighborsForElementWithComparisonCount=
  {
    new GroupedNeighborsForElementWithComparisonCount(neighbors, groupIdList, numNeighbors, pComparisons)
  }
}

case class GroupedNeighborsForElement(pNeighbors:Map[Int,NeighborsForElement], val groupIdList:List[Int], val numNeighbors:Int) extends GroupedNeighborsForElementAbstract

object GroupedNeighborsForElement
{
  def newEmpty(groupIdList:List[Int], numNeighbors:Int)=new GroupedNeighborsForElement(new HashMap[Int,NeighborsForElement], groupIdList, numNeighbors)
}
