package es.udc.graph.ml.entities


abstract class NeighborsForElementAbstract
{
  def numNeighbors:Int
  var _maxDistance:Double
  var _listNeighbors: List[IndexDistancePair]
  def listNeighbors=_listNeighbors
  def setListNeighbors(l:List[IndexDistancePair])=
  {
    _listNeighbors=l
    _maxDistance=Double.MinValue
    for (n <- l)
      if (n.distance>_maxDistance)
        _maxDistance=n.distance
  }

  def addElement(index:Long, distance:Double):Unit=
  {
    if (listNeighbors.size<numNeighbors)
    {
      if (_maxDistance<distance)
        _maxDistance=distance
      if (!listNeighbors.map(_.index).contains(index))
        _listNeighbors=IndexDistancePair(index, distance) :: listNeighbors
    }
    else //Already have enough neighbors
    {
      if ((_maxDistance>distance) && !listNeighbors.map(_.index).contains(index)) //Only update if this one is closer than the ones we already have
      {
        //Loop once through the existing neighbors replacing the one that was farthest with the new one and, at the same time, updating the maximum distance
        var maxDistanceFound=Double.MinValue
        var isInserted=false
        for (n <- listNeighbors)
        {
          if (!isInserted)
          {
            if (n.distance==_maxDistance)
            {
              n.index=index
              n.distance=distance
              isInserted=true
            }
          }
          if (n.distance>maxDistanceFound)
            maxDistanceFound=n.distance
        }
        _maxDistance=maxDistanceFound
      }
    }
  }
  def addElements(n:List[IndexDistancePair]):Unit=
  {
    for (p <- n)
      this.addElement(p.index, p.distance)
  }
  def addElements(n:NeighborsForElementAbstract):Unit=addElements(n.listNeighbors)

  def wrapWithCount(pComparisons:Int):NeighborsForElementWithComparisonCount=
  {
    val newN=new NeighborsForElementWithComparisonCount(numNeighbors, pComparisons)
    newN._listNeighbors=_listNeighbors
    return newN
  }
}

case class NeighborsForElement(val numNeighbors:Int, var _listNeighbors: List[IndexDistancePair]=List[IndexDistancePair](),
                               var _maxDistance:Double=Double.MinValue) extends NeighborsForElementAbstract

object NeighborsForElement
{
  def merge(n1:NeighborsForElement, n2:NeighborsForElement):NeighborsForElement=
  {
    val maxNeighbors=math.max(n1.numNeighbors, n2.numNeighbors)
    var sNeighbors1=n1.listNeighbors.sortBy(_.distance)
    var sNeighbors2=n2.listNeighbors.sortBy(_.distance)

    var finalNeighbors:List[IndexDistancePair]=Nil

    while(finalNeighbors.size<maxNeighbors && (!sNeighbors1.isEmpty || !sNeighbors2.isEmpty))
    {
      if (sNeighbors2.isEmpty || (!sNeighbors1.isEmpty && sNeighbors1.head.distance<sNeighbors2.head.distance))
      {
        if ((finalNeighbors==Nil) || !finalNeighbors.contains(sNeighbors1.head))
          finalNeighbors=sNeighbors1.head :: finalNeighbors
        sNeighbors1=sNeighbors1.tail
      }
      else
      {
        if ((finalNeighbors==Nil) || !finalNeighbors.contains(sNeighbors2.head))
          finalNeighbors=sNeighbors2.head :: finalNeighbors
        sNeighbors2=sNeighbors2.tail
      }
    }

    val newElem=new NeighborsForElement(maxNeighbors)
    newElem.setListNeighbors(finalNeighbors)
    newElem
  }
}

case class IndexDistancePair(var index: Long, var distance: Double)
