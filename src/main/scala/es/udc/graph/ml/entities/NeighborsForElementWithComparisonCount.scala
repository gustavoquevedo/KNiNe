package es.udc.graph.ml.entities


case class NeighborsForElementWithComparisonCount(numNeighbors:Int, pComparisons:Int, var _listNeighbors: List[IndexDistancePair]=List[IndexDistancePair](),
                                                  var _maxDistance:Double=Double.MinValue)  extends NeighborsForElementAbstract
{
  def this(pNumNeighbors:Int)=this(pNumNeighbors,0)

  private var _comparisons=pComparisons
  def comparisons=_comparisons
  def addElements(n:NeighborsForElementWithComparisonCount):Unit=
  {
    super.addElements(n)
    _comparisons+=n.comparisons
  }

  def asGroupedWithCounts()=
    WrappedUngroupedNeighborsForElementWithComparisonCount.wrap(this)
}


