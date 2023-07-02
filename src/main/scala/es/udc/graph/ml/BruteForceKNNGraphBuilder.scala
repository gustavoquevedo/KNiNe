package es.udc.graph.ml

import es.udc.graph._
import es.udc.graph.ml.entities.{GroupedNeighborsForElement, GroupedNeighborsForElementWithComparisonCount, NeighborsForElement}
import org.apache.spark.ml.feature.LabeledPoint
import org.apache.spark.sql.{Dataset, SparkSession}

object BruteForceKNNGraphBuilder {
  def computeGroupedGraph(arrayIndices:Array[Long], lookup:LookupProvider, numNeighbors:Int, measurer:DistanceProvider, grouper:GroupingProvider):List[(Long, GroupedNeighborsForElementWithComparisonCount)]=
  {
    val closestNeighbors=new Array[GroupedNeighborsForElementWithComparisonCount](arrayIndices.length) //For each element stores the farthest near neighbor so far and a list of near neighbors with their distances
    
    //The computed distances could be stored elsewhere so that there is no symmetric repetition
    for(i <- 0 until arrayIndices.length)
      closestNeighbors(i)=GroupedNeighborsForElementWithComparisonCount.newEmpty(grouper.getGroupIdList(),numNeighbors)
    
    var graph:List[(Long, GroupedNeighborsForElementWithComparisonCount)]=Nil //Graph to be returned

    for(i <- 0 until arrayIndices.length)
    {
      assert(arrayIndices(i)>=0)
      for(j <- i+1 until arrayIndices.length)
      {
         if (measurer==null)
           println("NULL measurer")
         if (lookup==null)
           println("NULL lookup")
         if (arrayIndices==null)
           println("NULL arrayIndices")
         if (arrayIndices(i)<0)
           println(s"CHECK ERROR!!!! arrayIndices(i)=${arrayIndices(i)}")
         if (arrayIndices(j)<0)
           println(s"CHECK ERROR!!!! arrayIndices(i)=${arrayIndices(j)}")
           
         assert((arrayIndices(i)>=0) && (arrayIndices(j)>=0)) 
         
         val d=measurer.getDistance(lookup.lookup(arrayIndices(i)),
                                     lookup.lookup(arrayIndices(j)))
         
         //println("D("+arrayIndices(i)+"<->"+arrayIndices(j)+")="+d+"#"+feat1.toString()+feat2.toString())
         
         val x=lookup.lookup(arrayIndices(i))
         val y=lookup.lookup(arrayIndices(j))
         val grIdX=grouper.getGroupId(x)
         val grIdY=grouper.getGroupId(y)
         
         closestNeighbors(i).addElementOfGroup(grIdY, arrayIndices(j), d)
         closestNeighbors(j).addElementOfGroup(grIdX, arrayIndices(i), d)
         
      }
      
      graph = (arrayIndices(i),closestNeighbors(i)) :: graph
    }
    
    graph
  }
  
  def computeGroupedGraph(list1:List[Long], list2:List[Long], lookup:LookupProvider, numNeighbors:Int, measurer:DistanceProvider, grouper:GroupingProvider):List[(Long, GroupedNeighborsForElementWithComparisonCount)]=
  {
    val points2=list2.map({case index =>
                            val p=lookup.lookup(index)
                            (index,p,grouper.getGroupId(p),GroupedNeighborsForElementWithComparisonCount.newEmpty(grouper.getGroupIdList(), numNeighbors))
                          })
    list1.map({case index =>
              val p=lookup.lookup(index)
              val neighs=GroupedNeighborsForElementWithComparisonCount.newEmpty(grouper.getGroupIdList(), numNeighbors)
              val grId=grouper.getGroupId(p)
              for ((i2,p2,grId2,n2) <- points2)
              {
                val d=measurer.getDistance(p,p2)
                neighs.addElementOfGroup(grId2, i2, d)
                n2.addElementOfGroup(grId, index, d)
              }
              (index,neighs)
          }).toList ++ points2.map({case (id, p, grId, neighs) => (id,neighs)})
  }
  
  def computeGroupedGraph(data:Dataset[(Long,LabeledPoint)], numNeighbors:Int):List[(Long, GroupedNeighborsForElementWithComparisonCount)]=
  {
    val arrayIndices=data.rdd.map(_._1).collect()
    val lookup=new BroadcastLookupProvider(data)
    computeGroupedGraph(arrayIndices, new BroadcastLookupProvider(data), numNeighbors)
  }
  
  def computeGroupedGraph(arrayIndices:Array[Long], lookup:LookupProvider, numNeighbors:Int):List[(Long, GroupedNeighborsForElementWithComparisonCount)]=
    computeGroupedGraph(arrayIndices, lookup, numNeighbors, new EuclideanDistanceProvider())
    
  def computeGroupedGraph(arrayIndices:Array[Long], lookup:LookupProvider, numNeighbors:Int, measurer:DistanceProvider):List[(Long, GroupedNeighborsForElementWithComparisonCount)]=
    computeGroupedGraph(arrayIndices, lookup, numNeighbors, measurer, new DummyGroupingProvider())
  
  /*def computeGroupedGraph(arrayIndices:Array[Long], lookup:LookupProvider, numNeighbors:Int, measurer:DistanceProvider, grouper:GroupingProvider):List[(Long, (Int,List[(Int,List[(Long, Double)])]))]=
  {
    val graph=computeGroupedGraph(arrayIndices, lookup, numNeighbors, measurer, grouper)
    return graph.map(
        {
          case (index, (numVisited, groupedNeighbors)) => (index, (numVisited,groupedNeighbors.head._2))
        })
  }*/
  
  def parallelComputeGroupedGraph(arrayIndices:Array[Long], lookup:LookupProvider, numNeighbors:Int, measurer:DistanceProvider, grouper:GroupingProvider, numPartitions:Int)(implicit spark: SparkSession):Dataset[(Long, GroupedNeighborsForElement)]=
  {
    import spark.implicits._

    var sc=sparkContextSingleton.getInstance()
    
    var rddIndices=sc.parallelize(arrayIndices, numPartitions)
    
    var rddPairs=rddIndices.cartesian(rddIndices).coalesce(numPartitions)
    
    //TODO Check whether filtering i<j improves performance
    return rddPairs.flatMap({case (i,j) => if (i==j)
                                             None
                                           else
                                           {
                                             val x=lookup.lookup(i)
                                             val y=lookup.lookup(j)
                                             val d=measurer.getDistance(x,y)
                                             
                                             val n=new NeighborsForElement(numNeighbors)
                                             n.addElement(j, d)
                                             Some(((i,grouper.getGroupId(x)), n))
                                           }
                })
            .reduceByKey({case (n1, n2) => n1.addElements(n2)
                                           n1
                }: (NeighborsForElement, NeighborsForElement) => NeighborsForElement)
            .map({case ((index,groupingId), neighbors) =>
                                      val gr=GroupedNeighborsForElement.newEmpty(grouper.getGroupIdList(),numNeighbors)
                                      gr.addElementsOfGroup(groupingId, neighbors)
                                      (index,gr)
                })
            .reduceByKey({case (l1,l2) =>
                                  l1.addElements(l2)
                                  l1}: (GroupedNeighborsForElement, GroupedNeighborsForElement) => GroupedNeighborsForElement).toDS()
  }
  
  def parallelComputeGraph(data:Dataset[(Long,LabeledPoint)], numNeighbors:Int, numPartitions:Int):(Dataset[(Long, NeighborsForElement)],LookupProvider)=
    parallelComputeGraph(data, numNeighbors, new EuclideanDistanceProvider(), numPartitions)
    
  def parallelComputeGraph(data:Dataset[(Long,LabeledPoint)], numNeighbors:Int, measurer:DistanceProvider, numPartitions:Int):(Dataset[(Long, NeighborsForElement)],LookupProvider)=
  {
    implicit val spark = data.sparkSession
    import spark.implicits._

    val lookup=new BroadcastLookupProvider(data)
    val graph=parallelComputeGroupedGraph(data.rdd.map(_._1).collect(), lookup, numNeighbors, measurer, new DummyGroupingProvider(), numPartitions)
    (graph.rdd.map(
               {case (i1,groupedNeighbors) =>
                 (i1,groupedNeighbors.groupedNeighborLists.head._2)
               }).toDS(),lookup)
  }
}