package es.udc.graph.ml

import es.udc.graph._
import es.udc.graph.ml.entities.{GroupedNeighborsForElement, GroupedNeighborsForElementWithComparisonCount, NeighborsForElement, NeighborsForElementWithComparisonCount}
import org.apache.spark.HashPartitioner
import org.apache.spark.ml.feature.LabeledPoint
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.Dataset
import org.apache.spark.sql.functions._


object GraphMerger extends Serializable
{
  def mergeGraphs(g1:Dataset[(Long, NeighborsForElement)], g2:Dataset[(Long, NeighborsForElement)], numNeighbors:Int, measurer:DistanceProvider):Dataset[(Long, NeighborsForElement)]=
  {
    if (g1==null) return g2
    if (g2==null) return g1
    import g1.sparkSession.implicits._

    g1.union(g2).groupByKey(_._1).mapValues(_._2).reduceGroups(NeighborsForElement.merge(_, _))
  }

  def mergeGroupedNeighbors(groupedNeighbors1:List[(Int,List[(Long,Double)])], groupedNeighbors2:List[(Int,List[(Long,Double)])], numNeighbors:Int):List[(Int, List[(Long, Double)])]=
  {
    val src=if (!groupedNeighbors1.isEmpty) groupedNeighbors1 else groupedNeighbors2
    val dest=if (!groupedNeighbors1.isEmpty) groupedNeighbors2 else groupedNeighbors1

    val mapNeighbors2=dest.toMap
    src.map({case (grId1, l1) =>
      val newList:List[(Long,Double)]=(if (mapNeighbors2.contains(grId1))
        l1 ++ mapNeighbors2.get(grId1).get
      else
        l1).toSet.toList
      if (newList.size<=numNeighbors)
        (grId1, newList)
      else
        (grId1, newList.sortBy(_._2).take(numNeighbors))
    })
  }
}

abstract class LSHKNNGraphBuilder extends GraphBuilder
{
  final def computeGroupedGraph(data:Dataset[(Long,LabeledPoint)], numNeighbors:Int, hasher:Hasher, startRadius:Option[Double], maxComparisonsPerItem:Option[Int], measurer:DistanceProvider, grouper:GroupingProvider):Dataset[(Long, GroupedNeighborsForElement)]=
  {
    val spark = data.sparkSession
    import spark.implicits._

    var fullGraph:Dataset[(Long, GroupedNeighborsForElementWithComparisonCount)]=null //(node, (viewed, List[(groupingId,List[(neighbor,distance))]]))
    var currentData:Dataset[(Long,(LabeledPoint,List[Int]))]=data.map({case (id, point) => (id,(point,List[Int]()))}) //Adding "special requests" list
    var radius=startRadius.getOrElse(KNiNeMain.DEFAULT_RADIUS_START)//5//0.1
    val totalElements=currentData.count()
    val bfOps:Double=totalElements*(totalElements-1)/2.0
    var totalOps:Long=0
    var numBuckets:Long=2
    var allElementsInSingleBucket=false
    var nodesLeft=currentData.count()
    val bHasher=spark.sparkContext.broadcast(hasher)

    println(f"Starting $numNeighbors%d-NN graph computation for $nodesLeft%d nodes")
    println(f"\t R0=$radius%g")
    println(f"\t cMAX=${maxComparisonsPerItem.getOrElse("auto")}%s\n")
    //while(!currentData.isEmpty())
    //while(nodesLeft>numNeighbors)
    //while((numBuckets>1 || nodesLeft>numNeighbors) && nodesLeft>1)
    while(nodesLeft>numNeighbors && nodesLeft>1 && !allElementsInSingleBucket)
    {
      //Maps each element to numTables (hash, index) pairs with hashes of keyLength length.
      val hash=(if (grouper.numGroups==1) //Special case for single group so that the number of hashes is not doubled and no useless separable buckets are created.
        currentData.flatMap({case (index, (point, specialRequests)) =>
          val hashes=bHasher.value.getHashes(point.features, index, radius)
          val grId=grouper.getGroupId(point)
          hashes.map({case (h,id) => (h.concat(new Hash(Array[Int](-1))),(id,grId,false))})
        })
      else
        currentData.flatMap({case (index, (point, specialRequests)) =>
          val hashes=bHasher.value.getHashes(point.features, index, radius)
          val grId=grouper.getGroupId(point)
          if (specialRequests.isEmpty)
            hashes.flatMap({case (h,id) => List[(Hash,(Long,Int,Boolean))]((h.concat(new Hash(Array[Int](-1))),(id,grId,false)),(h.concat(new Hash(Array[Int](grouper.getGroupId(point)))),(id,grId,false)))})
          else
            hashes.flatMap({case (h,id) => specialRequests.map({case request => (h.concat(new Hash(Array[Int](request))),(id,grId,request==grId))}) ++ List[(Hash,(Long,Int,Boolean))]((h.concat(new Hash(Array[Int](grouper.getGroupId(point)))),(id,grId,false)))})
        })
        ).coalesce(data.rdd.getNumPartitions)

      //TODO Should all distances be computed? Maybe there's no point in computing them if we still don't have enough neighbors for an example
      //Should they be stored/cached? It may be enough to store a boolean that records if they have been computed. LRU Cache?

      //Groups elements mapped to the same hash
      var hashBuckets:Dataset[(Hash, List[Long], Int)]=hash.filter(r => r._1.values(r._1.values.size-1)<0)
        .map({case (h,(id,grId,searchesForSelfClass)) => (h,id)})
        .groupByKey(_._1)
        .mapGroups({case (k, l) => (k, l.map(_._2).toSet.toList)})
        .flatMap({case (k, s) => if (s.size>1) Some((k, s, s.size)) else None})
      hashBuckets=hashBuckets.coalesce(data.rdd.getNumPartitions)
      val hashBucketsNotEmpty=(!hashBuckets.isEmpty)
      if (hashBucketsNotEmpty)
      {
        numBuckets=hashBuckets.count()
        val stepOps=hashBuckets.map({case (h,s,n) => (n,1)}).groupByKey(_._1)
          .mapValues(_._2).reduceGroups(_+_)

        val numStepOps=stepOps.map({case x => x._2 * x._1 * (x._1 - 1) /2.0})
          .groupBy().agg(sum(col("value"))).as[Double].first().toLong
        val largestBucketSize=stepOps.map(_._1).groupBy().agg(max(col("value"))).as[Int].first()
        allElementsInSingleBucket=largestBucketSize==nodesLeft
        totalOps=totalOps+numStepOps

        println(f"Performing $numStepOps%g ops (largest bucket has $largestBucketSize%d elements)")

        //TODO Evaluate bucket size and increase/decrease radius without bruteforcing if necessary.

        var subgraph=getGroupedGraphFromBuckets(data, hashBuckets, numNeighbors, measurer, grouper).coalesce(data.rdd.getNumPartitions)
        fullGraph=GraphBuilder.mergeSubgraphs(fullGraph, subgraph, numNeighbors, measurer).coalesce(data.rdd.getNumPartitions)
      }
      else //DEBUG
        println("No hash buckets created")

      //Separable buckets
      //println((currentData.first()._1,currentData.first()._2._2))
      //val interestID=currentData.first()._1
      //Groups elements mapped to the same hash
      val hashSeparableBuckets:Dataset[(Hash, List[Long], List[Long])]=
      hash.filter(r => r._1.values(r._1.values.size-1)>=0)
        .map({case (h,(id,grId,searchesForSelfClass)) =>
          val requestedId=h.values(h.values.size-1)
          val leftPart=if ((requestedId!=grId) || searchesForSelfClass) List[Long](id) else List[Long]()
          val rightPart=if (requestedId==grId) List[Long](id) else List[Long]()
          (h,(leftPart,rightPart))})
        .groupByKey(_._1).mapValues(r => r._2)
        .reduceGroups({case ((y1,n1),(y2,n2)) => (y1++y2,n1++n2)}: ((List[Long], List[Long]), (List[Long], List[Long])) => (List[Long], List[Long]))
        .map({case (k, (y, n)) => (k, (y.toSet.toList, n.toSet.toList))})
        .flatMap({case (k, (y, n)) =>
          if ((y.size>0) && (n.size>0))
          {
            //if (y.contains(interestID) || n.contains(interestID))
            //  println("Kept "+k.values(k.values.size-1)+" ["+y.map(_.toString).mkString(",")+"]"+"["+n.map(_.toString).mkString(",")+"]")
            Some((k, y, n))
          }
          else
          {
            //if (y.contains(interestID) || n.contains(interestID))
            //  println("Removed "+k.values(k.values.size-1)+" ["+y.map(_.toString).mkString(",")+"]"+"["+n.map(_.toString).mkString(",")+"]")
            None
          }})
      val hashSeparableBucketsNotEmpty=(!hashSeparableBuckets.isEmpty)
      if (hashSeparableBucketsNotEmpty)
      {
        numBuckets=hashSeparableBuckets.count()
        val stepOps=hashSeparableBuckets.map({case (h,y,n) => (y.size*n.size,1)})
          .groupByKey(_._1)
          .mapValues(_._2).reduceGroups(_+_)

        val numStepOps=stepOps.map({case x => x._2 * x._1 })
          .groupBy().agg(sum(col("value"))).as[Double].first().toLong
        val largestBucketSize=stepOps.map(_._1)
          .groupBy().agg(max(col("value"))).as[Int].first()
        totalOps=totalOps+numStepOps

        println(f"Performing $numStepOps%g ops (largest separable bucket needs $largestBucketSize%d ops)")

        //TODO Evaluate bucket size and increase/decrease radius without bruteforcing if necessary.

        val subgraph=getGroupedGraphFromPartitionedBuckets(data, hashSeparableBuckets, numNeighbors, measurer, grouper).coalesce(data.rdd.getNumPartitions)
        fullGraph=GraphBuilder.mergeSubgraphs(fullGraph, subgraph, numNeighbors, measurer).coalesce(data.rdd.getNumPartitions)
      }
      else //DEBUG
        println("No separable hash buckets created")

      if (hashBucketsNotEmpty || hashSeparableBucketsNotEmpty)
      {
        //Simplify dataset
        val newData=simplifyDataset(currentData, fullGraph, numNeighbors, maxComparisonsPerItem, grouper)
        currentData.unpersist(false)
        currentData=newData.coalesce(data.rdd.getNumPartitions)

        currentData.cache()
      }

      //DEBUG
      /*println("$$$$$$$$$$$$$$$$$$$$$$$$")
      currentData.flatMap({case (id,(p,r)) =>
                              val grId=grouper.getGroupId(p)
                              r.map({case req => ((grId,req),1)})})
                .reduceByKey(_+_)
                .sortBy(_._2)
                .foreach(println)
      */

      //Remove elements that need a grId that is no longer in the data.
      val existingGrIds=currentData.map({case (id,(p,r)) => grouper.getGroupId(p)}).distinct.collect()
      currentData=currentData.rdd.map({case (id,(p,r)) =>
        val numElems=r.size
        (id,(p,r.filter(existingGrIds.contains(_)),numElems))})
        .filter({case (id,(p,r,c)) => (r.size>0) || (c==0)})
        .map({case (id,(p,r,c)) => (id,(p,r))}).toDS()

      currentData = currentData.map({ case (id, (p, r)) =>
        val numElems = r.size
        (id, (p, r.filter(existingGrIds.contains(_)), numElems))
      })
        .filter(r => (r._2._2.size > 0) || (r._2._3 == 0) )
        .map({ case (id, (p, r, c)) => (id, (p, r)) })

      //else
      //  radius*=2
      radius*=2
      nodesLeft=currentData.count()

      //DEBUG
      println(" ----- "+nodesLeft+" nodes left ("+currentData.filter(r => !r._2._2.isEmpty).count()+" with at least one group complete). Radius:"+radius)
      //currentData.map({case (id,(p,r)) => (r.size,1)}).reduceByKey(_+_).foreach(println)
      //fullGraph.foreach(println(_))
    }
    if (fullGraph!=null)
    {
      val incomplete=fullGraph.filter(r => r._2.numberOfGroupsWithAtLeastKElements(numNeighbors)<grouper.numGroups)
        .map({case (id,neighs) =>
          val incompleteGroups=grouper.getGroupIdList()
            .flatMap({case grId =>
              val neighsOfGroup=neighs.neighborsOfGroup(grId)
              if (neighsOfGroup.size<numNeighbors)
                Some(grId)
              else
                None})
          (id,incompleteGroups)
        })
      if (!incomplete.isEmpty)
      {
        //DEBUG
        //incomplete.map({case (id,(viewed,groupSizes)) => (groupSizes.sum,1)}).reduceByKey(_+_).sortBy(_._1).foreach(println)
        //incomplete.take(100).foreach({case (id,(viewed,neighborCounts)) => println(id+" -> "+viewed+" views ("+neighborCounts.mkString(";")+")")})
        println("Recovering "+incomplete.count()+" nodes that didn't have all neighbors")
        currentData=currentData.union(incomplete.joinWith(data, incomplete("_1") === data("_1"), "inner")
          .map({case ((id,requests),(_,p)) => (id,(p,requests))})).coalesce(data.rdd.getNumPartitions)

      }
    }
    if (nodesLeft>0)
    {
      //println("Elements left:"+currentData.map(_._1).collect().mkString(", "))
      if (fullGraph!=null)
      {
        /*
        //If there are any items left, look in the neighbor's neighbors.
        val neighbors:Dataset[List[Long]]=fullGraph.cartesian(currentData)
                                                   .flatMap(
                                                       {
                                                         case ((dest, (viewed,groupedNeig)), (orig,point)) =>
                                                             groupedNeig.flatMap(
                                                                 {
                                                                   case (grId,neig) =>
                                                                     if (neig.map(_._1).contains(orig))
                                                                        Some((orig,(dest :: neig.map(_._1)).toSet))
                                                                      else
                                                                        None
                                                                 }
                                                                 )
                                                        })
                                                   .reduceByKey({case (dl1,dl2) => dl1 ++ dl2})
                                                   .map({case (o,dl) => dl + o})
        */
        //The remaining points are grouped and collected.
        val remainingData=currentData.map({case (id,(point,requests)) => (grouper.getGroupId(point),List((id,requests)))})
          .groupByKey(_._1).mapValues(_._2).reduceGroups(_++_).collect()
        val bRemainingData=sparkContextSingleton.getInstance().broadcast(remainingData)
        //Get all nodes pointing to a remaining point
        val reverseNeighborPairs:Dataset[(Long,Long)]=fullGraph
          .flatMap({case (dest, neighs) =>
            val rData=bRemainingData.value
            val rDataMap=rData.toMap
            //Each neighbor group that was already in the graph is examined
            neighs.groupedNeighborLists.flatMap(
              {
                case (grId,neig) =>
                  //If we have remaining datapoints of that group
                  if (rDataMap.contains(grId))
                    rDataMap(grId).flatMap(
                      {
                        //Each datapoint of that group
                        case (idOrig,requestsOrig) =>
                          //If it is one of the neighbors for dest in the graph
                          if (neig.listNeighbors.map(_.index).contains(idOrig))
                          {
                            val srcList=if (requestsOrig.isEmpty) //Any neighbor will do, so all are selected
                              grouper.getGroupIdList()
                            else
                              requestsOrig
                            srcList.map({case reqId => (idOrig,(dest :: neighs.neighborsOfGroup(reqId).get.listNeighbors.map(_.index).filter(_!=idOrig)).toSet.toList)})
                          }
                          else
                            None
                      })
                  else
                    None
              }
            )
          }).groupByKey(_._1).mapValues(_._2)
          .reduceGroups({case (dl1,dl2) => dl1 ++ dl2}: (List[Long], List[Long]) => List[Long])
          .flatMap({case (o,dl) => dl.map((o,_))})

        val pairsWithNewNeighbors:Dataset[(Long, Long)]=currentData
          .joinWith(fullGraph, currentData("_1") === fullGraph("key"), "inner") //Joined with currentData to restrict this to only elements in the current dataset.
          .flatMap({case ((id, point),(_,groupedNeighs)) => groupedNeighs.groupedNeighborLists.flatMap({case (grId,neighs) => neighs.listNeighbors.flatMap({case dest => Some((dest.index,id))})})})
          .groupByKey(_._1).mapValues(_._2).mapGroups((k, v) => (k, v.toList))
          .joinWith(fullGraph, col("_1") === fullGraph("key"), "inner")
          .flatMap(
            {
              case ((via, ids),(_,groupedNeighs)) =>
                groupedNeighs.groupedNeighborLists.flatMap({case (grId,neighs) => neighs.listNeighbors.flatMap({case dest => ids.flatMap({case id=>if (id!=dest.index) Some((id,dest.index)) else None})})})
            })

        val totalPairs = pairsWithNewNeighbors
          .union(reverseNeighborPairs)
          .map({ case (x, y) => if (x < y) (x, y) else (y, x) })
          .groupByKey(_._1).mapGroups((k, v) => (k, v.toList.map(_._2)))
          .flatMap({ case (d, neighs) => neighs.toSet.toArray.map({ case x => (d, x) }) })

//val totalPairs = pairsWithNewNeighbors.rdd
//  .union(reverseNeighborPairs.rdd)
//  .map({ case (x, y) => if (x < y) (x, y) else (y, x) })
//  .groupByKey().flatMap({ case (d, neighs) => neighs.toSet.toArray.map({ case x => (d, x) }) }).toDS()

        val newOps=totalPairs.count()
        println("Performing "+newOps+" additional comparisons")
        totalOps=totalOps+newOps.toLong
        var subgraph=getGroupedGraphFromIndexPairs(data,
          totalPairs,
          numNeighbors,
          measurer,
          grouper)

        if (!subgraph.isEmpty)
        {
          //subgraph.foreach(println(_))
          fullGraph=GraphBuilder.mergeSubgraphs(fullGraph, subgraph, numNeighbors, measurer)
          currentData=simplifyDataset(currentData, fullGraph, numNeighbors, maxComparisonsPerItem, grouper)
          nodesLeft=currentData.count()
        }
        //println(nodesLeft+" nodes left after first attempt")
      }
      /*if (nodesLeft>0) //No solution other than to check these points with every other
      {
        val pairs=currentData.cartesian(fullGraph.map({case (point, neighbors) => point}))
        val subgraph=getGroupedGraphFromPairs(data, pairs, numNeighbors, measurer, grouper)
        fullGraph=mergeSubgraphs(fullGraph, subgraph, numNeighbors, measurer)
totalOps=totalOps+pairs.count()
      }*/
    }

    println(s"Operations wrt bruteforce: ${totalOps/bfOps} "+f"($totalOps%d total ops / ${bfOps.toLong}%d)")
    //println((totalOps/bfOps)+"#")

    return fullGraph.map({case (node, neighs) => (node,neighs.toGroupedNeighborsForElement())}).coalesce(data.rdd.getNumPartitions)
  }

  def computeGroupedGraph(data:Dataset[(Long,LabeledPoint)], numNeighbors:Int, startRadius:Option[Double], maxComparisonsPerItem:Option[Int], measurer:DistanceProvider, grouper:GroupingProvider):Dataset[(Long, GroupedNeighborsForElement)]=
  {
    val cMax=if (maxComparisonsPerItem.isDefined) math.max(maxComparisonsPerItem.get,numNeighbors) else math.max(128,10*numNeighbors)
    val factor=2.0
    val (hasher,nComps,suggestedRadius)=EuclideanLSHasher.getHasherForDataset(data, (factor*cMax).toInt)
    return computeGroupedGraph(data, numNeighbors, hasher, Some(startRadius.getOrElse(suggestedRadius)), Some(cMax.toInt), measurer, grouper)
  }

  def computeGroupedGraph(data:Dataset[(Long,LabeledPoint)], numNeighbors:Int, keyLength:Int, numTables:Int, startRadius:Option[Double], maxComparisonsPerItem:Option[Int], measurer:DistanceProvider, grouper:GroupingProvider):Dataset[(Long, GroupedNeighborsForElement)]= {
    import data.sparkSession.implicits._
    //    computeGroupedGraph(data, numNeighbors, new EuclideanLSHasher(data.map({case (index, point) => point.features.size}).reduce(Math.max(_, _)), keyLength, numTables), startRadius, maxComparisonsPerItem, measurer, grouper)
    computeGroupedGraph(data, numNeighbors, new EuclideanLSHasher(data.groupBy().agg(max(size(col("_2")))).as[Int].first(), keyLength, numTables), startRadius, maxComparisonsPerItem, measurer, grouper)
  }

  def computeGraph(data:Dataset[(Long,LabeledPoint)], numNeighbors:Int, startRadius:Option[Double], maxComparisonsPerItem:Option[Int], measurer:DistanceProvider):Dataset[(Long, NeighborsForElement)]=
  {
    val cMax=if (maxComparisonsPerItem.isDefined) math.max(maxComparisonsPerItem.get,numNeighbors) else math.max(128,10*numNeighbors)
    val factor=2.0
    val (hasher,nComps,suggestedRadius)=EuclideanLSHasher.getHasherForDataset(data, (factor*cMax).toInt)
    return computeGraph(data, numNeighbors, hasher, Some(startRadius.getOrElse(suggestedRadius)), Some(cMax.toInt), measurer)
  }

  def computeGraph(data:Dataset[(Long,LabeledPoint)], numNeighbors:Int, hasher:Hasher, startRadius:Option[Double], maxComparisonsPerItem:Option[Int], measurer:DistanceProvider):Dataset[(Long, NeighborsForElement)]=
  {
    val spark = data.sparkSession
    import spark.implicits._

    val graph=computeGroupedGraph(data, numNeighbors, hasher, startRadius, maxComparisonsPerItem, measurer, new DummyGroupingProvider())
    graph.map(
      {
        case (index, groupedNeighs) => (index, groupedNeighs.groupedNeighborLists.head._2)
      })
  }

  def computeGraph(data:Dataset[(Long,LabeledPoint)], numNeighbors:Int, hasherKeyLength:Int, hasherNumTables:Int, startRadius:Option[Double], maxComparisonsPerItem:Option[Int], measurer:DistanceProvider):Dataset[(Long, NeighborsForElement)]= {
    import data.sparkSession.implicits._

    computeGraph(data,
      numNeighbors,
      //                           new EuclideanLSHasher(data.map({case (index, point) => point.features.size}).max(), hasherKeyLength, hasherNumTables),//Get dimension from dataset
      new EuclideanLSHasher(data.groupBy().agg(max(size(col("_2")("features")))).as[Int].first(), hasherKeyLength, hasherNumTables),//Get dimension from dataset
      startRadius,
      maxComparisonsPerItem,
      measurer)
  }

  protected def splitLargeBuckets(data:Dataset[(Long,LabeledPoint)], hashBuckets:Dataset[(Hash, List[Long], Int)], maxBucketSize:Int, radius:Double, hasher:Hasher):Dataset[(Hash, List[Long], Int)]

  protected def getGroupedGraphFromBuckets(data:Dataset[(Long,LabeledPoint)], hashBuckets:Dataset[(Hash, List[Long], Int)], numNeighbors:Int, measurer:DistanceProvider, grouper:GroupingProvider):Dataset[(Long, GroupedNeighborsForElementWithComparisonCount)]

  protected def getGroupedGraphFromPartitionedBuckets(data:Dataset[(Long,LabeledPoint)], hashBuckets:Dataset[(Hash, List[Long], List[Long])], numNeighbors:Int, measurer:DistanceProvider, grouper:GroupingProvider):Dataset[(Long, GroupedNeighborsForElementWithComparisonCount)]

  protected def getGroupedGraphFromElementIndexLists(data:Dataset[(Long,LabeledPoint)], elementIndexLists:Dataset[List[Long]], numNeighbors:Int, measurer:DistanceProvider):Dataset[(Long, GroupedNeighborsForElementWithComparisonCount)]

  protected def getGroupedGraphFromPairs(data:Dataset[(Long,LabeledPoint)], pairs:Dataset[((Long, LabeledPoint), Long)], numNeighbors:Int, measurer:DistanceProvider, grouper:GroupingProvider):Dataset[(Long, GroupedNeighborsForElementWithComparisonCount)]

  //protected def getGroupedGraphFromIndexPairs(data:Dataset[(Long,LabeledPoint)], pairs:Dataset[(Long, Long)], numNeighbors:Int, measurer:DistanceProvider, grouper:GroupingProvider):Dataset[(Long, (BDV[Int],List[(Int,List[(Long, Double)])]))]

  private def simplifyDataset(dataset:Dataset[(Long,(LabeledPoint,List[Int]))], currentGraph:Dataset[(Long, GroupedNeighborsForElementWithComparisonCount)], numNeighbors:Int, maxComparisonsPerItem:Option[Int], grouper:GroupingProvider):Dataset[(Long, (LabeledPoint, List[Int]))]=
  {
    val spark = dataset.sparkSession
    import spark.implicits._

    val requestsByNodes:Dataset[(Long,Option[List[Int]])]=if (maxComparisonsPerItem.isDefined)
      currentGraph.map({case (index, neighs) =>
        if (neighs.numberOfGroupsWithAtLeastKElements(numNeighbors)>0) //There's at least one full
        {
          val incompleteGroups=neighs.getIdsOfGroupsWithLessThanKComparisons(maxComparisonsPerItem.get)
          (index,if (incompleteGroups.isEmpty) None else Some(incompleteGroups)) //None indicates that it should be removed.
        }
        else
          (index,Some(List[Int]()))
      })

    else//Remove only elements that already have all their neighbors
      currentGraph.map({case (index, neighs) =>
        val incompleteGroups=neighs.getIdsOfIncompleteGroups()
        (index,if (incompleteGroups.isEmpty) None else Some(incompleteGroups)) //None indicates that it should be removed.
      })
    dataset.joinWith(requestsByNodes, dataset("_1") === requestsByNodes("_1"), "left")
      .flatMap({case ((index, neighbors1), (_, requestsOption)) =>
      if (requestsOption == null)
        Some((index, (neighbors1._1, List[Int]())))
      else
      {
        if (requestsOption.isDefined)
          Some((index, (neighbors1._1, requestsOption.get)))
        else
          None
      }
    })
    //TODO More advanced simplifications can be done, such as removing only elements that are very "surrounded" (i.e. they landed in various large buckets)
    /*val completeNodes=if (maxComparisonsPerItem.isDefined)
                        currentGraph.filter({case (index, (viewed,groupedList)) => (viewed>maxComparisonsPerItem.get) && (groupedList.forall({case (grId,list) => list.toSet.size>=1.0}) && (groupedList.size>=grouper.numGroups))})
                      else//Remove only elements that already have all their neighbors
                        currentGraph.filter({case (index, (viewed,groupedList)) => groupedList.forall({case (grId,list) => list.toSet.size>=numNeighbors})})

    val deletableElements=completeNodes
    //Remove deletable elements from dataset
    return dataset.leftOuterJoin(deletableElements).flatMap({case (index, (neighbors1, n)) =>
                                                                if (n==None)
                                                                  Some((index, (neighbors1._1, neighbors1._2)))
                                                                else
                                                                  None
                                                                  })*/
  }
}

class LSHLookupKNNGraphBuilder(data:Dataset[(Long,LabeledPoint)]) extends LSHKNNGraphBuilder
{
  var lookup:BroadcastLookupProvider=new BroadcastLookupProvider(data)
  //TEST - Trying to broadcast a very large variable and getting memory errors. Testing this solution now.
  //var lookup:SplittedBroadcastLookupProvider=new SplittedBroadcastLookupProvider(data)

  override def splitLargeBuckets(data:Dataset[(Long,LabeledPoint)], hashBuckets:Dataset[(Hash, List[Long], Int)], maxBucketSize:Int, radius:Double, hasher:Hasher):Dataset[(Hash, List[Long], Int)]=
  {
    val spark = data.sparkSession
    import spark.implicits._

    val l=lookup
    hashBuckets.flatMap({case (k, s, n) => s.map({ x => (k,x,n) })})
      .flatMap({case(k, x, bucketSize) => if (bucketSize<maxBucketSize) Some((k,x))
      else hasher.getHashes(l.lookup(x).features, x, radius).map({case (nk,i) => (k.concat(nk),i)})}) //Concat new key
      .groupByKey(_._1).mapGroups((k, v) => (k, v.map(_._2).toSet.toList))
      .flatMap({case (k, s) => if (s.size>1) Some((k, s, s.size)) else None})
  }

  override def getGroupedGraphFromBuckets(data:Dataset[(Long,LabeledPoint)], hashBuckets:Dataset[(Hash, List[Long], Int)], numNeighbors:Int, measurer:DistanceProvider, grouper:GroupingProvider):Dataset[(Long, GroupedNeighborsForElementWithComparisonCount)]=
  {
    val spark = data.sparkSession
    import spark.implicits._

    val l=lookup
    //Discard single element hashes and for the rest get every possible pairing to build graph
    val graph=hashBuckets.filter(_._2.size>1)
      //TODO Possibly repartition after filter
      //.repartition
      .flatMap({case (hash, indices, size) =>
        //Remove duplicates from indices
        val arrayIndices=indices.toSet.toArray
        if (arrayIndices.length>1)
          BruteForceKNNGraphBuilder.computeGroupedGraph(arrayIndices, l, numNeighbors, measurer, grouper)
        else
          Nil
      })
      //Merge neighbors found for the same element in different hash buckets
      .groupByKey(_._1).mapValues(_._2).reduceGroups({case (groupedNeighbors1, groupedNeighbors2) =>
        groupedNeighbors1.addElements(groupedNeighbors2)
        groupedNeighbors1
      }: (GroupedNeighborsForElementWithComparisonCount, GroupedNeighborsForElementWithComparisonCount) => GroupedNeighborsForElementWithComparisonCount)
      .coalesce(data.rdd.getNumPartitions)
    graph
  }

  override def getGroupedGraphFromPartitionedBuckets(data:Dataset[(Long,LabeledPoint)], hashBuckets:Dataset[(Hash, List[Long], List[Long])], numNeighbors:Int, measurer:DistanceProvider, grouper:GroupingProvider):Dataset[(Long, GroupedNeighborsForElementWithComparisonCount)]=
  {
    val spark = data.sparkSession
    import spark.implicits._
    val l=lookup
    //Discard single element hashes and for the rest get every possible pairing to build graph
    val graph=hashBuckets.flatMap({case (hash, l1, l2) =>
      BruteForceKNNGraphBuilder.computeGroupedGraph(l1, l2, l, numNeighbors, measurer, grouper)
    })
      //Merge neighbors found for the same element in different hash buckets
      .groupByKey(_._1).mapValues(_._2).reduceGroups({case (neighs1, neighs2) => neighs1.addElements(neighs2)
        neighs1
      }: (GroupedNeighborsForElementWithComparisonCount, GroupedNeighborsForElementWithComparisonCount) => GroupedNeighborsForElementWithComparisonCount)
      .coalesce(data.rdd.getNumPartitions)
    graph
  }

  override def getGroupedGraphFromElementIndexLists(data:Dataset[(Long,LabeledPoint)], elementIndexLists:Dataset[List[Long]], numNeighbors:Int, measurer:DistanceProvider):Dataset[(Long, GroupedNeighborsForElementWithComparisonCount)]=
  {
    val spark = data.sparkSession
    import spark.implicits._

    val l=lookup
    //Discard single element hashes and for the rest get every possible pairing to build graph
    val graph=elementIndexLists.filter(_.size>1)
      //TODO Possibly repartition after filter
      //.repartition
      .flatMap({case (indices) =>
        //Remove duplicates from indices
        val arrayIndices=indices.toSet.toArray
        if (arrayIndices.length>1)
          BruteForceKNNGraphBuilder.computeGroupedGraph(arrayIndices, l, numNeighbors, measurer)
        else
          Nil
      })
      //Merge neighbors found for the same element in different hash buckets
      .groupByKey(_._1).mapValues(_._2).reduceGroups({case (groupedNeighbors1,groupedNeighbors2) =>
        groupedNeighbors1.addElements(groupedNeighbors2)
        groupedNeighbors1
      }: (GroupedNeighborsForElementWithComparisonCount, GroupedNeighborsForElementWithComparisonCount) => GroupedNeighborsForElementWithComparisonCount)

    graph
  }

  override def getGroupedGraphFromPairs(data:Dataset[(Long,LabeledPoint)], pairs:Dataset[((Long, LabeledPoint), Long)], numNeighbors:Int, measurer:DistanceProvider, grouper:GroupingProvider):Dataset[(Long, GroupedNeighborsForElementWithComparisonCount)]=
  {
    val spark = data.sparkSession
    import spark.implicits._

    val l=lookup
    //Discard single element hashes and for the rest get every possible pairing to build graph
    val graph=pairs.map(
      {
        case ((i1,p1),i2) =>
          val p2=l.lookup(i2)
          val newN=new NeighborsForElementWithComparisonCount(numNeighbors)
          newN.addElement(i2, measurer.getDistance(p1, p2))
          ((i1,grouper.getGroupId(p2)), newN)
      })
      //Merge neighbors found for the same element in different hash buckets
      .groupByKey(_._1).mapValues(_._2).reduceGroups({case (neighs1, neighs2) =>
        neighs1.addElements(neighs2)
        neighs1
      }: (NeighborsForElementWithComparisonCount, NeighborsForElementWithComparisonCount) => NeighborsForElementWithComparisonCount)
      .map(
        {
          case ((i1,grId2),neighborList) =>
            val grNeighs=GroupedNeighborsForElementWithComparisonCount.newEmpty(grouper.getGroupIdList(),numNeighbors)
            grNeighs.addElementsOfGroup(grId2, neighborList, neighborList.comparisons)
            (i1,grNeighs)
        }
      )
      .groupByKey(_._1).mapValues(_._2).reduceGroups(
        {
          case (neighs1,neighs2) =>
            neighs1.addElements(neighs2)
            neighs1
        }: (GroupedNeighborsForElementWithComparisonCount, GroupedNeighborsForElementWithComparisonCount) => GroupedNeighborsForElementWithComparisonCount
      )

    graph
  }

  override def getGroupedGraphFromIndexPairs(data:Dataset[(Long,LabeledPoint)], pairs:Dataset[(Long, Long)], numNeighbors:Int, measurer:DistanceProvider, grouper:GroupingProvider):Dataset[(Long, GroupedNeighborsForElementWithComparisonCount)]=
  {
    val spark = data.sparkSession
    import spark.implicits._
    val l=lookup
    //Discard single element hashes and for the rest get every possible pairing to build graph
    val graph=pairs.flatMap({case (i1,i2) =>
      var p1=l.lookup(i1)
      var p2=l.lookup(i2)
      val d=measurer.getDistance(p1, p2)
      val grN1=GroupedNeighborsForElementWithComparisonCount.newEmpty(grouper.getGroupIdList(),numNeighbors)
      grN1.addElementOfGroup(grouper.getGroupId(p2), i2, d)
      val grN2=GroupedNeighborsForElementWithComparisonCount.newEmpty(grouper.getGroupIdList(),numNeighbors)
      grN2.addElementOfGroup(grouper.getGroupId(p1), i1, d)
      List[((Long,Int),GroupedNeighborsForElementWithComparisonCount)](((i1,grouper.getGroupId(p2)), grN1),((i2,grouper.getGroupId(p1)), grN2))
    })
      //Merge neighbors found for the same element in different hash buckets
      .groupByKey(_._1).mapValues(_._2).reduceGroups({case (neigh1, neigh2) =>
        neigh1.addElements(neigh2)
        neigh1
      }: (GroupedNeighborsForElementWithComparisonCount, GroupedNeighborsForElementWithComparisonCount) => GroupedNeighborsForElementWithComparisonCount)
      .map(
        {
          case ((i1,grId2),neighs) => (i1,neighs)
        }
      )
      .groupByKey(_._1).mapValues(_._2).reduceGroups(
        {
          case (neighs1, neighs2) =>
            neighs1.addElements(neighs2)
            neighs1
        }: (GroupedNeighborsForElementWithComparisonCount, GroupedNeighborsForElementWithComparisonCount) => GroupedNeighborsForElementWithComparisonCount
      )

    graph
  }
}