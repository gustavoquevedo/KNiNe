package es.udc.graph.ml

import breeze.linalg.{DenseVector => BDV}
import es.udc.graph.ml.entities.{GroupedNeighborsForElement, GroupedNeighborsForElementWithComparisonCount, NeighborsForElement, NeighborsForElementWithComparisonCount, WrappedUngroupedNeighborsForElementWithComparisonCount}
import es.udc.graph.ml.entities.IndexDistancePair
import es.udc.graph.mllib.{DistanceProvider, DummyGroupingProvider, GroupingProvider}
import org.apache.spark.ml.feature.LabeledPoint
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.{Dataset, SparkSession}
import org.apache.spark.{HashPartitioner, SparkContext}

import java.io.File
import scala.Array._

object GraphBuilder
{
  def mergeSubgraphs(g1:Dataset[(Long, GroupedNeighborsForElementWithComparisonCount)], g2:Dataset[(Long, GroupedNeighborsForElementWithComparisonCount)], numNeighbors:Int, measurer:DistanceProvider):Dataset[(Long, GroupedNeighborsForElementWithComparisonCount)]=
  {
    if (g1==null) return g2
    if (g2==null) return g1
    import g1.sparkSession.implicits._

    g1.union(g2).groupByKey(_._1).mapValues(_._2).reduceGroups((groupedNeighbors1, groupedNeighbors2) => {
                                                      groupedNeighbors1.addElements(groupedNeighbors2)
                                                      groupedNeighbors1}).map(r => (r._1, r._2)).toDF("key", "values").as[(Long, GroupedNeighborsForElementWithComparisonCount)]
  }

  def readFromFiles(prefix:String)(implicit spark: SparkSession):Dataset[(Long, GroupedNeighborsForElement)]=
  {
    println(s"Reading files from $prefix")
    //Get list of files involved
    import spark.implicits._
    val sc = spark.sparkContext

    val dirPath=prefix.substring(0,prefix.lastIndexOf("/")+1)
    val d = new File(dirPath)
    val allFilesInDir=
      if (d.exists && d.isDirectory) {
          d.listFiles.filter(_.isFile).toList
      } else {
          List[File]()
      }
    assert(!allFilesInDir.isEmpty)
    val matchingFiles=allFilesInDir.filter(
        {case f =>
            val sf=f.toPath().toString()
            sf.length()>prefix.length() && sf.substring(0,prefix.length())==prefix &&
            sf.endsWith(".txt")
            })
    val pattern=""".*c(\d+).txt""".r
    val rdds=matchingFiles.map(
        {case f =>
           val fs=f.toPath().toString()
           val groupId=fs match {case pattern(grId) => grId.toInt
                                 case default => 0
                                 }
           sc.textFile(fs)
             .map({case line =>
                     val parts=line.substring(1,line.length()-1).split(",")
                     (parts(0).toLong,IndexDistancePair(parts(1).toLong,parts(2).toDouble))
                 })
             .groupByKey()
             .map({case (id, nList) =>
                         val neighs=new NeighborsForElement(nList.size)
                         neighs.addElements(nList.toList)
                         (id, (groupId, neighs))})
         })
     val fullDataset=if (rdds.size>1)
                   sc.union(rdds)
                 else
                   rdds(0)
     println(fullDataset.count())
     val numNeighbors=fullDataset.map({case (id,(grId,neighs)) => neighs.listNeighbors.size}).max
     val groupIdList=matchingFiles.map(
        {case f =>
           val fs=f.toPath().toString()
           fs match {case pattern(grId) => grId.toInt
                     case default => 0}
        })
     fullDataset.groupByKey()
            .map({case (id,groupedList) =>
                      val map=scala.collection.mutable.Map(groupedList.toSeq : _*)
                      (id, new GroupedNeighborsForElement(map,groupIdList,numNeighbors))}).toDS()
  }
}

abstract class GraphBuilder
{
  /**
 * @param g
 * @return Resulting graph of searching for closer neighbors in the neighbors of the neighbors for each node
 */
  /*private */def refineGraph(data:Dataset[(Long,LabeledPoint)], g:Dataset[(Long, NeighborsForElementWithComparisonCount)], numNeighbors:Int, measurer:DistanceProvider):Dataset[(Long, NeighborsForElementWithComparisonCount)]=
  {
    import data.sparkSession.implicits._

    val graph=refineGroupedGraph(data, g.map({case (index, neighs) => (index,neighs.asGroupedWithCounts())}), numNeighbors, measurer, new DummyGroupingProvider())
    graph.map({case (i1, groupedNeighbors) => (i1, WrappedUngroupedNeighborsForElementWithComparisonCount.unwrap(groupedNeighbors))})
  }

  /*private */def refineGroupedGraph(dataDS:Dataset[(Long,LabeledPoint)], gDS:Dataset[(Long, GroupedNeighborsForElementWithComparisonCount)], numNeighbors:Int, measurer:DistanceProvider, grouper:GroupingProvider):Dataset[(Long, GroupedNeighborsForElementWithComparisonCount)]=
  {
    import dataDS.sparkSession.implicits._

    val gDSNaig = gDS.flatMap(
      {
        case (node, neighbors) =>
          val groupedNeighborLists = neighbors.groupedNeighborLists
          val allNeighbors = groupedNeighborLists.flatMap(_._2.listNeighbors.map(_.index))
          groupedNeighborLists.flatMap(
            {
              case (grId, neighs) => neighs.listNeighbors.map({ case pair => (pair.index, node) })
            })
            .map({ case (dest, node) => (dest, (node, allNeighbors)) })
      }).coalesce(dataDS.rdd.getNumPartitions)

    var pairsWithNewNeighbors = gDSNaig.joinWith(gDS, gDSNaig("_1") === gDS("_1"), "inner")
      .flatMap({ case ((dest, (node, neighbors)), (_, neighsGroupedNeighs)) => neighsGroupedNeighs.groupedNeighborLists.flatMap(
      {
        case (grId, neighsNeighs) =>
          neighsNeighs.listNeighbors.flatMap(
            { case pair => if ((node != pair.index) && (!neighbors.contains(pair.index)))
              Some((node, pair.index))
            else
              None
            })
      })
    })
      .map({ case (x, y) => if (x < y) (x, y) else (y, x) })

    pairsWithNewNeighbors=pairsWithNewNeighbors.groupByKey(_._1).mapValues(_._2).flatMapGroups({case (d, neighs) => neighs.toSet.toArray.map({case x => (d, x)})})
    val totalElements=dataDS.count()
    val bfOps:Double=totalElements*(totalElements-1)/2.0
    val stepOps=pairsWithNewNeighbors.count()
    println("Refining step takes "+stepOps+" comparisons ("+(stepOps/bfOps)+" wrt bruteforce)")
    var subgraph=getGroupedGraphFromIndexPairs(dataDS,
                                        pairsWithNewNeighbors,
                                        numNeighbors,
                                        measurer,
                                        grouper)
    GraphBuilder.mergeSubgraphs(gDS, subgraph, numNeighbors, measurer)
  }
  protected def getGroupedGraphFromIndexPairs(data:Dataset[(Long,LabeledPoint)], pairs:Dataset[(Long, Long)], numNeighbors:Int, measurer:DistanceProvider, grouper:GroupingProvider):Dataset[(Long, GroupedNeighborsForElementWithComparisonCount)]
}









