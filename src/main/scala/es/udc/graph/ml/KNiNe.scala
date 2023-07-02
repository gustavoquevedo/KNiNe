package es.udc.graph.ml


import es.udc.graph.KNiNeMain.KNiNeConfiguration
import es.udc.graph.utils.SourceUnit
import org.apache.log4j.{Level, Logger}
import org.apache.spark.sql.{DataFrame, SparkSession}
import org.apache.spark.sql.functions._

object KNiNe
{
  def process(kNiNeConf: KNiNeConfiguration, data: DataFrame, numPartitions: Integer,
              method: String, numNeighbors: Int)(implicit spark: SparkSession): (Option[DataFrame], Seq[DataFrame]) = {
    import spark.implicits._

    //println("Using "+method+" to compute "+numNeighbors+"NN graph for dataset "+justFileName)
    //println("R0:"+radius0+(if (numTables!=null)" num_tables:"+numTables else "")+(if (keyLength!=null)" keyLength:"+keyLength else "")+(if (maxComparisons!=null)" maxComparisons:"+maxComparisons else ""))

    //Set up Spark Context
    println(s"Default parallelism: ${spark.sparkContext.defaultParallelism}")
    //Stop annoying INFO messages
    val rootLogger = Logger.getRootLogger()
    rootLogger.setLevel(Level.WARN)

    val dataDS = SourceUnit.dataFrameInputToDataset(data)
    val dataRDD = dataDS.rdd
    /* DATASET INSPECTION - DEBUG
    val summary=data.map({case x => (x._1.features.toArray,x._1.features.toArray,x._1.features.toArray)}).reduce({case ((as,aM,am),(bs,bM,bm)) => (as.zip(bs).map({case (ea,eb) => ea+eb}),aM.zip(bM).map({case (ea,eb) => Math.max(ea,eb)}),am.zip(bm).map({case (ea,eb) => Math.min(ea,eb)}))})
    val total=data.count()
    val medias=summary._1.map({ x => x/total })
    val spans=summary._2.zip(summary._3).map({case (a,b) => (a-b)})
    println(Vectors.dense(medias))
    println(Vectors.dense(spans))
    val stddevs=data.map(_._1.features.toArray.zip(medias).map({case (x,u) => (x-u)*(x-u) })).reduce({case (a,b) => a.zip(b).map({case (ea,eb) => ea+eb})}).map({ x => Math.sqrt(x/total) })
    println(Vectors.dense(stddevs))
    println(stddevs.max)
    println(stddevs.min)
    println(stddevs.sum/stddevs.length)
    System.exit(0)
    */

    //val n=data.count()
    //println("Dataset has "+n+" elements")

    /* GRAPH VERSION

    val graph=LSHGraphXKNNGraphBuilder.getGraph(data, numNeighbors, dimension)
    println("There goes the graph:")
    graph.foreach(println(_))

    */


    //EuclideanLSHasher.getBucketCount(data.map(_.swap), hasher, radius)
    //System.exit(0)


    val timeStart = System.currentTimeMillis();
    var builder: GraphBuilder = null
    val (graph, lookup) = method match {
      case "fastKNN-AGH" =>
        val kLength = data
          .withColumn("label_sqrt", log(2, 'label.cast("Int")))
          .groupBy().agg(max('label_sqrt).cast("Integer")).as[Integer].first()

        println(s"Method: fastKNN with AGH (must be precomputed on dataset labels) as LSH. BlockSz=${kNiNeConf.blockSz} Iterations=${kNiNeConf.iterations}  keyLength=$kLength")
        builder = new SimpleLSHLookupKNNGraphBuilder(dataDS)
        (builder.asInstanceOf[SimpleLSHLookupKNNGraphBuilder].iterativeComputeGraph(dataDS, numNeighbors, kLength, 0, new EuclideanDistanceProvider(), Some(kNiNeConf.blockSz), kNiNeConf.iterations, true), builder.asInstanceOf[SimpleLSHLookupKNNGraphBuilder].lookup)
      case "fastKNN-proj" =>
        println(s"Method: fastKNN with random projections as LSH. BlockSz=${kNiNeConf.blockSz} KeyLength=${kNiNeConf.keyLength.get}  NumTables=${kNiNeConf.numTables.get} Iterations=${kNiNeConf.iterations}")
        builder = new SimpleLSHLookupKNNGraphBuilder(dataDS)
        (builder.asInstanceOf[SimpleLSHLookupKNNGraphBuilder].iterativeComputeGraph(dataDS, numNeighbors, kNiNeConf.keyLength.get, kNiNeConf.numTables.get, new EuclideanDistanceProvider(), Some(kNiNeConf.blockSz), kNiNeConf.iterations), builder.asInstanceOf[SimpleLSHLookupKNNGraphBuilder].lookup)
      case "vrlsh" =>
        /* LOOKUP VERSION */
        builder = new LSHLookupKNNGraphBuilder(dataDS)
        if (kNiNeConf.keyLength.isDefined && kNiNeConf.numTables.isDefined)
          (builder.asInstanceOf[LSHLookupKNNGraphBuilder].computeGraph(dataDS, numNeighbors, kNiNeConf.keyLength.get, kNiNeConf.numTables.get, kNiNeConf.radius0, kNiNeConf.maxComparisons, new EuclideanDistanceProvider()), builder.asInstanceOf[LSHLookupKNNGraphBuilder].lookup)
        else {
          //val cMax=if (kNiNeConf.maxComparisons>0) kNiNeConf.maxComparisons else 250
          val cMax = if (kNiNeConf.maxComparisons.isDefined) math.max(kNiNeConf.maxComparisons.get, numNeighbors) else math.max(128, 10 * numNeighbors)
          //val factor=if (options.contains("fast")) 4.0 else 0.8
          val factor = 2.0
          val (hasher, nComps, suggestedRadius) = EuclideanLSHasher.getHasherForDataset(dataDS, (cMax * factor).toInt) //Make constant size buckets
          (builder.asInstanceOf[LSHLookupKNNGraphBuilder].computeGraph(dataDS, numNeighbors, hasher, Some(suggestedRadius), Some(cMax.toInt), new EuclideanDistanceProvider()), builder.asInstanceOf[LSHLookupKNNGraphBuilder].lookup)
        }
      case somethingElse =>
        /* BRUTEFORCE VERSION */
        BruteForceKNNGraphBuilder.parallelComputeGraph(dataDS, numNeighbors, numPartitions)
    }


    //Print graph
    /*println("There goes the graph:")
    graph.foreach({case (elementIndex, neighbors) =>
                    for(n <- neighbors)
                      println(elementIndex+"->"+n._1+"("+n._2+")")
                  })
    */

    //

    //DEBUG
    //var counted=edges.map({case x=>(x._1,1)}).reduceByKey(_+_).sortBy(_._1)
    //var forCount=counted.map(_._2)

    var countEdges = graph.rdd.map({ case (index, neighbors) => neighbors.listNeighbors.toSet.size }).sum
    println("Obtained " + countEdges + " edges for " + graph.count() + " nodes in " + (System.currentTimeMillis() - timeStart) + " milliseconds")

    // DEBUG - Skip save
    val skipSave = false
    val edges = if (skipSave) None else Some({
      //        var i=0
      //        while (java.nio.file.Files.exists(java.nio.file.Paths.get(fileName.substring(7))))
      //        {
      //          i=i+1
      //          fileName=fileNameOriginal+"-"+i
      //        }
      graph
        .flatMap({ case (index, neighbors) => neighbors.listNeighbors
          .map({ case destPair => (index, destPair.index, math.sqrt(destPair.distance)) }).toSet
        })
    })

    val edgesRList = if (method == "brute") Nil else {
      var refinedGraph = graph.map({ case (v, neighs) => (v, neighs.wrapWithCount(1)) })
      (0 to kNiNeConf.refine).map(i => {
        println(s"Performing neighbor descent step ${i + 1}")
        val timeStartR = System.currentTimeMillis();
        refinedGraph = builder.refineGraph(dataDS, refinedGraph, numNeighbors, new EuclideanDistanceProvider())
        //        val fileNameR=fileName+"refined"+i
        val edgesR = refinedGraph.flatMap({ case (index, neighs) =>
          neighs.listNeighbors.map({ case destPair =>
            (index, destPair.index, math.sqrt(destPair.distance))
          }).toSet
        })
        //TODO - Move sqrt in previous line to graph class.
        val totalElements = data.count()
        val e = edgesR.first()
        println("Added " + (System.currentTimeMillis() - timeStartR) + " milliseconds")

        edgesR
      })
    }

    (edges.map(SourceUnit.distanceDatasetToDataFrame) ,
      edgesRList.map(SourceUnit.distanceDatasetToDataFrame))
//    (None, Nil)
  }
}
