package es.udc.graph


import es.udc.graph.utils.SourceUnit
import org.apache.spark.sql.{DataFrame, SparkSession}

object KNiNeMain{
  private val DEFAULT_METHOD = "vrlsh"
  private val DEFAULT_K = 10
  private val DEFAULT_REFINEMENT = 1
  private val DEFAULT_BLOCKSZ: Int = 100
  private val DEFAULT_ITERATIONS: Int = 1
  private val DEFAULT_NUM_TABLES: Double = 0
  private val DEFAULT_KEY_LENGTH: Double = 0
  private val DEFAULT_SPARK_API = "dfds"
  val DEFAULT_NUM_PARTITIONS: Double = 512
  val DEFAULT_RADIUS_START = 0.1

  def main(args: Array[String]) {
    //      todo # calc outputPartitionNumber based on input size
    import mllib.KNiNe

    val outputPartitionNumber = 1

    if (args.length <= 0) {
      showUsageAndExit()
    }

    val options: Map[String, Any] = parseParams(args) //  ++ Map("num_tables" -> 5d, "key_length" -> 5d)

    val datasetFile = options("dataset").asInstanceOf[String]

    val numNeighbors = options("num_neighbors").asInstanceOf[Double].toInt
    val numPartitions = options("num_partitions").asInstanceOf[Double].toInt
    val method = options("method").asInstanceOf[String]
    val sparkApi = options("spark_api").asInstanceOf[String]
    val format = if ((datasetFile.length() > 7) && (datasetFile.substring(datasetFile.length() - 7) == ".libsvm"))
      "libsvm"
    else
      "text"


    implicit val spark = SparkSession.builder.appName("KNiNe")
      .master("local[1]")
      .config("spark.driver.maxResultSize", "2048MB")
      .getOrCreate()

    spark.sqlContext.setConf("spark.sql.shuffle.partitions", s"$numPartitions")
    spark.sqlContext.setConf("spark.default.parallelism", s"$numPartitions")


    val data: DataFrame = (if (format == "libsvm") {
      SourceUnit.readInputLibsvm(datasetFile)
    } else {
      SourceUnit.readInputTxt(datasetFile)

    }).repartition(numPartitions)


    System.out.println("------------------------------")
    System.out.println(s"$options, $format, $datasetFile, $numPartitions, $method, $numNeighbors")

    val kNiNeConf = KNiNeConfiguration.getConfigurationFromOptions(options)


    var edges: Option[DataFrame] = None
    var edgesRList: Seq[DataFrame] = Seq.empty[DataFrame]

    if (sparkApi == "rdd"){
      val (edgesRDD, edgesRRDDList) = mllib.KNiNe.process(kNiNeConf, format, datasetFile, numPartitions, method, numNeighbors)
      edges = Some(SourceUnit.distanceRDDToDataFrame(edgesRDD.get))
      edgesRList = edgesRRDDList.map(SourceUnit.distanceRDDToDataFrame)
    }
    else {
      val (edgesDf, edgesRListDf) = ml.KNiNe.process(kNiNeConf, data, numPartitions, method, numNeighbors)
      edges = edgesDf
      edgesRList = edgesRListDf
    }

    val fileName = options("output").asInstanceOf[String]
    if(edges.isDefined)
      edges.get.coalesce(outputPartitionNumber)
        .toDF("id1", "id2", "distance")
        .sort("id1", "distance")
        .write.mode("overwrite").csv(fileName)

    //    edgesRList.zipWithIndex.foreach(edgesR => {
    //            edgesR._1.coalesce(outputPartitionNumber)
    //              .toDF("id1", "id2", "distance")
    //              .write.csv(fileName+"refined"+edgesR._2)
    //    })



    //    if (compareFile != null) {
    //      //Compare with ground truth
    //      CompareGraphs.printResults(CompareGraphs.compare(compareFile, fileName, None))
    //CompareGraphs.comparePositions(compareFile.replace(numNeighbors+"", "128"), fileName)

    //Compare refined with ground truth

    //      if (fileName != fileNameR)
    //        CompareGraphs.printResults(CompareGraphs.compare(compareFile, fileNameR, None))
    //CompareGraphs.comparePositions(compareFile.replace(numNeighbors+"", "128"), fileName)

    spark.stop()
  }

  def parseParams(p: Array[String]): Map[String, Any] = {
    val m = scala.collection.mutable.Map[String, Any]("num_neighbors" -> DEFAULT_K.toDouble,
      "method" -> DEFAULT_METHOD,
      "radius_start" -> DEFAULT_RADIUS_START,
      "num_partitions" -> DEFAULT_NUM_PARTITIONS,
      "refine" -> DEFAULT_REFINEMENT,
      "spark_api" -> DEFAULT_SPARK_API,
      "key_length" -> DEFAULT_KEY_LENGTH,
      "num_tables" -> DEFAULT_NUM_TABLES)
    if (p.length <= 1)
      showUsageAndExit()

    m("dataset") = p(0)
    m("output") = p(1)

    var i = 2
    while (i < p.length) {
      if ((i >= p.length) || (p(i).charAt(0) != '-')) {
        println("Unknown option: " + p(i))
        showUsageAndExit()
      }
      val readOptionName = p(i).substring(1)
      val option = readOptionName match {
        case "k" => "num_neighbors"
        case "m" => "method"
        case "r" => "radius_start"
        case "n" => "num_tables"
        case "l" => "key_length"
        case "t" => "max_comparisons"
        case "c" => "compare"
        case "p" => "num_partitions"
        case "d" => "refine"
        case "b" => "blocksz"
        case "i" => "iterations"
        case "a" => "spark_api"
        case somethingElse => readOptionName
      }
      if (!m.keySet.exists(_ == option) && option == readOptionName) {
        println("Unknown option:" + readOptionName)
        showUsageAndExit()
      }
      if (option == "method") {
        if (p(i + 1) == "vrlsh" || p(i + 1) == "brute" || p(i + 1) == "fastKNN-proj" || p(i + 1) == "fastKNN-AGH")
          m(option) = p(i + 1)
        else {
          println("Unknown method:" + p(i + 1))
          showUsageAndExit()
        }
      }
      else if (option == "spark_api") {
        if (p(i + 1) == "rdd" || p(i + 1) == "dfds")
          m(option) = p(i + 1)
        else {
          println("Unknown spark_api:" + p(i + 1))
          showUsageAndExit()
        }
      }
      else if (option == "compare")
        m(option) = p(i + 1)
      else if ((option == "refine") || (option == "blocksz") || (option == "iterations") ||
          (option == "key_length") || (option == "num_tables"))
        m(option) = p(i + 1).toInt
      else
        m(option) = p(i + 1).toDouble


      i = i + 2
    }
    return m.toMap
  }

  def showUsageAndExit() = {
    println(
      """Usage: KNiNe dataset output_file [options]
    Dataset must be a libsvm or text file
Options:
    -k    Number of neighbors (default: """ + DEFAULT_K +
        """)
    -m    Method used to compute the graph. Valid values: vrlsh, brute, fastKNN-proj, fastKNN-AGH (default: """ + DEFAULT_METHOD +
        """)
    -r    Starting radius (default: """ + DEFAULT_RADIUS_START +
        """)
    -t    Maximum comparisons per item (default: auto)
    -c    File containing the graph to compare to (default: nothing)
    -p    Number of partitions for the data RDDs (default: """ + DEFAULT_NUM_PARTITIONS +
        """)
    -d    Number of refinement (descent) steps (LSH only) (default: """ + DEFAULT_REFINEMENT +
        """)
    -b    blockSz (fastKNN only) (default: """ + DEFAULT_BLOCKSZ +
        """)
    -i    iterations (fastKNN only) (default: """ + DEFAULT_ITERATIONS +
        """)
    -a    Spark API used. Valid values: rdd, dfds (default: """ + DEFAULT_SPARK_API +
        """)

Advanced LSH options:
    -n    Number of hashes per item (default: auto)
    -l    Hash length (default: auto)

""")
    System.exit(-1)
  }
  object KNiNeConfiguration
  {
    def getConfigurationFromOptions(options:Map[String, Any]):KNiNeConfiguration=
    {
      val radius0=if (options.exists(_._1=="radius_start"))
        Some(options("radius_start").asInstanceOf[Double])
      else
        None
      val numTables=if (options.exists(_._1=="num_tables"))
        Some(options("num_tables").asInstanceOf[Double].toInt)
      else
        None
      val keyLength=if (options.exists(_._1=="key_length"))
        Some(options("key_length").asInstanceOf[Double].toInt)
      else
        None
      val maxComparisons=if (options.exists(_._1=="max_comparisons"))
        Some(options("max_comparisons").asInstanceOf[Double].toInt)
      else
        None
      val blockSz=if (options.exists(_._1=="blocksz"))
        options("blocksz").asInstanceOf[Int]
      else
        DEFAULT_BLOCKSZ
      val iterations=if (options.exists(_._1=="iterations"))
        options("iterations").asInstanceOf[Int]
      else
        DEFAULT_ITERATIONS
      return new KNiNeConfiguration(numTables, keyLength, maxComparisons, radius0, options("refine").asInstanceOf[Int], blockSz, iterations)
    }
  }

  class KNiNeConfiguration(val numTables:Option[Int], val keyLength:Option[Int], val maxComparisons:Option[Int], val radius0:Option[Double], val refine:Int, val blockSz:Int, val iterations:Int)
  {
    def this() = this(None, None, None, None, DEFAULT_REFINEMENT, DEFAULT_BLOCKSZ, DEFAULT_ITERATIONS)
    override def toString():String=
    {
      return "R0="+this.radius0+";NT="+this.numTables+";KL="+this.keyLength+";MC="+this.maxComparisons+";Refine="+this.refine
    }
  }
}
