package es.udc.graph

import org.apache.spark.SparkContext
import org.apache.spark.sql.SparkSession

object sparkContextSingleton {
    /*@transient private var instance: SparkContext = _
    private val conf : SparkConf = new SparkConf().setAppName("KNiNe")
                                                  .setMaster("local[8]")
                                                  //.set("spark.serializer", "org.apache.spark.serializer.KryoSerializer")
                                                  //.set("spark.broadcast.factory", "org.apache.spark.broadcast.HttpBroadcastFactory")
                                                  //.set("spark.kryoserializer.buffer.max", "512")
                                                  //.set("spark.driver.maxResultSize", "2048")
    */

  def getSparkSession(): SparkSession = {
    SparkSession.builder //.appName("KNiNe")
      .master("local[1]")
      //.config("spark.driver.maxResultSize", "2048MB")
      .getOrCreate()
  }
  def getInstance(): SparkContext = {
      val spark = SparkSession.builder //.appName("KNiNe")
        .master("local[1]")
        //.config("spark.driver.maxResultSize", "2048MB")
        .getOrCreate()
      /*if (instance == null)
        instance = SparkContext.getOrCreate(conf)//new SparkContext(conf)
      instance*/
      spark.sparkContext
    }
}
