package main.scala

import org.apache.spark._
import org.apache.spark.SparkContext
import org.apache.spark.SparkContext._
import org.apache.spark.SparkConf
import org.apache.spark.{SparkConf, Logging}
import org.apache.spark.sql.SQLContext

import org.apache.spark.mllib.clustering._
import org.apache.spark.mllib.clustering.KMeans
import org.apache.spark.mllib.clustering.KMeansModel
import org.apache.spark.mllib.clustering.StreamingKMeans
import org.apache.spark.mllib.linalg.Vectors
import org.apache.spark.mllib.linalg.{Matrix, Vector}
import org.apache.spark.mllib.regression.LabeledPoint

import org.apache.spark.rdd.RDD
import org.apache.spark.rdd._
import org.apache.spark.streaming._
import org.apache.spark.streaming.dstream.DStream
import org.apache.spark.streaming.{Duration, Time}
import org.apache.spark.streaming.{Seconds, StreamingContext}
 
import scala.util.Random.nextDouble
import scala.util.Random.nextGaussian
import java.io._
import com.google.gson.{GsonBuilder, JsonParser}

/* Student: Linnea Strågefors
 * Date: 2016-01-12
 * Course: Social Media Echosystems (4ME304)
 * 
 * This application contains the source code for Assignment 3. 
 */

object SparkApplication {
  def main(args: Array[String]) {
    
    val sparkConf = new SparkConf().setAppName("stragefors_a3").setMaster("local[*]")
    val sc = new SparkContext(sparkConf)
    
    // ASSIGNMNENT TASK 1: Collecting data
    /* create an RDD of local tweets from the file tweets.json
     * (The tweets were posted between 15:39:49 - 15:49:56, Fri Nov 20 2015)
     */
    // Data source: tweets.json
    val rdd = sc.textFile("file:///Users/Linnea/Documents/Medietkn/HT15/LP2 Soc media echosyst/Assignments/A3/stragefors_A3/SparkApplication/src/main/resources/tweets.json")
    println("Number of all tweets: " + rdd.count()) //36148
    
    // ASSIGNMENT TASK 2: Cleaning and preparing the dataset
    /* Clean the data-set
     * Filter the rdd to to only show tweets starting with {"created_at. 
     * Store cleaned data in a new rdd
     */
    //Input: tweets.json
    val linesWithCreatedAt =  rdd.filter(line => line.startsWith("""{"created_at"""))
    println("Number tweets starting with '{created at': " + linesWithCreatedAt.count() ) //32152
    linesWithCreatedAt.take(1).foreach(println) // show first line starting with {"created_at"
    
    //Output: createdAt_tweets.json
    //store the tweets starting with {"created_at" in directory /resources/createdAt_tweets
    linesWithCreatedAt.saveAsTextFile("file:///Users/Linnea/Documents/Medietkn/HT15/LP2 Soc media echosyst/Assignments/A3/stragefors_A3/SparkApplication/src/main/resources/createdAt_tweets")

    //Selecting features with Spark SQL
    /* Feature 1: friends_count
     * Feature 2: followers_count
     */
    //Input: createdAt_tweets.json    
    //val createdAtTweets = sc.textFile("file:///Users/Linnea/Documents/Medietkn/HT15/LP2 Soc media echosyst/Assignments/A3/stragefors_A3/SparkApplication/src/main/resources/createdAt_tweets")
    
    val sqlContext = new org.apache.spark.sql.SQLContext(sc)
    import sqlContext.implicits._
    val tweetTable = sqlContext.read.json(linesWithCreatedAt).cache()

    tweetTable.registerTempTable("tweetTable")

    //print tweetTable Schema
    //tweetTable.printSchema()
    
    //Select the features
    val featuresDataset = sqlContext.sql("SELECT user.friends_count, user.followers_count FROM tweetTable")
    
    //clean features from null values, replacing with 0.0
    val featuresNoNull =  featuresDataset.na.fill(0.0)
    
    //write to CSV format
    val featuresCSV = featuresNoNull.map(t => t(0)+","+t(1))
    
    //featuresCSV.collect().take(3).foreach(println) // check CSV format: 74,371 
    
    // Output: cluster_tweets.json
    // save the data-set with the features into /resources/cluster_tweets, in json-format
    featuresCSV.saveAsTextFile("/Users/Linnea/Documents/Medietkn/HT15/LP2 Soc media echosyst/Assignments/A3/stragefors_A3/SparkApplication/src/main/resources/cluster_tweets")
    println("Number of selected tweets with the features: "+featuresCSV.count()) //32152, all tweets
    
    //ASSIGNMENT TASK 3: Choose two clustering algorithms from Spark MLlib
    /* Input: cluster_tweets.json
     * Algorithm 1: K-means algorithm
     * Algoritm 2: Gaussian Mixture. This is implemented in SparkApplication3.scala
     */
    
    val nrClusters = 5
    val nrIterations = 20
    
    //load and parse the data
    val parsedData = featuresCSV.map(s => Vectors.dense(s.split(",").map(_.toDouble)))
    parsedData.cache()
    
    //Algorithm 1: K-means
    val kmModel = KMeans.train(parsedData, nrClusters, nrIterations)

    val objRDD = sc.makeRDD(kmModel.clusterCenters, nrClusters)
    
    //Output: the cluster model in the directory clusterModel1
    //save clusters to /resources/clusterModel1
    val outputPath1 = "/Users/Linnea/Documents/Medietkn/HT15/LP2 Soc media echosyst/Assignments/A3/stragefors_A3/SparkApplication/src/main/resources/clusterModel1"
    objRDD.saveAsTextFile(outputPath1)
        
    kmModel.clusterCenters.foreach(println) //print the 5 cluster centers
    
    //Test samples: print max 10 feature-sets in each cluster
    //The samples are collected from the whole dataset of about 32151 tweets
    val test_tweets = featuresCSV.take(32151)
   
    var nr = 1 //number of tweets counter
    def f(y: Int) = { nr += y; nr} //increment the counter
    
    for (i <- 0 until nrClusters) {
      println(s"\nCLUSTER $i:")
      nr=1 
      
      test_tweets.foreach { t =>
        if(nr <= 10){
            if (kmModel.predict(Vectors.dense(t.split(",").map(_.toDouble))) == i) {
              println(t) //74,371
              f(1) //increment counter by 1
          }
        }
      
        }   
    }    
    
    //Algorithm 2: Gaussian mixture    
    //Source code is implemented in SparkApplication2.scala
    
    
    //ASSIGNMENT TASK 4: Apply the model in real-time by using Spark Streaming API
    //Initializing Spark Streaming Context
    val ssc = new StreamingContext(sc, Seconds(1))

    val pathTestData = "file:///Users/Linnea/Documents/Eclipse_medietkn/SparkApplication/src/main/resources/testData"
    val dataStream = ssc.textFileStream(pathTestData)
   
    //Output the results in clustering_result.csv
    val writer = new PrintWriter(new File("clustering_result.csv" ))
    writer.write("friends_count,followers_count,cluster number\n")
    
    dataStream.foreachRDD { rdd =>
      val arrStr = rdd.collect
      			for(str <- arrStr){
      			  val clusterNr = kmModel.predict(Vectors.dense(str.split(",").map(_.toDouble)))
				      writer.write(str + "," + clusterNr + "\n")
      			}
     }
    
    dataStream.print() //check the first 10 elements in the input stream
    
    ssc.start()
    ssc.awaitTermination()


  }  
  
}