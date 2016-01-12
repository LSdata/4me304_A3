package main.scala

import org.apache.spark.SparkContext
import org.apache.spark.SparkContext._
import org.apache.spark.SparkConf
import org.apache.spark.sql.SQLContext
import com.google.gson.{GsonBuilder, JsonParser}
import org.apache.spark.mllib.clustering.KMeans

import org.apache.spark.mllib.linalg.Vectors
import org.apache.spark.mllib.linalg.{Matrix, Vector}
import org.apache.spark.rdd._
import org.apache.spark.mllib.clustering._

import org.apache.spark.mllib.clustering.GaussianMixture
import org.apache.spark.mllib.clustering.GaussianMixtureModel
import java.io._

/* Student: Linnea Strågefors
 * Date: 2016-01-12
 * Course: Social Media Echosystems (4ME304)
 * 
 * This application contains the source code for the second part of Task 3 in Assignment 3. 
 * The second clustering algorithm chosen from the Spark MLlib is the Gaussian Mixture. 
 */


object SparkApplication3 {
  def main(args: Array[String]) {
    
    val sparkConf = new SparkConf().setAppName("stragefors_a3_3").setMaster("local")
    val sc = new SparkContext(sparkConf)
    
    //ASSIGNMENT TASK 3: Choose two clustering algorithms from the Spark MLib library
    /* (1. The K-means algorithm. This is implemented in the file SparkApplication.scala)
     * 2. The Gaussian Mixture
     */
    val cluster_tweets = sc.textFile("/Users/Linnea/Documents/Medietkn/HT15/LP2 Soc media echosyst/Assignments/A3/stragefors_A3/SparkApplication/src/main/resources/cluster_tweets")

    val nrClusters = 5 //five clusters
    val nrIterations = 20
    
    //load and parse the data
    val parsedData = cluster_tweets.map(s => Vectors.dense(s.split(",").map(_.toDouble)))
    parsedData.cache()
    
    // Cluster the data into 5 classes using GaussianMixture
    val gmm = new GaussianMixture().setK(5).run(parsedData)
    
    // Save and load model
    gmm.save(sc, "ClusterModel2")

    val gmmLoaded = GaussianMixtureModel.load(sc, ClusterModel2)
    
    // write info about the 5 Gaussian clusters
    // Shows max-likelihood model. 
    // weight: the weight of each Gaussian cluster
    // Mu: mean value. Sigma: covariance. Of each Gaussian cluster
     val writer = new PrintWriter(new File("GaussianModelInfo.txt" ))
    
    for (i <- 0 until gmm.k) {
      println("weight=%f\nmu=%s\nsigma=\n%s\n" format
        (gmm.weights(i), gmm.gaussians(i).mu, gmm.gaussians(i).sigma))
        
      writer.write("weight=%f\nmu=%s\nsigma=\n%s\n" format
        (gmm.weights(i), gmm.gaussians(i).mu, gmm.gaussians(i).sigma))

    }    
    writer.close()
    
    
    
   sc.stop()
  }
  
}