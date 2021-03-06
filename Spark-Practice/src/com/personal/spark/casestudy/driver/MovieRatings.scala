package com.personal.spark.casestudy.driver

import org.apache.spark.SparkConf
import org.apache.spark.SparkContext
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.SQLContext
import org.apache.spark.sql.types.{StructType,StructField,IntegerType,StringType}
import org.apache.spark.sql.types.LongType
import org.apache.spark.sql.functions._
import org.apache.spark.sql.Row
import org.apache.spark.sql.SaveMode

object MovieRatings {
  def main(args:Array[String]){
  	/*
  	 * For testing in windows os - with eclipse
  	 * Steps::
  	 * 
  	 * Create the following directory structure: "C:\hadoop_home\bin" (or replace "C:\hadoop_home" with whatever you like)
		 * Download the following file: http://public-repo-1.hortonworks.com/hdp-win-alpha/winutils.exe
     * Put the file from step 2 into the "bin" directory from step 1.
	   * Set the "hadoop.home.dir" system property to "C:\hadoop_home" (or whatever directory you created in step 1, without the "\bin" at the end). Note: You should be declaring this property in the beginning of your Spark code
  	 */
  	sys.props.+=(("hadoop.home.dir","C:\\hadoop_home"))
  	
  	val sparkConf = new SparkConf().setMaster("local").setAppName("Sample")
  	val sparkContext = new SparkContext(sparkConf)
  	val spark = SparkSession.builder().appName("Sample").getOrCreate()
  	
  	
  	/*
  	 * Expected output ratings file sample 
  	 */
  	//val sparkContext = new SparkContext(sparkConf)
  	val sqlContext = new SQLContext(sparkContext)
  	val ratingsRDD = spark.read.text(".\\src\\main\\resources\\ratings.dat").rdd
  																.map{row => 
  																	val colsData = row.getAs[String](0).replace("::",":").split(":")
  																	Ratings(colsData(0).toInt,colsData(1).toInt,colsData(2).toInt,colsData(3).toLong)}
  	
  	val ratingsDf = spark.createDataFrame(ratingsRDD).repartition(10).cache()
  	ratingsDf.show();
  	println(ratingsDf.schema)
  	
  	/*
  	 * Expected output movie file sample 
  	 */
  	val moviesRDD = spark.read.option("delimiter","::").text(".\\src\\main\\resources\\movies.dat").rdd
  																.map{row => 
  																	val colsData = row.getAs[String](0).replace("::",":").split(":")
  																	Movies(colsData(0).toInt,colsData(1),colsData(2))}
  	
  	val moviesDf = spark.createDataFrame(moviesRDD).repartition(10).cache()
  	moviesDf.show();
  	println(moviesDf.schema)
  	
  	/*
  	 * Unique users
  	 */
  	val distinctUidDF = ratingsDf.groupBy(col("userId")).agg(min(col("movieId"))).select(col("userId"))
  	distinctUidDF.show()
  	distinctUidDF.coalesce(1).write.mode(SaveMode.Overwrite).csv(".\\src\\main\\resources\\distinctUidData")
  	println(distinctUidDF.count())
  	
  	/*
  	 * Most rated movie
  	 */
    val mostRatedMoviesDF = ratingsDf.groupBy(col("movieId")).agg(count(col("userId")).as("cnt"),avg(col("rating")).as("average_rating")).orderBy(col("cnt").desc)
    val topRecMostRatedDF = mostRatedMoviesDF.limit(1)
    topRecMostRatedDF.coalesce(1).write.mode(SaveMode.Overwrite).csv(".\\src\\main\\resources\\mostRatedMovie")
  	topRecMostRatedDF.show()
  	
  	/*
  	 * First top 10 Rated Movies with Movie names
  	 */
  	val joinedMovieRatingsDF = mostRatedMoviesDF.join(moviesDf, Seq("movieId"), "inner").orderBy(col("cnt").desc).select(col("movieId"),col("movieName"),col("average_rating")).limit(10)
  	joinedMovieRatingsDF.coalesce(1).write.mode(SaveMode.Overwrite).csv(".\\src\\main\\resources\\top10MostRated")
  	joinedMovieRatingsDF.show()
  	
  	/*
  	 * Avg rating for each movie
  	 */
	 	val averageRatingForMovieDF = joinedMovieRatingsDF.limit(10)
  	averageRatingForMovieDF.show()
	 	averageRatingForMovieDF.coalesce(1).write.mode(SaveMode.Overwrite).csv(".\\src\\main\\resources\\averageRatingPerMovie")
		/*
  	 * Worst Rated Movie
  	 */
		val worstRatedMovieDF = ratingsDf.groupBy(col("movieId")).agg(count(col("userId")).as("cnt")).orderBy(col("cnt")).limit(20)
  	worstRatedMovieDF.show()
		worstRatedMovieDF.coalesce(1).write.mode(SaveMode.Overwrite).csv(".\\src\\main\\resources\\worstRated")
  	
  	/*
  	 * Ratings save to Hive
  	 */
  		//ratingsDf.write.mode(SaveMode.Overwrite).saveAsTable("movie_ratings_database.ratings")
  		//val ratingsHiveDF = spark.sql("Select * from movie_ratings_database.ratings")
  		//ratingsHiveDF.printSchema()
  }
  case class Ratings (userId:Integer, movieId:Integer, rating:Integer, timestamp:Long)
  case class Movies (movieId:Integer, movieName:String, genre:String)
}