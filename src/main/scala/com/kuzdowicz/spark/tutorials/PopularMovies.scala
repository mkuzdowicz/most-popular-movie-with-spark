package com.kuzdowicz.spark.tutorials

import org.apache.log4j.{Level, Logger}
import org.apache.spark.sql.{SaveMode, SparkSession}

case class MovieRating(userId: Long, movieId: Long, rating: Double, timestamp: Long)

case class Movie(movieId: Long, title: String, genres: String)

case class MovieRatingWithDescription(userId: Long, movieId: Long, rating: Double, timestamp: Long, title: String, genres: String)

case class MostPopularMovieResult(title: String, clicks: Int)

object PopularMovies extends App {

  private val outputPath = "output"
  private val inputPath = "data/small/ml-latest-small"
  private val log = Logger.getLogger(getClass)

  val spark = SparkSession.builder()
    .master("local")
    .appName("most-popular-movies")
    .getOrCreate()

  spark.sparkContext.setLogLevel(Level.ERROR.toString)

  import spark.implicits._

  val ratings = spark.read.format("csv")
    .option("header", "true")
    .option("inferSchema", "true")
    .load(s"$inputPath/ratings.csv")
    .as[MovieRating]

  val movies = spark.read.format("csv")
    .option("header", "true")
    .option("inferSchema", "true")
    .load(s"$inputPath/movies.csv")
    .as[Movie]

  val moviesRatingsWithDescription = ratings.join(movies, "movieId")
    .as[MovieRatingWithDescription]

  moviesRatingsWithDescription.show()

  val numberOfPartitions = ratings.rdd.partitions.length

  log.info(s"numberOfPartitions => $numberOfPartitions")

  val mapEachMovieIdTo1 = moviesRatingsWithDescription.map(r => (r.title, 1))

  val mostPopularMovies = mapEachMovieIdTo1.rdd
    .reduceByKey((acc, v) => acc + v)
    .map { case (title, ratingsCount) => (ratingsCount, title) }
    .sortByKey(ascending = false)
    .map { case (ratingsCount, title) => (title, ratingsCount) }
    .toDF("title", "clicks")
    .as[MostPopularMovieResult]

  log.info("Most popular movies result:")

  mostPopularMovies.show()

  log.info(s"saving results to: $outputPath")

  mostPopularMovies
    .write
    .mode(SaveMode.Overwrite)
    .csv(outputPath)

}
