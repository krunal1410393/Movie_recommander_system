// Databricks notebook source
//---- SQL -----
import org.apache.spark.SparkContext
import org.apache.spark.SparkContext._
import org.apache.spark.SparkConf
import org.apache.spark.sql._
import sqlContext.implicits._ 

//---- MLLib-----
import org.apache.spark.mllib.linalg.Vectors
import org.apache.spark.mllib.regression.LabeledPoint
import org.apache.spark.mllib.tree.{DecisionTree, RandomForest}
import org.apache.spark.sql.SQLContext
import org.apache.spark.{SparkConf, SparkContext}

// File uploaded to /FileStore/tables/movies.dat
// File uploaded to /FileStore/tables/README
// File uploaded to /FileStore/tables/users.dat
// File uploaded to /FileStore/tables/ratings.dat

// COMMAND ----------

val a =sc.textFile("/FileStore/tables/ratings.dat")
case class ratings(UserID: String, MovieID: String, Rating:Int, Timestamp:Double)
val ratings_df = a.map(_.split("::")).map(x=> ratings(x(0),x(1),x(2).toInt,x(3).toFloat)).toDF()
ratings_df.createOrReplaceTempView("ratings_table")
sqlContext.sql("select * from ratings_table").show(10,false)

// COMMAND ----------

ratings_df.printSchema()

// COMMAND ----------

val b =sc.textFile("/FileStore/tables/movies.dat")
case class movies(MovieID: String, Title:String, Mtype:String)
val movies_df = b.map(_.split("::")).map(x=> movies(x(0),x(1),x(2))).toDF()
movies_df.createOrReplaceTempView("movies_table")
sqlContext.sql("select * from movies_table").show(10,false)

// COMMAND ----------

val c =sc.textFile("/FileStore/tables/users.dat")
case class users(UserID:String, Gender:String, Age:Int, Occupation:Int, Zipcode:String)
//val users_df = c.map(_.split("::")).map(x=> users(x(0),x(1),x(2),x(3),x(4))).toDF()
val users_df = c.map(_.split("::")).map(x=> users(x(0),x(1),x(2).toInt,x(3).toInt,x(4).toString)).toDF()
users_df.createOrReplaceTempView("users_table")
sqlContext.sql("select * from users_table").show(10,false)

// COMMAND ----------

//(1) Find out the animated movies that are rated 4 or above.
val a =sc.textFile("/FileStore/tables/ratings.dat")
val ratings_df = sqlContext.sql ("select * from ratings_table where Rating >4 ").show(10,false) 

// COMMAND ----------

spark.conf.set("spark.sql.crossJoin.enabled", "true") // this is for enable joining table

val users_df = c.map(_.split("::")).map(x=> users(x(0),x(1),x(2).toInt,x(3).toInt,x(4).toString)).toDF()
val ratings_df = a.map(_.split("::")).map(x=> ratings(x(0),x(1),x(2).toInt,x(3).toFloat)).toDF()
val joined_df = ratings_df.join(users_df)

// COMMAND ----------

joined_df.printSchema()

// COMMAND ----------

joined_df.createOrReplaceTempView("joined_df_table")
sqlContext.sql("select * from joined_df_table").show(10,false)

// COMMAND ----------

//(2) Group the ratting by Age.
spark.conf.set("spark.sql.crossJoin.enabled", "true") // this is for enable joining table

val users_df = c.map(_.split("::")).map(x=> users(x(0),x(1),x(2).toInt,x(3).toInt,x(4).toString)).toDF()
val ratings_df = a.map(_.split("::")).map(x=> ratings(x(0),x(1),x(2).toInt,x(3).toFloat)).toDF()
val joined_df = ratings_df.join(users_df)
val joined_df_Age = sqlContext.sql ("select * from joined_df_table where Age >18 ").show(10,false) 


// COMMAND ----------

//(3)find out Average Ratting of movie
movie_counts = ratings_df.groupBy("movieid").count()

// COMMAND ----------

//we need for avg ratings count
movie_counts = movie_counts.sort(desc("count"))

// COMMAND ----------

movie_counts.show( 10 )


// COMMAND ----------

avg_ratings = ratings_df.groupBy("movieid").agg( {"rating":"avg"} )

// COMMAND ----------

avg_ratings.printSchema()

// COMMAND ----------

avg_ratings = avg_ratings.sort( desc( "avg(rating)" ) )

// COMMAND ----------

avg_ratings.show( 10 )

// COMMAND ----------

//(4) find out title of best rated movie
top_movies = avg_ratings_count.limit(10)                            
       .join( movies_df,
               avg_ratings_count.movieid == movies_df.movieid,
               "inner" ).drop(movies_df.movieid)

// COMMAND ----------

top_movies_10 = top_movies.select( "movieid", "mean_rating", "count", "name" )

// COMMAND ----------

top_movies_10.collect

// COMMAND ----------


