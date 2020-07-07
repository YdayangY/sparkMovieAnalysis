package sparksql_version

import org.apache.spark.sql.SparkSession
import org.apache.log4j.Logger
import org.apache.log4j.Level
import org.apache.spark.sql.types.StructType
import org.apache.spark.sql.types.StructField
import org.apache.spark.sql.types.StringType
import org.apache.spark.sql.types.DoubleType
import org.apache.spark.sql.Row

/*需求三
 * 1.所有电影平均得分最高的前十部电影
 * 2.观看人数最多的前十部电影
 * */
object Movie_sparksql3 {
  def main(args: Array[String]): Unit = {
     Logger.getLogger("org").setLevel(Level.ERROR)//配置日志
    val spark=SparkSession
    .builder()
    .appName("spark sql basic ")
    .master("local[*]")
    .getOrCreate()
    
    var userID="18"
    
    import spark.implicits._
    
    val moviessql=spark.sparkContext.textFile("data/movies.dat")
    //val occupationssql=spark.read.textFile("data/occupations.dat")
    val ratinsql=spark.sparkContext.textFile("data/ratings.dat")
    //val userssql=spark.read.textFile("data/users.dat")
    
    //println("movies.count="+moviessql.count()+" occupations="+occupationssql.count+" ratings="+ratinsql.count()+" userssql="+userssql.count)
   
  val ratingsRDD=ratinsql.map(line=>{
     val fields=line.split("::")
     val mid=fields(1)
     val grade=fields(2).toDouble
     Row(mid,grade)
    })

    val structRatings=StructType(List(
            StructField("mid",StringType),
            StructField("grade",DoubleType)
            ))
    
    val ratingsDF=spark.createDataFrame(ratingsRDD, structRatings)
    
   // ratingsDF.createTempView("v_ratings")
    
    val moviesRDD=moviessql.map(line=>{
       val fields=line.split("::")
       val MovieId=fields(0)
       val Title=fields(1)
       val Genres=fields(2)
       Row(MovieId,Title,Genres)
    })
    
    val structMovies=StructType(List(
        StructField("MovieId",StringType),
        StructField("Title",StringType),
        StructField("Genres",StringType)
        ))
    val moviesDF=spark.createDataFrame(moviesRDD, structMovies)
    
   // moviesDF.createTempView("v_movies")
    
    
    
    //spark.sql("select mid,avg(grade) avggrade from v_ratings group by mid order by avggrade desc").show(10)
    /*
     * +----+--------+
        | mid|avggrade|
        +----+--------+
        |3382|     5.0|
        |3172|     5.0|
        |3881|     5.0|
        |3233|     5.0|
        |3607|     5.0|
        | 787|     5.0|
        |3280|     5.0|
        | 989|     5.0|
        |3656|     5.0|
        |1830|     5.0|
        +----+--------+
     * */
//    //sql版 1.所有电影平均得分最高的前十部电影
//    spark.sql("select  MovieId,Title,Genres, AVG(grade) avggrade from v_ratings right"
//               +" join v_movies on v_ratings.mid=v_movies.MovieId group by MovieId ,Title,Genres order by avggrade desc").show(10)
//    
//    //2.观看人数最多的前十部电影
//    val resultDF=spark.sql("select  MovieId,Title,Genres, count(grade) countgrade , AVG(grade) avggrade from v_ratings right"
//               +" join v_movies on v_ratings.mid=v_movies.MovieId group by MovieId ,Title,Genres order by countgrade desc")

               
               
    //DSL版 1.所有电影平均得分最高的前十部电影
   ratingsDF
       .join(moviesDF, $"mid"===$"MovieID","right")
       .groupBy("MovieID","Title","Genres")
       .agg("grade"->"avg")
       .orderBy($"avg(grade)" desc).toDF().show(10,false)
   
   val resultDF=ratingsDF
       .join(moviesDF, $"mid"===$"MovieID","right")
       .groupBy("MovieID","Title","Genres")
       .agg("grade"->"count","grade"->"avg")
       .orderBy($"count(grade)" desc)
    
    //println(resultDF.count)
    resultDF.show(10,false)
    spark.stop
  }
}

//
//(3428,(2858,American Beauty (1999),Comedy|Drama,14800.0,4.3173862310385065))
//(2991,(260,Star Wars: Episode IV - A New Hope (1977),Action|Adventure|Fantasy|Sci-Fi,13321.0,4.453694416583082))
//(2990,(1196,Star Wars: Episode V - The Empire Strikes Back (1980),Action|Adventure|Drama|Sci-Fi|War,12836.0,4.292976588628763))
//(2883,(1210,Star Wars: Episode VI - Return of the Jedi (1983),Action|Adventure|Romance|Sci-Fi|War,11598.0,4.022892819979188))
//(2672,(480,Jurassic Park (1993),Action|Adventure|Sci-Fi,10057.0,3.7638473053892216))
//(2653,(2028,Saving Private Ryan (1998),Action|Drama|War,11507.0,4.337353938937053))
//(2649,(589,Terminator 2: Judgment Day (1991),Action|Sci-Fi|Thriller,10751.0,4.058512646281616))
//(2590,(2571,Matrix, The (1999),Action|Sci-Fi|Thriller,11178.0,4.315830115830116))
//(2583,(1270,Back to the Future (1985),Comedy|Sci-Fi,10307.0,3.9903213317847466))
//(2578,(593,Silence of the Lambs, The (1991),Drama|Thriller,11219.0,4.3518231186966645))