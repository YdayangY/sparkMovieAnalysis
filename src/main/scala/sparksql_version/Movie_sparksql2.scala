package sparksql_version

import org.apache.spark.sql.SparkSession
import org.apache.log4j.Logger
import org.apache.log4j.Level
/*需求二
 * 1.某个用户看过电影的数量
 * 2.这些电影的信息格式为:(MovieId,Title,Genres)
 * */

object Movie_sparksql2 {
  def main(args: Array[String]): Unit = {
     Logger.getLogger("org").setLevel(Level.ERROR)//配置日志
    val spark=SparkSession
    .builder()
    .appName("spark sql basic ")
    .master("local[*]")
    .getOrCreate()
    
    var userID="18"
    
    import spark.implicits._
    
    val moviessql=spark.read.textFile("data/movies.dat")
    //val occupationssql=spark.read.textFile("data/occupations.dat")
    val ratinsql=spark.read.textFile("data/ratings.dat")
    //val userssql=spark.read.textFile("data/users.dat")
    
    //println("movies.count="+moviessql.count()+" occupations="+occupationssql.count+" ratings="+ratinsql.count()+" userssql="+userssql.count)
    
    //18观看过的电影数:305
    
  val ratingsDF=ratinsql.map(line=>{
     val fields=line.split("::")
     val uid=fields(0)
     val mid=fields(1)
     (uid,mid)
    }).toDF("uid","mid")
    
    ratingsDF.createTempView("v_ratings")
    
    val moviesDF=moviessql.map(line=>{
       val fields=line.split("::")
       val MovieId=fields(0)
       val Title=fields(1)
       val Genres=fields(2)
       (MovieId,Title,Genres)
    }).toDF("MovieId","Title","Genres")
    
    moviesDF.createTempView("v_movies")
    
    //sql版
//    spark.sql("select count(uid) movies from v_ratings where uid="+userID).show
//    
//    val resultDF=spark.sql("select MovieId,Title,Genres from v_ratings right join v_movies on v_ratings.mid=v_movies.MovieId where uid="+userID)

    //DSL版
   println(ratingsDF.select("uid").where("uid="+userID).count())
   
   val resultDF=ratingsDF.join(moviesDF,$"mid"===$"MovieId","right")
     .filter($"uid"===userID)
    .select("MovieId","Title","Genres")
    
    println(resultDF.count)
    resultDF.show
    spark.stop
  }
}