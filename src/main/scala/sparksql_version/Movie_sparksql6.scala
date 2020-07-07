package sparksql_version

import org.apache.spark.sql.SparkSession
import org.apache.log4j.Logger
import org.apache.log4j.Level
import org.apache.spark.sql.types.StructType
import org.apache.spark.sql.types.StructField
import org.apache.spark.sql.types.StringType
import org.apache.spark.sql.types.DoubleType
import org.apache.spark.sql.Row

/**需求6:分析 不同类型的电影总数
    //输出格式:(类型,数量)
 * */
object Movie_sparksql6 {
  def main(args: Array[String]): Unit = {
     Logger.getLogger("org").setLevel(Level.ERROR)//配置日志
    val spark=SparkSession
    .builder()
    .appName("spark sql basic ")
    .master("local[*]")
    .getOrCreate()
    
    import spark.implicits._
    
    val moviessql=spark.read.textFile("data/movies.dat")
    //val occupationssql=spark.read.textFile("data/occupations.dat")
//    val ratinsql=spark.read.textFile("data/ratings.dat")
//    val userssql=spark.read.textFile("data/users.dat")
    
    //println("movies.count="+moviessql.count()+" occupations="+occupationssql.count+" ratings="+ratinsql.count()+" userssql="+userssql.count)
   
//  val ratingsDF=ratinsql.map(line=>{
//     val fields=line.split("::")
//     val ruid=fields(0)
//     val mid=fields(1)
//     val grade=fields(2).toDouble
//     (ruid,mid,grade)
//    }).toDF("ruid","mid","grade")
//
//    
//    ratingsDF.createTempView("v_ratings")
    
    val moviesDS=moviessql.map(line=>{
      val fields=line.split("::")
      val Genres=fields(2)
      (Genres)
    })
   
    val movieswithGenres=moviesDS
                         .flatMap( _.split("\\|"))//DS才能这样split
                         .toDF("Genres")  
    //DF的flatmap
    /*
     * moviesDF.flatMap(row=>{
     * 	val genres=line.getString(2)
     * 	genres.split("\\|") //Array->traversable   同上面的结果一样
     * })
     * */
    //movieswithGenres.show()
    
    movieswithGenres.createTempView("v_movies")
    
   
//    val usersDF=userssql.map(line=>{
//      val fields=line.split("::")
//      val uid=fields(0)
//      val age=fields(2).toInt
//      (uid,age)
//    }).toDF("uid","age")
//    usersDF.createTempView("v_users")
//   
   
    spark.sql("select Genres,count(Genres) countGenres from v_movies group by Genres order by countGenres desc").show(false)
    
    /*+-----------+-----------+
      |Genres     |countGenres|
      +-----------+-----------+
      |Drama      |1603       |
      |Comedy     |1200       |
      |Action     |503        |
      |Thriller   |492        |
      |Romance    |471        |
      |Horror     |343        |
      |Adventure  |283        |
      |Sci-Fi     |276        |
      |Children's |251        |
      |Crime      |211        |
      |War        |143        |
      |Documentary|127        |
      |Musical    |114        |
      |Mystery    |106        |
      |Animation  |105        |
      |Western    |68         |
      |Fantasy    |68         |
      |Film-Noir  |44         |
      +-----------+-----------+
     * */
               
   movieswithGenres
       .groupBy("Genres")
       .agg("Genres"->"count")
       .orderBy($"count(Genres)" desc)
       .toDF().show(false)
 /*
  * +-----------+-------------+
    |Genres     |count(Genres)|
    +-----------+-------------+
    |Drama      |1603         |
    |Comedy     |1200         |
    |Action     |503          |
    |Thriller   |492          |
    |Romance    |471          |
    |Horror     |343          |
    |Adventure  |283          |
    |Sci-Fi     |276          |
    |Children's |251          |
    |Crime      |211          |
    |War        |143          |
    |Documentary|127          |
    |Musical    |114          |
    |Mystery    |106          |
    |Animation  |105          |
    |Western    |68           |
    |Fantasy    |68           |
    |Film-Noir  |44           |
    +-----------+-------------+
  * 
  * */      
//   val resultDF=ratingsDF
//       .join(usersDF, $"ruid"===$"uid")
//       .join(moviesDF,$"mid"===$"MovieId")
//       .where($"age"===age)
//       .groupBy("mid","Title","Genres")
//       .agg("grade"->"sum","grade"->"count","grade"->"avg")
//       .orderBy($"avg(grade)" desc,$"count(grade)".desc,$"Title".asc)
//       .limit(10)
//    
//    //println(resultDF.count)
//    resultDF.show(false)
    spark.stop
  }
}