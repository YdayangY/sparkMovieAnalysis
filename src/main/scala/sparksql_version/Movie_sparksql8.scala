package sparksql_version

import org.apache.spark.sql.SparkSession
import org.apache.log4j.Logger
import org.apache.log4j.Level
import org.apache.spark.sql.types.StructType
import org.apache.spark.sql.types.StructField
import org.apache.spark.sql.types.StringType
import org.apache.spark.sql.types.DoubleType
import org.apache.spark.sql.Row
import java.util.regex.Pattern

/**需求8:每个年度下不同类型电影生产总数
    //输出格式:(年度,类型,数量)
 * */
object Movie_sparksql8 {
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
    
//    val moviesDS=moviessql.map(line=>{  
//      val fields=line.split("::")
//      val Title=fields(1)
//      val Genres=fields(2)
//      var year=""
//      val pattern=Pattern.compile("(.*) (\\(\\d{4}\\))")
//      val matcher=pattern.matcher(Title)
//      if(matcher.find()){
//        year=matcher.group(2)
//        year=year.substring(1,year.length()-1)
//      }
//      if(year==""){
//         (1,Genres)
//      }else{
//        (year.toInt,Genres)
//      }
//    })
   
//    val movieswithGenres=moviesDS
//                         .flatMap(line=>{  DS的flatMap
//                           val year=line._1
//                           val Genres=line._2.split("\\|")
//                           for(  i <-0 until Genres.length) yield (year,Genres(i))
//                         })
//                         .toDF("year","Genres")  
      
    /*
     * moviesDF.flatMap(row=>{
     * 	val genres=line.getString(2)
     * 	genres.split("\\|") //Array->traversable   同上面的结果一样
     * })
     * */
    val moviesDF=moviessql.map(line=>{
      val fields=line.split("::")
      val Title=fields(1)
      val Genres=fields(2)
      var year=""
      val pattern=Pattern.compile("(.*) (\\(\\d{4}\\))")//" (.*) (\\(\\d{4}\\))" 为1的数据变为691条
      val matcher=pattern.matcher(Title)
      if(matcher.find()){
        year=matcher.group(2)
        year=year.substring(1,year.length()-1)
      }
      if(year==""){
         (1,Genres)
      }else{
        (year.toInt,Genres)
      }
    }).toDF()
    val movieswithGenres=moviesDF.flatMap(row=>{//DF的flatmap
      val year=row.getInt(0)
      val Genres=row.getString(1).split("\\|")
      for(i<-0 until Genres.length) yield (year,Genres(i))
    }).toDF("year","Genres")
     
    /* yield作用
     * 针对每一次 for 循环的迭代, yield 会产生一个值，被循环记录下来 (内部实现上，像是一个缓冲区). 
              当循环结束后, 会返回所有 yield 的值组成的集合. 
              返回集合的类型与被遍历的集合类型是一致的.
     * */
                         
    movieswithGenres.show()
    
    movieswithGenres.createTempView("v_movies")
    
   
//    val usersDF=userssql.map(line=>{
//      val fields=line.split("::")
//      val uid=fields(0)
//      val age=fields(2).toInt
//      (uid,age)
//    }).toDF("uid","age")
//    usersDF.createTempView("v_users")
//   
   
    spark.sql("select year,Genres,count(Genres) countGenres from v_movies group by year,Genres order by year asc").show(false)
    
  
               
   movieswithGenres
       .groupBy("year","Genres")
       .agg("Genres"->"count")
       .orderBy($"year" asc)
       .toDF().show(false)
 
    
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

/*+----+---------+-----------+
  |year|Genres   |countGenres|
  +----+---------+-----------+
  |1   |Comedy   |1          |
  |1919|Action   |1          |
  |1919|Adventure|1          |
  |1919|Comedy   |1          |
  |1919|Drama    |2          |
  |1920|Comedy   |2          |
  |1921|Action   |1          |
  |1922|Horror   |1          |
  |1922|Drama    |1          |
  |1923|Drama    |1          |
  |1923|Comedy   |2          |
  |1925|Drama    |3          |
  |1925|Comedy   |3          |
  |1925|War      |1          |
  |1926|Comedy   |1          |
  |1926|Thriller |1          |
  |1926|Sci-Fi   |1          |
  |1926|Crime    |1          |
  |1926|Adventure|1          |
  |1926|Drama    |4          |
  +----+---------+-----------+
 * */
