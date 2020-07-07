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

/**需求7:输出格式:(年度,数量)
 * */
object Movie_sparksql7 {
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
    
    val moviesDF=moviessql.map(line=>{
      val fields=line.split("::")
      var year=""
      val pattern=Pattern.compile("(.*) (\\(\\d{4}\\))")//Toy Story (1995)
      val matcher=pattern.matcher(fields(1))
      if(matcher.find()){
        year=matcher.group(2)
        year=year.substring(1,year.length-1)
      }
      if(year==""){
        (1)
      }else{
      (year.toInt)
      }
    }).toDF("year")
   
   
    
    //moviesDF.show(100)
    
    moviesDF.createTempView("v_movies")
    
   
//    val usersDF=userssql.map(line=>{
//      val fields=line.split("::")
//      val uid=fields(0)
//      val age=fields(2).toInt
//      (uid,age)
//    }).toDF("uid","age")
//    usersDF.createTempView("v_users")
//   
   
    spark.sql("select year,count(year) countyear from v_movies group by year order by year asc").show()
    
    /*
     * +----+---------+
        |year|countyear|
        +----+---------+
        |   1|        1|
        |1919|        3|
        |1920|        2|
        |1921|        1|
        |1922|        2|
        |1923|        3|
        |1925|        6|
        |1926|        8|
        |1927|        6|
        |1928|        3|
        |1929|        3|
        |1930|        7|
        |1931|        7|
        |1932|        7|
        |1933|        7|
        |1934|        7|
        |1935|        6|
        |1936|        8|
        |1937|       11|
        |1938|        6|
        +----+---------+*/
    
   
               
   moviesDF
       .groupBy("year")
       .agg("year"->"count")
       .orderBy($"year" asc)
       .toDF().show()
 /*
  * +----+-----------+
    |year|count(year)|
    +----+-----------+
    |   1|          1|
    |1919|          3|
    |1920|          2|
    |1921|          1|
    |1922|          2|
    |1923|          3|
    |1925|          6|
    |1926|          8|
    |1927|          6|
    |1928|          3|
    |1929|          3|
    |1930|          7|
    |1931|          7|
    |1932|          7|
    |1933|          7|
    |1934|          7|
    |1935|          6|
    |1936|          8|
    |1937|         11|
    |1938|          6|
    +----+-----------+
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