package sparksql_version

import org.apache.spark.sql.SparkSession
import org.apache.log4j.Logger
import org.apache.log4j.Level
import org.apache.spark.sql.types.StructType
import org.apache.spark.sql.types.StructField
import org.apache.spark.sql.types.StringType
import org.apache.spark.sql.types.DoubleType
import org.apache.spark.sql.Row

/*需求四
 * 1.分析男性用户最喜欢看的前十部电影
 * 2.女性用户最喜欢看的前十部电影
 * */
object Movie_sparksql4 {
  def main(args: Array[String]): Unit = {
     Logger.getLogger("org").setLevel(Level.ERROR)//配置日志
    val spark=SparkSession
    .builder()
    .appName("spark sql basic ")
    .master("local[*]")
    .getOrCreate()
    
    import spark.implicits._
    
    val moviessql=spark.sparkContext.textFile("data/movies.dat")
    //val occupationssql=spark.read.textFile("data/occupations.dat")
    val ratinsql=spark.sparkContext.textFile("data/ratings.dat")
    val userssql=spark.sparkContext.textFile("data/users.dat")
    
    //println("movies.count="+moviessql.count()+" occupations="+occupationssql.count+" ratings="+ratinsql.count()+" userssql="+userssql.count)
   
  val ratingsRDD=ratinsql.map(line=>{
     val fields=line.split("::")
     val ruid=fields(0)
     val mid=fields(1)
     val grade=fields(2).toDouble
     Row(ruid,mid,grade)
    })

    val structRatings=StructType(List(
            StructField("ruid",StringType),
            StructField("mid",StringType),
            StructField("grade",DoubleType)
            ))
    
    val ratingsDF=spark.createDataFrame(ratingsRDD, structRatings)
    
    //ratingsDF.createTempView("v_ratings")
    
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
    //moviesDF.createTempView("v_movies")
    
    val usersRDD=userssql.map(line=>{
      val fields=line.split("::")
      val uid=fields(0)
      val sex=fields(1)
      Row(uid,sex)
    })
    
    val structUsers=StructType(List(
      StructField("uid",StringType),
      StructField("sex",StringType)
    ))
    val usersDF=spark.createDataFrame(usersRDD, structUsers)
    //usersDF.createTempView("v_users")
   
    
    //sql版 1.分析男性用户最喜欢看的前十部电影  
//   spark.sql("select  mid,Title,Genres,avg(grade) avggrade from v_ratings "
//               +" inner join v_users on v_ratings.ruid=v_users.uid "
//               +" inner join v_movies on v_ratings.mid=v_movies.MovieId "
//               +" where sex='M' group by mid,Title,Genres order by avggrade desc limit 10").show(false)
    /*"select  mid,avg(grade) avggrade from v_ratings right"
              +" join v_users on v_ratings.ruid=v_users.uid  where sex='M' group by mid order by avggrade desc"
     * +----+--------+ 
      |mid |sumgrade|  // 两表联立
      +----+--------+
      |2858|10790.0 |
      |260 |10537.0 |
      |1196|10175.0 |
      |2028|9141.0  |
      |1210|9074.0  |
      |2571|9056.0  |
      |589 |9025.0  |
      |1198|8779.0  |
      |593 |8203.0  |
      |110 |8153.0  |
      +----+--------+*/
               
   /*  三表联立
    *  spark.sql("select  mid,Title,Genres,avg(grade) avggrade from v_ratings "
               +" inner join v_users on v_ratings.ruid=v_users.uid "
               +" inner join v_movies on v_ratings.mid=v_movies.MovieId "
               +" where sex='M' group by mid,Title,Genres order by avggrade desc limit 10").show(false)
    * +----+-----------------------------------------+-----------+--------+
      |mid |Title                                    |Genres     |avggrade|
      +----+-----------------------------------------+-----------+--------+
      |3172|Ulysses (Ulisse) (1954)                  |Adventure  |5.0     |
      |985 |Small Wonders (1996)                     |Documentary|5.0     |
      |1830|Follow the Bitch (1998)                  |Comedy     |5.0     |
      |3656|Lured (1947)                             |Crime      |5.0     |
      |3233|Smashing Time (1967)                     |Comedy     |5.0     |
      |989 |Schlafes Bruder (Brother of Sleep) (1995)|Drama      |5.0     |
      |130 |Angela (1995)                            |Drama      |5.0     |
      |787 |Gate of Heavenly Peace, The (1995)       |Documentary|5.0     |
      |439 |Dangerous Game (1993)                    |Drama      |5.0     |
      |3280|Baby, The (1973)                         |Horror     |5.0     |
      +----+-----------------------------------------+-----------+--------+
    * */
    
               
    //2.女性用户最喜欢看的前十部电影
//    val resultDF=spark.sql("select  mid,sum(grade) sumgrade from v_ratings right"
//               +" join v_users on v_ratings.ruid=v_users.uid  where sex='F' group by mid order by sumgrade desc")
/*+----+--------+
  |mid |sumgrade| 
  +----+--------+
  |2858|4010.0  |
  |2396|3337.0  |
  |593 |3016.0  |
  |2762|2973.0  |
  |318 |2846.0  |
  |527 |2806.0  |
  |260 |2784.0  |
  |608 |2771.0  |
  |1197|2762.0  |
  |1196|2661.0  |
  +----+--------+
 * 
 * */
               
               
    //DSL版 1.所有电影平均得分最高的前十部电影
    
 
   ratingsDF
       .join(usersDF, $"ruid"===$"uid")
       .join(moviesDF,$"mid"===$"MovieID")
       .where($"sex"==="M")
       .groupBy("mid","Title","Genres")
       .agg("grade"->"avg")
       .orderBy($"avg(grade)" desc,$"mid" asc).toDF().show(10,false) 
     /*+----+----------------------------------+-----------+----------+  完整版  三表联立
      |mid |Title                             |Genres     |avg(grade)|
      +----+----------------------------------+-----------+----------+
      |130 |Angela (1995)                     |Drama      |5.0       |
      |1830|Follow the Bitch (1998)           |Comedy     |5.0       |
      |3172|Ulysses (Ulisse) (1954)           |Adventure  |5.0       |
      |3233|Smashing Time (1967)              |Comedy     |5.0       |
      |3280|Baby, The (1973)                  |Horror     |5.0       |
      |3517|Bells, The (1926)                 |Crime|Drama|5.0       |
      |3656|Lured (1947)                      |Crime      |5.0       |
      |439 |Dangerous Game (1993)             |Drama      |5.0       |
      |787 |Gate of Heavenly Peace, The (1995)|Documentary|5.0       |
      |985 |Small Wonders (1996)              |Documentary|5.0       |
      +----+----------------------------------+-----------+----------+
     * */
   
//   val resultDF=ratingsDF   简版
//       .join(usersDF, $"ruid"===$"uid")
//       .where($"sex"==="F")
//       .groupBy("mid")
//       .agg("grade"->"avg")
//       .orderBy($"avg(grade)" desc,$"mid" asc)
//    
//    //println(resultDF.count)
//    resultDF.show(10,false)
    spark.stop
  }
}
