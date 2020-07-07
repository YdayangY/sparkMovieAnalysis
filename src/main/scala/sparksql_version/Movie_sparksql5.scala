package sparksql_version

import org.apache.spark.sql.SparkSession
import org.apache.log4j.Logger
import org.apache.log4j.Level
import org.apache.spark.sql.types.StructType
import org.apache.spark.sql.types.StructField
import org.apache.spark.sql.types.StringType
import org.apache.spark.sql.types.DoubleType
import org.apache.spark.sql.Row

/**需求5: 分析最受不同年龄段人员欢迎的电影的前10
 原始数据集中的年龄段划分
 under 18: 1
 18 - 24: 18
 25 - 34: 25
 35 - 44: 35
 45 - 49: 45
 50 - 55: 50
 56 + 56
 * */
object Movie_sparksql5 {
  def main(args: Array[String]): Unit = {
     Logger.getLogger("org").setLevel(Level.ERROR)//配置日志
    val spark=SparkSession
    .builder()
    .appName("spark sql basic ")
    .master("local[*]")
    .getOrCreate()
    
    var age=1
    
    import spark.implicits._
    
    val moviessql=spark.read.textFile("data/movies.dat")
    //val occupationssql=spark.read.textFile("data/occupations.dat")
    val ratinsql=spark.read.textFile("data/ratings.dat")
    val userssql=spark.read.textFile("data/users.dat")
    
    //println("movies.count="+moviessql.count()+" occupations="+occupationssql.count+" ratings="+ratinsql.count()+" userssql="+userssql.count)
   
  val ratingsDF=ratinsql.map(line=>{
     val fields=line.split("::")
     val ruid=fields(0)
     val mid=fields(1)
     val grade=fields(2).toDouble
     (ruid,mid,grade)
    }).toDF("ruid","mid","grade")

    
    ratingsDF.createTempView("v_ratings")
    
    val moviesDF=moviessql.map(line=>{
       val fields=line.split("::")
       val MovieId=fields(0)
       val Title=fields(1)
       val Genres=fields(2)
       (MovieId,Title,Genres)
    }).toDF("MovieId","Title","Genres")
    
    
    moviesDF.createTempView("v_movies")
    
    val usersDF=userssql.map(line=>{
      val fields=line.split("::")
      val uid=fields(0)
      val age=fields(2).toInt
      (uid,age)
    }).toDF("uid","age")
    usersDF.createTempView("v_users")
   
    
    //sql版  分析最受不同年龄段人员欢迎的电影的前10  简版
   spark.sql("select  mid,sum(grade) sumgrade,count(grade) countgrade,avg(grade) avgrade from v_ratings right"
               +" join v_users on v_ratings.ruid=v_users.uid  where age="+age+" group by mid order by avgrade desc,countgrade desc limit 10").show(false)
/*
 * +----+--------+----------+-------+
    |mid |sumgrade|countgrade|avgrade|
    +----+--------+----------+-------+
    |1354|20.0    |4         |5.0    |
    |1286|15.0    |3         |5.0    |
    |550 |15.0    |3         |5.0    |
    |1900|10.0    |2         |5.0    |
    |534 |10.0    |2         |5.0    |
    |1966|10.0    |2         |5.0    |
    |3838|10.0    |2         |5.0    |
    |2357|10.0    |2         |5.0    |
    |1827|10.0    |2         |5.0    |
    |1113|10.0    |2         |5.0    |
    +----+--------+----------+-------+
 * */
               
    //常规版              
    spark.sql("select  mid,Title,Genres,sum(grade) sumgrade,count(grade) countgrade,avg(grade) avgrade from v_ratings "
               +" inner join v_users on v_ratings.ruid=v_users.uid "
               +" inner join v_movies on v_ratings.mid=v_movies.MovieId where age="+age+" group by mid,Title,Genres "
               +" order by avgrade desc,countgrade desc,Title asc limit 10").show(false)           
 
 /*增加了 Title asc排序 所以结果和上面不一样
  * +----+--------------------------------------------------+------------------+--------+----------+-------+
|mid |Title                                             |Genres            |sumgrade|countgrade|avgrade|
+----+--------------------------------------------------+------------------+--------+----------+-------+
|1354|Breaking the Waves (1996)                         |Drama             |20.0    |4         |5.0    |
|1286|Somewhere in Time (1980)                          |Drama|Romance     |15.0    |3         |5.0    |
|550 |Threesome (1994)                                  |Comedy|Romance    |15.0    |3         |5.0    |
|1113|Associate, The (1996)                             |Comedy            |10.0    |2         |5.0    |
|1827|Big One, The (1997)                               |Comedy|Documentary|10.0    |2         |5.0    |
|2677|Buena Vista Social Club (1999)                    |Documentary       |10.0    |2         |5.0    |
|2357|Central Station (Central do Brasil) (1998)        |Drama             |10.0    |2         |5.0    |
|1900|Children of Heaven, The (Bacheha-Ye Aseman) (1997)|Drama             |10.0    |2         |5.0    |
|2068|Fanny and Alexander (1982)                        |Drama             |10.0    |2         |5.0    |
|613 |Jane Eyre (1996)                                  |Drama|Romance     |10.0    |2         |5.0    |
+----+--------------------------------------------------+------------------+--------+----------+-------+
  * 
  * */
               
/*
 * (5.0,(1245,10.0,2))
(5.0,(5674,10.0,2))
(5.0,(5375,10.0,2))
(5.0,(989,5.0,1))
(5.0,(2574,5.0,1))
(5.0,(5274,5.0,1))
(5.0,(2475,15.0,3))
(5.0,(884,15.0,3))
(5.0,(4668,10.0,2))
(5.0,(3755,15.0,3))*/               
               
               
    //DSL版  分析最受不同年龄段人员欢迎的电影的前10  简版
   ratingsDF
       .join(usersDF, $"ruid"===$"uid")
       .where($"age"===age)
       .groupBy("mid")
       .agg("grade"->"sum","grade"->"count","grade"->"avg")
       .orderBy($"avg(grade)" desc,$"count(grade)" desc)
       .limit(10)
       .toDF().show(false)
/* 大概数据出现的时机不一致所以上下两次结果不完全一致  只有部分一致
 * +----+----------+------------+----------+
    |mid |sum(grade)|count(grade)|avg(grade)|
    +----+----------+------------+----------+
    |1354|20.0      |4           |5.0       |
    |1286|15.0      |3           |5.0       |
    |550 |15.0      |3           |5.0       |
    |3838|10.0      |2           |5.0       |
    |2357|10.0      |2           |5.0       |
    |3839|10.0      |2           |5.0       |
    |1827|10.0      |2           |5.0       |
    |1900|10.0      |2           |5.0       |
    |2068|10.0      |2           |5.0       |
    |534 |10.0      |2           |5.0       |
    +----+----------+------------+----------+
 * 
 *  添加上 limit 10  数据完全一致
 * +----+----------+------------+----------+
    |mid |sum(grade)|count(grade)|avg(grade)|
    +----+----------+------------+----------+
    |1354|20.0      |4           |5.0       |
    |1286|15.0      |3           |5.0       |
    |550 |15.0      |3           |5.0       |
    |3838|10.0      |2           |5.0       |
    |2357|10.0      |2           |5.0       |
    |3839|10.0      |2           |5.0       |
    |1827|10.0      |2           |5.0       |
    |1900|10.0      |2           |5.0       |
    |2068|10.0      |2           |5.0       |
    |534 |10.0      |2           |5.0       |
    +----+----------+------------+----------+
 * */   
   val resultDF=ratingsDF
       .join(usersDF, $"ruid"===$"uid")
       .join(moviesDF,$"mid"===$"MovieId")
       .where($"age"===age)
       .groupBy("mid","Title","Genres")
       .agg("grade"->"sum","grade"->"count","grade"->"avg")
       .orderBy($"avg(grade)" desc,$"count(grade)".desc,$"Title".asc)
       .limit(10)
    
    //println(resultDF.count)
    resultDF.show(false)
    spark.stop
  }
}

/*+----+--------------------------------------------------+------------------+----------+------------+----------+
  |mid |Title                                             |Genres            |sum(grade)|count(grade)|avg(grade)|
  +----+--------------------------------------------------+------------------+----------+------------+----------+
  |1354|Breaking the Waves (1996)                         |Drama             |20.0      |4           |5.0       |
  |1286|Somewhere in Time (1980)                          |Drama|Romance     |15.0      |3           |5.0       |
  |550 |Threesome (1994)                                  |Comedy|Romance    |15.0      |3           |5.0       |
  |1113|Associate, The (1996)                             |Comedy            |10.0      |2           |5.0       |
  |1827|Big One, The (1997)                               |Comedy|Documentary|10.0      |2           |5.0       |
  |2677|Buena Vista Social Club (1999)                    |Documentary       |10.0      |2           |5.0       |
  |2357|Central Station (Central do Brasil) (1998)        |Drama             |10.0      |2           |5.0       |
  |1900|Children of Heaven, The (Bacheha-Ye Aseman) (1997)|Drama             |10.0      |2           |5.0       |
  |2068|Fanny and Alexander (1982)                        |Drama             |10.0      |2           |5.0       |
  |613 |Jane Eyre (1996)                                  |Drama|Romance     |10.0      |2           |5.0       |
  +----+--------------------------------------------------+------------------+----------+------------+----------+
   * 
 * */
