package sparksql_version

import org.apache.spark.sql.SparkSession
import org.apache.log4j.Logger
import org.apache.log4j.Level
import java.util.Properties

/*
 * 分析不同职业对观看电影类型的影响
 * 格式:(职位名,(电影类型,观影次数))
 * 用户与职位
 * */
object Movie_sparksql9 {
  def main(args: Array[String]): Unit = {
     Logger.getLogger("org").setLevel(Level.ERROR)//配置日志
    val spark=SparkSession
    .builder()
    .appName("spark sql basic ")
    .master("local[*]")
    .getOrCreate()
    
    import spark.implicits._
    
    val moviessql=spark.read.textFile("data/movies.dat")
    val occupationssql=spark.read.textFile("data/occupations.dat")
    val ratinsql=spark.read.textFile("data/ratings.dat")
    val userssql=spark.read.textFile("data/users.dat")
    
   // println("movies.count="+moviessql.count()+" occupations="+occupationssql.count+" ratings="+ratinsql.count()+" userssql="+userssql.count)
    
   val moviesDF=moviessql.flatMap(line=>{
     val fields=line.split("::")
     val mid=fields(0)
     val Genres=fields(2).split("\\|")
     for( i<-0 until Genres.length) yield (mid,Genres(i))
   }).toDF("mid","Genres")
   
   //moviesDF.createTempView("v_movies")
    
   
   val ratingsDF=ratinsql.map(line=>{
     val fields=line.split("::")
     val ruid=fields(0).toInt
     val rmid=fields(1)
     (ruid,rmid)
   }).toDF("ruid","rmid")
   
   //ratingsDF.createTempView("v_ratings")
   
  val occupationsDF=occupationssql.map(line=>{
     val fields=line.split("::")
     val oid=fields(0).toInt
     val occ=fields(1)
     (oid,occ)
    }).toDF("oid","occ")
    
    //occupationsDF.createTempView("v_occupations")
    
    val usersDF=userssql.map(line=>{
       val fields=line.split("::")
       val uid=fields(0).toInt
       val uoid=fields(3).toInt
       (uid,uoid)
    }).toDF("uid","uoid")
    
    //usersDF.createTempView("v_users")
    
    /*
 * 分析不同职业对观看电影类型的影响
 * 格式:(职位名,(电影类型,观影次数))
 * 用户与职位
 * */
    //sql版
//   val resultDF=spark.sql("select occ,Genres,count(ruid) cntruid from v_ratings "
//       +" join v_movies on v_ratings.rmid=v_movies.mid "
//       +" join v_users on v_ratings.ruid=v_users.uid "
//       +" join v_occupations on v_users.uoid=v_occupations.oid "
//       +" group by occ,Genres order by occ desc,cntruid desc")
 /*  +----------+-----------+--------+
      |       occ|     Genres|cntgrade|
      +----------+-----------+--------+
      |    writer|      Drama|   23225|
      |    writer|     Comedy|   22092|
      |    writer|     Action|   12504|
      |    writer|   Thriller|   10363|
      |    writer|    Romance|    8949|
      |    writer|     Sci-Fi|    8191|
      |    writer|  Adventure|    6966|
      |    writer|      Crime|    4911|
      |    writer|     Horror|    4348|
      |    writer| Children's|    4271|
      |    writer|        War|    3859|
      |    writer|    Musical|    2723|
      |    writer|  Animation|    2648|
      |    writer|    Mystery|    2588|
      |    writer|    Fantasy|    1900|
      |    writer|  Film-Noir|    1434|
      |    writer|    Western|    1148|
      |    writer|Documentary|     748|
      |unemployed|     Comedy|    5461|
      |unemployed|      Drama|    4941|
      +----------+-----------+--------+ */
             
   
    //DSL版
    val resultDF=ratingsDF
                 .join(moviesDF,$"rmid"===moviesDF("mid"))
                 .join(usersDF,$"ruid"===$"uid")
                 .join(occupationsDF,usersDF("uoid")===occupationsDF("oid"))
                 .groupBy(occupationsDF("occ"), $"Genres")
                 .agg("ruid"->"count")
                 .orderBy(occupationsDF("occ") desc,$"count(ruid)" desc)
    //println(resultDF.count)
    
//    val props=new Properties()   保存到数据库
//     props.put("user","root")
//     props.put("password","a")
//     props.put("driver","com.mysql.jdbc.Driver")
//                 
//    resultDF.write.mode("ignore").jdbc("jdbc:mysql://localhost:3306/bigdata", "result1", props)
//    //.format("jdbc").options(Map("url"->"jdbc:myslq://localshot:3306/bigdata","driver"->"com.mysql.jdbc.Driver","user"->"root","password" -> "a","dbtable"->"result"))
//    println("保存到mysql成功")
    resultDF.show
    spark.stop
  }
}

/*
 * +----------+-----------+-----------+
  |       occ|     Genres|count(ruid)|
  +----------+-----------+-----------+
  |    writer|      Drama|      23225|
  |    writer|     Comedy|      22092|
  |    writer|     Action|      12504|
  |    writer|   Thriller|      10363|
  |    writer|    Romance|       8949|
  |    writer|     Sci-Fi|       8191|
  |    writer|  Adventure|       6966|
  |    writer|      Crime|       4911|
  |    writer|     Horror|       4348|
  |    writer| Children's|       4271|
  |    writer|        War|       3859|
  |    writer|    Musical|       2723|
  |    writer|  Animation|       2648|
  |    writer|    Mystery|       2588|
  |    writer|    Fantasy|       1900|
  |    writer|  Film-Noir|       1434|
  |    writer|    Western|       1148|
  |    writer|Documentary|        748|
  |unemployed|     Comedy|       5461|
  |unemployed|      Drama|       4941|
  +----------+-----------+-----------+
 * */
