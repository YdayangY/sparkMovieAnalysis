package sparksql_version

import org.apache.spark.sql.SparkSession
import org.apache.log4j.Logger
import org.apache.log4j.Level
object Movie_sparksql1 {
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
    
    println("movies.count="+moviessql.count()+" occupations="+occupationssql.count+" ratings="+ratinsql.count()+" userssql="+userssql.count)
    
    
  val occupationsDF=occupationssql.map(line=>{
     val fields=line.split("::")
     val oid=fields(0).toInt
     val occ=fields(1)
     (oid,occ)
    }).toDF("oid","occ")
    
   // occupationsDF.createTempView("v_occupations")
    
    val usersDF=userssql.map(line=>{
       val fields=line.split("::")
       val uid=fields(0).toInt
       val sex=fields(1)
       val age=fields(2).toInt
       val ouid=fields(3).toInt
       val postcode=fields(4)
       (uid,sex,age,ouid,postcode)
    }).toDF("uid","sex","age","ouid","postcode")
    
    //usersDF.createTempView("v_users")
    
    //sql版
   // val resultDF=spark.sql("select oid,uid,sex,age,postcode,occ from v_occupations , v_users where v_occupations.oid=v_users.ouid")
    
    //DSL版
    val resultDF=occupationsDF.join(usersDF,$"oid"===$"ouid")
    .select("oid","uid","sex","age","postcode","occ")
    
    println(resultDF.count)
    resultDF.show
    spark.stop
  }
}