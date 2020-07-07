package rdd_version 

import org.apache.log4j.Logger
import org.apache.log4j.Level
import org.apache.spark.SparkConf
import org.apache.spark.SparkContext

/*需求三
 * 1.所有电影平均得分最高的前十部电影
 * 2.观看人数最多的前十部电影
 * */
object Movie_RDD_3 {
  def main(args: Array[String]): Unit = {
    Logger.getLogger("org").setLevel(Level.INFO)//配置日志
    var masterUrl="local[2]"
    var appName="movie analysis"
    var userId="18"
    //将来上线后 jar,submit提交到服务器 可以命令行传参
    if(args.length>0){
      masterUrl=args(0)
    }else if(args.length>1){
      appName=args(1)
    }
    //创建上下文
    val conf=new SparkConf().setAppName(appName).setMaster(masterUrl)
    val sc=new SparkContext(conf)
    val filepath="data/"
    val usersRDD=sc.textFile(filepath+"users.dat")
    val occupationsRDD=sc.textFile(filepath+"occupations.dat")
    val ratingsRDD=sc.textFile(filepath+"ratings.dat")
    val moviesRDD=sc.textFile(filepath+"movies.dat")
    
 /*   usersRDD.cache()
    occupationsRDD.cache()
    ratingsRDD.cache()
    moviesRDD.cache()*/
   
    
    val ratings=ratingsRDD.map(_.split("::"))
    .map(x=>(x(0),x(1),x(2)))
    ratings.cache()
    //需求三1.所有电影平均得分最高的前十部电影
   val userWathcedMovie=ratings.map( item=>(item._2,(item._3.toDouble,1)))//元组(movieId,评分)
    .reduceByKey((a,b)=> (a._1+b._1,a._2+b._2 )).map(item=>(item._1,item._2._1/item._2._2))
    .sortBy(_._2,false).take(10)
   
    println("评分排名前十的电影:")
    userWathcedMovie.foreach(println)
   
    println("这些电影的详情：(moviedid,title,genre,总评分，观影次数，品军分)\n")
    val moviesInfo=moviesRDD.map(_.split("::"))
    .map(movie=>{(movie(0),(movie(1),movie(2)))})
    
   val ratingsInfo= ratings.map(x=>(x._2,(x._3.toDouble,1)))  //(movieid,(rating,1)) 
    .reduceByKey((x,y)=>{
      (x._1+y._1,x._2+y._2)
    })
    .map(x=>(x._1,(x._2._1/x._2._2,x._2._1,x._2._2)))//(movieid,(平均分,总分,总次数))
    
    moviesInfo.join(ratingsInfo)//(movieid,((title,genres),(平均分,总分,总次数)))
    .map(info=>{(info._2._2._1,(info._1,info._2._1._1,info._2._1._2,info._2._2._2,info._2._2._3))})//(平均分，(movied,title,genres,总分,总次数))
    .sortByKey(false)
    .take(10)
    .foreach(println)
    
    
    //需求三:观看人数最多的前十部电影
    println("简版的输出(观影人数,电影编号)")
    ratings.map(x=>(x._2,1))
    .reduceByKey((x,y)=>{
      (x+y)
    })//movieid,总次数
    .map(x=>(x._2,x._1))//总次数,movied
    .sortByKey(false)
    .take(10)
    .foreach(println)
    
    println("详情的输出(观影人数,电影编号)") //ratingsInfo(MovieId,(平均分,总分,总次数))
    moviesInfo.join(ratingsInfo)//MovieID,((Title,Genres),(平均分,总分,总次数))
    .map(info=>{
      (info._2._2._3,(info._1,info._2._1._1,info._2._1._2,info._2._2._2,info._2._2._1))
    })//总次数,(movieID,title,genres,总分,平均分)
    .sortByKey(false)
    .take(10)
    .foreach(println)
    sc.stop()
  }
 
}