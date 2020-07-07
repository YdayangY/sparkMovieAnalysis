package rdd_version 

import org.apache.log4j.Logger
import org.apache.log4j.Level
import org.apache.spark.SparkConf
import org.apache.spark.SparkContext

/*需求二
 * 1.某个用户看过电影的数量
 * 2.这些电影的信息格式为:(Movield,Title,Genres)
 * */
object Movie_RDD_2 {
  def main(args: Array[String]): Unit = {
    Logger.getLogger("org").setLevel(Level.INFO)//配置日志
    var masterUrl="local[2]"
    var appName="movie analysis"
    var userId="18"
    //将来上线后 jar,submit提交到服务器 可以命令行传参
    if(args.length>0){
      masterUrl=args(0)
      userId=args(1)
    }else if(args.length>1){
      appName=args(1)
    }
   // println(args.length)
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
   
    
    //需求二 1.某个用户看过电影的数量
   val userWathcedMovie=ratingsRDD.map(_.split("::"))
    .map( item=>{(item(1),item(0))})  //元组(movieId,UserId)
    .filter(_._2.equals(userId))
   
    /*val userWathcedMovie=ratingsRDD.map( _.split("::"))
    .filter(item=>item(0).equals(userId)) //数组
*/    
    println(userId+"观看过的电影数:"+userWathcedMovie.count())
   
    println("这些电影的详情：\n")
    //需求二:这些电影的信息:格式为:(MovieId,Title,Genres)
    //userWatchedMovie与moviesRDD join
    //userWatchedMovie =>(MovieId,UserId) ->已完成
    //moviesRDD => (MovieId,(Title,Genres))
    val movieInfoRDD=moviesRDD.map( _.split("::"))
    .map( movie=>{(movie(0),(movie(1),movie(2)))})
    
    val result=userWathcedMovie.join(movieInfoRDD) //(movieId,(UserId,(Title,Genres)))
    .map(item=>{(item._1,item._2._1,item._2._2._1,item._2._2._2)})//(movieId,UserId,Title,Genres)
    
    result.foreach(println)
    sc.stop()
  }
 
}