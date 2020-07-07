package rdd_version 

import org.apache.log4j.Logger
import org.apache.log4j.Level
import org.apache.spark.SparkConf
import org.apache.spark.SparkContext
import java.util.regex.Pattern

/*需求一
 * 1.读取信息，统计数据条数，职业数，电影数，用户数，评分条数
 * 2.显示每个职业下的用户详细信息，显示为:(职业编号,(人的编号，性别,年龄，邮编)，职业名)
 * */
object Movie_RDD_7 {
  def main(args: Array[String]): Unit = {
    Logger.getLogger("org").setLevel(Level.ERROR)//配置日志
    var masterUrl="local[2]"
    var appName="movie analysis"
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
    
   
    moviesRDD.cache()
   
    
    
    /*输出格式:(年度,数量)
     * */
    moviesRDD.map(x=>x.split("::"))
    .map(x=>(x(1),1))//(电影名,1) ->从电影名中取出year
    .map(item=>{//(year,1)
      var mname=""
      var year=""
      val pattern=Pattern.compile("(.*) (\\(\\d{4}\\))")//Toy Story (1995)
      val matcher=pattern.matcher(item._1)
      if(matcher.find()){
        mname=matcher.group(1)
        year=matcher.group(2)
        year=year.substring(1,year.length-1)
      }
      if(year==""){
        (-1,1)
      }else{
        (year.toInt,1)
      }
    })
    .reduceByKey((x,y)=>x+y)
    .sortByKey()
    .collect()
    .foreach(println)
    
    
     sc.stop()
  }
 
}