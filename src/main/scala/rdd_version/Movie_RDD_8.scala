package rdd_version 

import org.apache.log4j.Logger
import org.apache.log4j.Level
import org.apache.spark.SparkConf
import org.apache.spark.SparkContext
import java.util.regex.Pattern

/*需求一
	每个年度下不同类型电影生产总数
 * */
object Movie_RDD_8 {
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
    .map(x=>(x(1),(1,x(2))))//(电影名,1) ->从电影名中取出year
    .map(item=>{//(year,(1,类型))
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
        (-1,item._2)
      }else{
        (year.toInt,item._2)
      }
    })
    .groupByKey() //(年份,[(1,类型),(1,类型)])
    .flatMapValues( array =>{  //[(1,类型s),(1,类型s)]
      var A:Map[String,Int]=Map() //类型 次数
      array.foreach( item=>{ //(1,类型S)
        var count=item._1
        var types=item._2.split("\\|")
        for(t<-types){
          if(A.contains(t)){
            var oldCount=A.getOrElse(t, 0)+1
            A+=(t->oldCount)
          }else{
            A+=(t->1)
          }
        }
      })
      A
    })  //(year,(类型,总数))
    .sortByKey()
    .collect()
    .foreach(println)
    
    
     sc.stop()
  }
 
}