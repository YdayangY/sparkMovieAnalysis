package rdd_version 

import org.apache.log4j.Logger
import org.apache.log4j.Level
import org.apache.spark.SparkConf
import org.apache.spark.SparkContext

/*需求一
 * 1.读取信息，统计数据条数，职业数，电影数，用户数，评分条数
 * 2.显示每个职业下的用户详细信息，显示为:(职业编号,(人的编号，性别,年龄，邮编)，职业名)
 * */
object Movie_RDD_6 {
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
    
  //需求6:分析 不同类型的电影总数
    //输出格式:(类型,数量)
    
    //Animation|Children's|Comedy
    // ->切分 1:[Animation,Children's,Comedy] 
    // => (1,Animation),(1,Children's),(1,Comedy)
    // =>交换位置 (Animation,1)(Children's,1)(Comedy,1)
    //汇总
    moviesRDD.map(x=>x.split("::"))
    .map(x=>(x(0),x(2))) //(1,Animation|Children's|Comedy)
    .flatMapValues(types=>{types.split("\\|")}) //(1,Animation),(1,Children's),(1,Comedy)
    .map(x=>(x._2,1))
    .reduceByKey((x,y)=>(x+y))
    .foreach(println)
    
    
     sc.stop()
  }
 
}


/*
 * (War,143)
(Sci-Fi,276)
(Fantasy,68)
(Comedy,1200)
(Western,68)
(Documentary,127)
(Musical,114)
(Mystery,106)
(Horror,343)
(Romance,471)
(Crime,211)
(Drama,1603)
(Animation,105)
(Thriller,492)
(Children's,251)
(Adventure,283)
(Film-Noir,44)
(Action,503)
 * */
