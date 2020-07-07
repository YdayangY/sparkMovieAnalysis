package rdd_version 

import org.apache.log4j.Logger
import org.apache.log4j.Level
import org.apache.spark.SparkConf
import org.apache.spark.SparkContext

/*需求一
 * 1.读取信息，统计数据条数，职业数，电影数，用户数，评分条数
 * 2.显示每个职业下的用户详细信息，显示为:(职业编号,(人的编号，性别,年龄，邮编)，职业名)
 * */
object Movie_RDD_1 {
  def main(args: Array[String]): Unit = {
    Logger.getLogger("org").setLevel(Level.INFO)//配置日志
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
    
    usersRDD.cache()
    occupationsRDD.cache()
    ratingsRDD.cache()
    moviesRDD.cache()
    
    println("职业数:"+occupationsRDD.count())
    println("电影数:"+moviesRDD.count())
    println("用户数:"+usersRDD.count())
    println("评分条数:"+ratingsRDD.count())
    
    //需求1:显示每个职业下的用户信息(职业编号,(人的编号，性别,年龄，邮编)，职业名)
    val usersBasic=usersRDD.map(_.split("::"))//UserId,Gender,Age,OccupationID,Zip-code
    .map(user=>(user(3),(user(0),user(1),user(2),user(4))))
     val occupations=occupationsRDD.map(_.split("::"))//OccupationID,OccupationName
     .map(occupation=>{
       (occupation(0),occupation(1))
     })
     //合并
     val usersInfo=usersBasic.join(occupations)
     println("用户详情(格式:(职业编号,(人的编号,性别,年龄,邮编),职业名)):")
     usersInfo.foreach(println)
     
     println("合并后共有:"+usersInfo.count()+"条users记录")
     sc.stop()
  }
 
}