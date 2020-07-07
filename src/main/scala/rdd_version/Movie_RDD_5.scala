package rdd_version 

import org.apache.log4j.Logger
import org.apache.log4j.Level
import org.apache.spark.SparkConf
import org.apache.spark.SparkContext
import org.apache.spark.rdd.RDD.rddToPairRDDFunctions



/*需求四
 *需求5: 分析最受不同年龄段人员欢迎的电影的前10
 原始数据集中的年龄段划分
 under 18: 1
 18 - 24: 18
 25 - 34: 25
 35 - 44: 35
 45 - 49: 45
 50 - 55: 50
 56 + 56

 * */
object Movie_RDD_5 {
  def main(args: Array[String]): Unit = {
    Logger.getLogger("org").setLevel(Level.ERROR)//配置日志
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
    
    var age="1"//18岁一下的人喜好的电影top10
    println("年龄段为:"+age+"喜爱的电影Top10")
    //1.筛选人 age=1
    var userWithinAge=usersRDD.map(_.split("::"))
    .map(x=>(x(0),x(2)))
    .filter(_._2.equals(age))//(userID,age)
    userWithinAge.cache()
    //解决方案 1.userwhthinAge.collect() -> broadcast
    //2. 做成两个RDD然后join
    val ratings=ratingsRDD.map(_.split("::"))
    .map(x=>{
      (x(1),(x(0),x(2)))
    }) //(MovieID,(UserID,Rating))
    ratings.cache()
    
    //join
    val ratingWithinAge=userWithinAge.join(ratings)//(UserID,(age,(movieID,Rating)))
    .map(item=>(item._2._2._1,item._2._1,item._1,item._2._2._2))//(movieID,age,userid,Rating)
    
    ratingWithinAge.map(x=>(x._1,(x._4.toDouble,1)))//(MovieID,(rating,1))
    .reduceByKey((x,y)=>(x._1+y._1,x._2+y._2))//(MovieID,(总分,总次数))
    .map(item=>(item._2._1/item._2._2,(item._1,item._2._1,item._2._2)))//(平均分,(电影ID,总分数,次数))
    .sortByKey(false)
    .take(10)
    .foreach(println)
    
    //扩展:上面的显示中加入电影信息  电影名,类型
    println("年龄段为:"+age+"喜欢看的电影TOP10:(movieid,电影名,平均分,总分,观影次数,类型)")
    val rdd=ratingWithinAge.map(x=>(x._1,(x._4.toDouble,1)))//(MovieID,(rating.1))
    .reduceByKey((x,y)=>(x._1+y._1,x._2+y._2))//(MovieID,(总分,总次数))
    .map(item=>(item._1,(item._2._1/item._2._2,item._2._1,item._2._2)))//(movieID,(平均分,总分数,总次数))
   
    val moiveInfos=moviesRDD.map(x=>x.split("::"))
    .map(x=>(x(0),(x(1),x(2))))//(movieid,(title,genre))
    
    val joinedRDD=rdd.join(moiveInfos)//(movieID,((平均分,总分数,次数),(title,genre)))
    joinedRDD.cache
    
    joinedRDD.map(x=>(x._2._1._1,(x._1,x._2._2._1,x._2._1._2,x._2._1._3,x._2._2._2)))//(平均分,(movieid,电影名,总分,次数,genres))
    .sortByKey(false)//按均分排序
    .map(x=>(x._2._1,x._2._2.substring(0,x._2._2.indexOf("(")),x._1,x._2._3,x._2._4,x._2._5))//(movieid,电影名,平均分,总分数,次数,genres)
    .take(10)
    .foreach(println)
    
    //扩展二:二次排序,先根据平均分,再按观影次数(降序),再按电影名(升)
    println("二次排序,现根据平均分排序:如平均分相同,,再按观影次数(降序),再按电影名(升)")
    joinedRDD.sortBy(item=>(-item._2._1._1,-item._2._1._3,item._2._2._1) ) //tuple(movieID,((平均分,总分数,总次数),(title,genre)))
    .map(x=>(x._1,x._2._2._1,x._2._2._2,x._2._1._1,x._2._1._2,x._2._1._3))
    .take(10)
    .foreach(println)

    //扩展三:利用广播变量完成操作
    println("年龄在"+age+"的用户数据量:"+userWithinAge.count) //222个用户
    val userArray=userWithinAge.collect() //(userid,age)
    val broadcastRef=sc.broadcast(userArray)
    val ageRef=sc.broadcast(age)
    
    //(UserID,(MovieId,Rating))1
    val ratingWithAge=ratings.filter(rate=>{
      val userArray=broadcastRef.value
      val age=ageRef.value
      userArray.contains((rate._2._1,age))
    })
    
    println("用户打分的数据量有:"+ratingWithAge.count())//27211  //43017? 把movieid当成了userid
    
    ratingWithAge.map(item=>(item._2._1,(item._2._2.toDouble,1)))
    .reduceByKey((x,y)=>(x._1+y._1,x._2+y._2))//(movie,(总分,总次数))
    .map(item=>(item._1,item._2._1,item._2._2,item._2._1/item._2._2))//(movie,总分,总次数,平均分)
    .sortBy(item=>(-item._4,-item._3,item._1))
    .take(100)
    .foreach(println)
    sc.stop()
   
  }
 
}