package rdd_version 

import org.apache.log4j.Logger
import org.apache.log4j.Level
import org.apache.spark.SparkConf
import org.apache.spark.SparkContext
import java.util.regex.Pattern
import java.sql.DriverManager
import scala.collection.mutable.ArrayBuffer

/*
 * 分析不同职业对观看电影类型的影响
 * 格式:(职位名,(电影类型,观影次数))
 * 用户与职位
 * */
object Movie_RDD_10 {
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
   
    /*
 * 分析不同职业对观看电影类型的影响
 * 格式:(职位名,(电影类型,观影次数))
 *  +批量入库
 * */
    //用户与职位
   val user=usersRDD.map(_.split("::"))//userid::gender::age::occupationid::zip-code
   .map(x=>(x(3),x(0))) //(occupationid,userid) 
   val rating=ratingsRDD.map(_.split("::"))
   .map(x=>(x(0),x(1)))//(userid,movieid)
   val occ=occupationsRDD.map(_.split("::"))
   .map(x=>(x(0),x(1)))
   
   //合并用户与职业
   val uoRDD=user.join(occ)//(occupationid,(userid,occupationname))
   .map(item=>(item._2._1,item._2._2))//(userid,occupationname)
   //电影与电影类型
   val moviesTypeRDD=moviesRDD.map(_.split("::"))
   .map(x=>(x(0),x(2)))//(编号,类型)=>(1,Animation|Children's|Comedy)  
   .flatMapValues(types=>{types.split("\\|")}) //(编号,Animation),(编号,children's),(编号,comedy)
   
   val rdd=uoRDD.join(rating)//(UserID,(Occupation,movieid))
   .map(item=>(item._2._2,item._2._1)) //(movieid,OccupationName)
   .join(moviesTypeRDD)//(movieId,OccupationName,Animation)
   .map(item=>(item._2._1,(item._1,item._2._2)))//(occupationName,(movieeid,Animation))
   
   val resultRDD=rdd.groupByKey() //(OccupationName.Iterable[(1,Animation),(1,Animation),(1,Animation),(2,Animation),(2,Animation)])
   .flatMapValues(array=>{
     var A:Map[String,Int]=Map()
     array.foreach( item=>{
       if(A.contains(item._2)){
         var oldCount=A.getOrElse(item._2, 0)+1
         A+=(item._2->oldCount)
       }else{
         A+=(item._2->1)
       }
     })
     A
   })
   
   //入库
   resultRDD.mapPartitions(elements=>{
     val con=DriverManager.getConnection("jdbc:mysql://localhost:3306/bigdata","root","a")
     //事务处理
     con.setAutoCommit(false)//隐式事务提交
     val pstmt=con.prepareStatement("insert into result(occ,mytype,nums) values(?,?,?)")
     var result=new ArrayBuffer[String]()//受影响的行数
     
     for(ele <- elements){
       pstmt.setString( 1, ele._1)
       pstmt.setString( 2, ele._2._1)
       pstmt.setInt( 3, ele._2._2)
       pstmt.addBatch() //添加到批量任务中
       result+=(ele._1)
     }
     pstmt.executeBatch()
     con.commit
     con.setAutoCommit(true)
     con.close
     println("添加一个分区数据成功")
     result.iterator
   })
   .foreach(println)
   
   sc.stop()
  }
 
}