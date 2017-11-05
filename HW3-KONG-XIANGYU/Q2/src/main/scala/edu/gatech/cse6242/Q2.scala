package edu.gatech.cse6242

import org.apache.spark.SparkContext
import org.apache.spark.SparkContext._
import org.apache.spark.SparkConf
import org.apache.spark.sql.SQLContext
import org.apache.spark.sql.functions._

object Q2 {
    
    case class Line(src: String, tgt: String, weight: Int)

    def main(args: Array[String]) {
        val sc = new SparkContext(new SparkConf().setAppName("Q2"))
        val sqlContext = new SQLContext(sc)
        import sqlContext.implicits._

        val file = sc.textFile("hdfs://localhost:8020" + args(0))

        //generate the toDF 
        val totaledge = file.map(p => Line(p.split("\t")(0), p.split("\t")(1), p.split("\t")(2).trim.toInt)).toDF()

        val outsum = totaledge.filter($"weight" >= 5).groupBy("src").agg(sum("weight")as("outweight")).withColumnRenamed("outsum", "outweight")
        
        val insum = totaledge.filter($"weight" >= 5).groupBy("tgt").agg(sum("weight")as("inweight")).withColumnRenamed("insum", "inweight")

        val result = insum.as('in).join(outsum.as('out), $"in.tgt" === $"out.src","left")
                     .select(insum("tgt"),(coalesce('inweight, lit(0)) - coalesce('outweight, lit(0))).alias("gross"))
                     .na.fill(0)
                     
        //  coalesce(int numPartitions) : returns a new DataFrame that has exactly numPartitions partitions.
      result.map(x => x.mkString("\t")).saveAsTextFile("hdfs://localhost:8020" + args(1))
    }
}



