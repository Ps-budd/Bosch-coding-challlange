import scala.util.matching.Regex
import org.apache.spark;
import org.apache.spark.SparkContext
import org.apache.spark.SparkContext._
import org.apache.spark.SparkConf
import org.apache.spark.sql.{DataFrame, SQLContext, SaveMode}
import org.apache.spark.sql.SparkSession

object HelloApp {



    def main(args: Array[String]) = {
       // println("Hello, world")
        
        val sparkConf = new SparkConf().setAppName("Hello Application").setMaster("local[1]").setSparkHome(System.getenv("SPARK_HOME"))
        val sc = new SparkContext(sparkConf )
        val sparkSession = SparkSession.builder
        .config(conf = sparkConf)
        .appName("Hello Application")
        .getOrCreate()

        
        
        //val sqlContext = new org.apache.spark.sql.SQLContext(sc)

        var df = sparkSession.read.option("header","true").csv("C:/Users/dubey/OneDrive/Desktop/bosch1/a.csv")
       

        df.show()
        // boolean array
        val z = new Array[Boolean](df.columns.length)
        val columnNames: Array[String] = df.columns
       //avoid suffling
        val df = dfo.coalesce(2)

        df.show()

        //regex
        val numberPattern1: Regex = "\\([0-9]{3}\\)\\-[0-9]{3}\\-[0-9]{4}".r
        val numberPattern2: Regex = "[0-9]{3}\\-[0-9]{3}\\-[0-9]{4}".r

        val numberPattern3: Regex = "[0-9]{3}\\-[0-9]{7}".r
        val numberPattern4: Regex = "[0-9]{10}".r

        //df.foreach(f => println(f))

        df.foreach{ row => 
            val v: Seq[Any]= row.toSeq
            println("printing row")
            println(row)
            for (i <- 0 until (v.length)) {
            
            if(!z(i)){
                val input : String = v(i).toString
                if(numberPattern1.pattern.matcher(input).matches || numberPattern2.pattern.matcher(input).matches ||
                numberPattern3.pattern.matcher(input).matches  ||
                numberPattern4.pattern.matcher(input).matches
                ) 
                {println("match")}else { println("not match") 
                z.update(i,true)}

            }else{println("skipping column "+i+ "invalid")}
            }
        } 
        for (i <- 0 until (z.length)) {
            if(z(i)==true){
                println("del column "+i+ "invalid")
                df.drop(df(columnNames(i)))
            }
        }

  
     
        df.show()

    }
}