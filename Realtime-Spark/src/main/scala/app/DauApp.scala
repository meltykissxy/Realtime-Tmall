package app

import org.apache.spark.SparkConf
import org.apache.spark.streaming.{Seconds, StreamingContext}

object DauApp {
    def main(args: Array[String]): Unit = {
        val sparkConf: SparkConf = new SparkConf().setMaster("local[4]").setAppName("dau_app")
        val ssc = new StreamingContext(sparkConf, Seconds(5))
    }
}
