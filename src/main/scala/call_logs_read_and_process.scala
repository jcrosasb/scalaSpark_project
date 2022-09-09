import org.apache.spark.sql.functions._
import org.apache.spark.sql.{DataFrame, SparkSession}
import org.apache.spark.{SparkConf, SparkContext}
import java.io.File

object call_logs_read_and_process {

    def main(args: Array[String]): Unit = {

        val warehouse_location = new File("spark-warehouse").getAbsolutePath

        val spark: SparkSession = SparkSession
            .builder()
            .appName("call_logs_read_data")
            .master("local[2]")
            .config("spark.sql.warehouse.dir", warehouse_location)
            .getOrCreate()

        val df: DataFrame = spark.read
            .format("csv")
            .option("header", "true")
            .csv("hdfs://namenode:9000/call_logs/call_logs.csv")

       // Create dataframe for call_logs table
       val raw_call_logs = df.select(col("contact_id"), col("id_agn"), col("id_chan"), 
            col("id_sent"), col("id_cmpg"), col("id_cont"), col("id_ccl"), col("id_rsn"), 
            col("master_id"), col("start_date"), col("date"), col("start_time"), 
            col("prequeue"), col("inqueue"), col("agent_time"), col("postqueue"), 
            col("abandon"), col("abandon_time"), col("sla"))

        // Create dataframe for mod_call_logs table (a modified version of call_logs that includes curren timestamp)
        val mod_raw_call_logs = raw_call_logs.withColumn("current_date",current_timestamp().as("current_date"))

        // Save mod_call_logs as parquet file
        mod_raw_call_logs.write.mode("append").saveAsTable("mod_raw_call_logs")

        spark.stop()
        
    }
}
