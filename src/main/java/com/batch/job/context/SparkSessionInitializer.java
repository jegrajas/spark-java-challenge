package com.batch.job.context;

import org.apache.spark.sql.SparkSession;

public class SparkSessionInitializer {

    /**
     * Initialize the Spark Session with default executor and driver config and master node is local setup
     * @return
     */
    public static SparkSession initialize() {
        return SparkSession.builder()
                .appName("MetricAggregation")
                .master("local[*]")
                .config("spark.sql.session.timeZone", "UTC")
                .getOrCreate();
    }
}
