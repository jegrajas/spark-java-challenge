package com.batch.job.reader;

import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;

public class DatasetReader {
    /**
     * Read the csv file and generate the input dataset
     * @param spark
     * @param path
     * @return Dataset<Row>
     */
    public Dataset<Row> read(SparkSession spark, String path) {
        return spark.read().option("header", "true").csv(path);
    }
}