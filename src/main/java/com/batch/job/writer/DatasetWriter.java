package com.batch.job.writer;

import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SaveMode;

public class DatasetWriter {
    /**
     * Write the final dataset in to output folder
     * @param df
     * @param path
     */
    public void write(Dataset<Row> df, String path) {
        df.write().option("header", "true").mode(SaveMode.Overwrite).csv(path);
    }
}