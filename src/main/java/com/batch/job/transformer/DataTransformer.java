package com.batch.job.transformer;

import com.batch.job.constants.Constants;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.functions;

public class DataTransformer implements Transformer<Dataset<Row>, Long, Dataset<Row>> {

    /***
     *  transform the dataset with UTC Timestamp and time bucketing
     * @param df
     * @param duration
     * @return Dataset<Row>
     */
    public Dataset<Row> transform(Dataset<Row> df, Long duration) {

        // Convert timestamp to a proper TimestampType and create a time bucket column
        df = df.withColumn(Constants.PARSED_TIMESTAMP, functions.to_timestamp(df.col(Constants.TIMESTAMP), Constants.UTC_TIMESTAMP));

        df=df.withColumn(Constants.TIME_BUCKET, functions.floor(functions.unix_timestamp(df.col(Constants.PARSED_TIMESTAMP)).divide(duration)));

        return df;
    }
}