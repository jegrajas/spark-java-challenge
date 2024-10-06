package com.batch.job.aggregator;

import com.batch.job.constants.Constants;
import com.batch.job.utils.AggregatorUtils;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.functions;

import static org.apache.spark.sql.functions.*;


public class AggregateServiceImpl implements AggregateService {

    /**
     * Aggregate the data based on Metric and Time Bucket to calculate average min and max value
     *
     * @param transformDF
     * @param timeBucket
     * @return Dataset<Row>
     */
    @Override
    public Dataset<Row> aggregateData(Dataset<Row> transformDF, Long timeBucket) {

        // Implement the logic to aggregate the metrics
        // Group by Metric and TimeBucket, then calculate average , min, max values
        Dataset<Row> aggregateData = transformDF.groupBy(Constants.METRIC, Constants.TIME_BUCKET)
                .agg(round(functions.avg(Constants.VALUE), Constants.DECIMAL_SCALE).alias(Constants.AVERAGE_VALUE),
                        round(functions.min(Constants.VALUE), Constants.DECIMAL_SCALE).alias(Constants.MIN_VALUE),
                        round(functions.max(Constants.VALUE), Constants.DECIMAL_SCALE).alias(Constants.MAX_VALUE)
                ).withColumn(Constants.TIMESTAMP, date_format(from_unixtime(expr(Constants.TIME_BUCKET + " * " + timeBucket)), Constants.UTC_TIMESTAMP))
                .withColumn(Constants.TIME_BUCKET_DURATION, lit(AggregatorUtils.getTimeBucketReadableFormat(timeBucket))).drop(Constants.TIME_BUCKET)
                .select(Constants.METRIC, Constants.TIMESTAMP, Constants.TIME_BUCKET_DURATION, Constants.AVERAGE_VALUE, Constants.MIN_VALUE, Constants.MAX_VALUE);


        return aggregateData;
    }
}
