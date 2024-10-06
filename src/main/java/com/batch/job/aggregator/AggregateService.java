package com.batch.job.aggregator;

import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;

public interface AggregateService {
    Dataset aggregateData(Dataset<Row> transformDF, Long timeBucket);
}
