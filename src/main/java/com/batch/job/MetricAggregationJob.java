package com.batch.job;

import com.batch.job.transformer.Transformer;
import com.batch.job.utils.AggregatorUtils;
import com.batch.job.aggregator.AggregateServiceImpl;
import com.batch.job.aggregator.AggregateService;
import com.batch.job.context.SparkSessionInitializer;
import com.batch.job.properties.ApplicationProperties;
import com.batch.job.reader.DatasetReader;
import com.batch.job.transformer.DataTransformer;
import com.batch.job.writer.DatasetWriter;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class MetricAggregationJob {
    private static final Logger logger = LoggerFactory.getLogger(MetricAggregationJob.class);

    private final SparkSession spark;

    private final ApplicationProperties props;

    private final DatasetReader reader;

    private final DatasetWriter writer;

    private final Transformer<Dataset<Row>, Long, Dataset<Row>> dataTransformer;

    private final AggregateService aggregateDataService;
    private final Long timeBucket;


    public MetricAggregationJob(SparkSession spark, ApplicationProperties props, DatasetReader reader, DatasetWriter writer, Transformer<Dataset<Row>, Long, Dataset<Row>> dataTransformer, AggregateService aggregateDataService, Long timeBucket) {
        this.spark = spark;
        this.props = props;
        this.reader = reader;
        this.writer = writer;
        this.dataTransformer = dataTransformer;
        this.aggregateDataService = aggregateDataService;
        this.timeBucket = timeBucket;

    }

    public void aggregateMetrics() {

        //read the property from properties file
        String inputFile = props.getProperty("input_file");
        String outputPath = props.getProperty("output_path");

        // Read the input dataset
        Dataset<Row> df = reader.read(spark, inputFile);
        logger.info("Input dataset csv read");

        // Transform the dataset
        Dataset<Row> transformDF = dataTransformer.transform(df, timeBucket);
        logger.info("Transform the dataset completed");

        // Aggregate the dataset
        Dataset resultData = aggregateDataService.aggregateData(transformDF, timeBucket);
        logger.info("Input dataset aggregation completed");

        resultData.show(false);

        // write the dataset in to csv
        writer.write(resultData, outputPath);
        logger.info("Output dataset written");

        // Stop the Spark session
        spark.stop();

    }

    public static void main(String[] args) {
        String timeBucket = "24 hour";
        if (args.length > 0) {
            timeBucket = args[0];
        }

        Long timeBucketInterval = AggregatorUtils.getTimeBucketInterval(timeBucket);

        // Initialize Spark session
        SparkSession spark = SparkSessionInitializer.initialize();
        logger.info("Spark session initialized");
        ApplicationProperties props = new ApplicationProperties();
        DatasetReader datasetReader = new DatasetReader();
        DatasetWriter datasetWriter = new DatasetWriter();
        Transformer<Dataset<Row>, Long, Dataset<Row>> transformer = new DataTransformer();
        AggregateService aggregateService = new AggregateServiceImpl();

        MetricAggregationJob metricAggregationJob = new MetricAggregationJob(spark, props, datasetReader, datasetWriter, transformer, aggregateService, timeBucketInterval);
        metricAggregationJob.aggregateMetrics();


    }




}


