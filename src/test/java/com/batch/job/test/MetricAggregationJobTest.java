package com.batch.job.test;

import com.batch.job.MetricAggregationJob;
import com.batch.job.aggregator.AggregateService;
import com.batch.job.aggregator.AggregateServiceImpl;
import com.batch.job.reader.DatasetReader;
import com.batch.job.transformer.Transformer;
import com.batch.job.context.SparkSessionInitializer;
import com.batch.job.properties.ApplicationProperties;
import com.batch.job.transformer.DataTransformer;
import com.batch.job.writer.DatasetWriter;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;
import org.junit.Before;
import org.junit.Test;

import java.io.IOException;
import java.nio.file.*;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;

public class MetricAggregationJobTest {

    private static final String TARGET_PATH="src/test/resources/input/input_dataset.csv";
    private static final String FILE_MOVE_PATH="src/test/resources/input/orginal_input_dataset.csv";

    private static final String EMPTY_FILE_MOVE_PATH="src/test/resources/input/empty_input_dataset.csv";

    private static final String OUTPUT_CSV_PATH="src/test/resources/output/";

    private SparkSession spark;
    private ApplicationProperties props;
    private DatasetReader datasetReader;
    private DatasetWriter datasetWriter;
    private Transformer<Dataset<Row>, Long, Dataset<Row>> transformer;
    private AggregateService aggregateService;

    private final Long timeBucket= 60L;

    @Before
    public void setUp() {
        // Initialize Spark session
        spark = SparkSessionInitializer.initialize();
        props = new ApplicationProperties();
        datasetReader = new DatasetReader();
        datasetWriter = new DatasetWriter();
        transformer = new DataTransformer();
        aggregateService = new AggregateServiceImpl();
    }

    /**
     * Test will run against input dataset
     * @throws Exception
     */
    @Test
    public void testAggregateMetricsForInputDataset() throws Exception{

        Path fileToMovePath = Paths.get(FILE_MOVE_PATH);
        Path targetPath = Paths.get(TARGET_PATH);
        Files.copy(fileToMovePath, targetPath, StandardCopyOption.REPLACE_EXISTING);

        // Create an instance of MetricAggregationJob
        MetricAggregationJob job = new MetricAggregationJob(spark, props, datasetReader, datasetWriter, transformer, aggregateService, timeBucket);

        // Run the aggregateMetrics method
        job.aggregateMetrics();

        // Spark session is reinitialized again to read the ouptut dataset file
        spark = SparkSessionInitializer.initialize();

        // Read the output data
        Dataset<Row> outputData = datasetReader.read(spark, OUTPUT_CSV_PATH);

        assertEquals(2, outputData.count());

    }

    /**
     * Test will run against empty dataset file
     * @throws IOException
     */
    @Test
    public void testAggregateMetricsForEmptyDataset() throws IOException {

        Path fileToMovePath = Paths.get(EMPTY_FILE_MOVE_PATH);
        Path targetPath = Paths.get(TARGET_PATH);
        Files.copy(fileToMovePath, targetPath, StandardCopyOption.REPLACE_EXISTING);

        // Create an instance of MetricAggregationJob
        MetricAggregationJob job = new MetricAggregationJob(spark, props, datasetReader, datasetWriter, transformer, aggregateService, timeBucket);

        // Run the aggregateMetrics method
        job.aggregateMetrics();

        // Spark session is reinitialized again to read the ouptut dataset file
        spark = SparkSessionInitializer.initialize();

        // Read the output data
        Dataset<Row> outputData = datasetReader.read(spark, OUTPUT_CSV_PATH);

        assertTrue(outputData.isEmpty());
    }


}