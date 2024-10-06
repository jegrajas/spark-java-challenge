package com.batch.job.test;

import com.batch.job.MetricAggregationJob;
import com.batch.job.aggregator.AggregateService;
import com.batch.job.transformer.Transformer;
import com.batch.job.properties.ApplicationProperties;
import com.batch.job.reader.DatasetReader;
import com.batch.job.writer.DatasetWriter;
import org.apache.spark.sql.*;
import org.junit.*;
import org.junit.runner.RunWith;
import org.mockito.*;
import org.mockito.junit.MockitoJUnitRunner;


import static org.mockito.Mockito.*;

@RunWith(MockitoJUnitRunner.class)
public class MetricAggregationJobMockTest {

    @Mock
    private SparkSession spark;

    @Mock
    private DatasetReader datasetReader;

    @Mock
    private Dataset<Row> inputDataset;

    @Mock
    private Dataset<Row> transformedDataset;

    @Mock
    private Dataset<Row> aggregatedDataset;

    @Mock
    private ApplicationProperties applicationProperties;

    @Mock
    private Transformer<Dataset<Row>, Long, Dataset<Row>> dataTransformer;

    @Mock
    private AggregateService aggregateService;

    @Mock
    private DatasetWriter datasetWriter;

    @InjectMocks
    private MetricAggregationJob metricAggregationJob;

    private final Long timeBucket= 86400L;

    @Before
    public void setUp() {
        MockitoAnnotations.initMocks(this);

        when(applicationProperties.getProperty("input_file")).thenReturn("input.csv");
        when(applicationProperties.getProperty("output_path")).thenReturn("output.csv");

        metricAggregationJob = new MetricAggregationJob(spark, applicationProperties, datasetReader, datasetWriter, dataTransformer, aggregateService, timeBucket);
    }

    @Test
    public void testAggregateMetrics() {
        // Mock the transformations
        when(datasetReader.read(any(), any())).thenReturn(inputDataset);
        when(dataTransformer.transform(inputDataset, timeBucket)).thenReturn(transformedDataset);
        when(aggregateService.aggregateData(transformedDataset, timeBucket)).thenReturn(aggregatedDataset);

        // Execute the method to test
        metricAggregationJob.aggregateMetrics();

        // Verify interactions
        verify(dataTransformer).transform(inputDataset, timeBucket);
        verify(aggregateService).aggregateData(transformedDataset, timeBucket);
        verify(datasetWriter).write(aggregatedDataset, "output.csv");
    }
}