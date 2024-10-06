package com.batch.job.test;

import com.batch.job.utils.AggregatorUtils;
import org.junit.Assert;
import org.junit.Test;

public class AggregatorUtilsTest {

    @Test
    public void testGetTimeBucketInterval() {
        // Test for hours
        Assert.assertEquals(Long.valueOf(3600), AggregatorUtils.getTimeBucketInterval("1 hour"));
        Assert.assertEquals(Long.valueOf(7200), AggregatorUtils.getTimeBucketInterval("2 hours"));

        // Test for minutes
        Assert.assertEquals(Long.valueOf(1200), AggregatorUtils.getTimeBucketInterval("20 minutes"));
        Assert.assertEquals(Long.valueOf(60), AggregatorUtils.getTimeBucketInterval("1 minute"));

        // Test for invalid input
        Assert.assertEquals(Long.valueOf(0), AggregatorUtils.getTimeBucketInterval("invalid input"));
    }

    @Test
    public void testGetTimeBucketReadableFormat() {
        // Test for hours
        Assert.assertEquals("1 hour", AggregatorUtils.getTimeBucketReadableFormat(3600L));
        Assert.assertEquals("2 hours", AggregatorUtils.getTimeBucketReadableFormat(7200L));

        // Test for minutes
        Assert.assertEquals("20 minutes", AggregatorUtils.getTimeBucketReadableFormat(1200L));
        Assert.assertEquals("1 minute", AggregatorUtils.getTimeBucketReadableFormat(60L));

        // Test for seconds
        Assert.assertEquals("30 seconds", AggregatorUtils.getTimeBucketReadableFormat(30L));
        Assert.assertEquals("1 second", AggregatorUtils.getTimeBucketReadableFormat(1L));
    }
}
