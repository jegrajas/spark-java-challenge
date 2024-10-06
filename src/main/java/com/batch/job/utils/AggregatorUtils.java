package com.batch.job.utils;

public class AggregatorUtils {

    // Convert the time interevel  to seconds
    public static Long getTimeBucketInterval(String timeBucket) {
        Long timeBucketInterval = 0L;

        // Define time bucket interval in seconds (e.g., 20 minutes = 1200 seconds)
        if (timeBucket.contains("hour")) {
            String[] time = timeBucket.split(" ");
            timeBucketInterval = Long.parseLong(time[0]) * 3600;
        } else if (timeBucket.contains("minute")) {
            String[] time = timeBucket.split(" ");
            timeBucketInterval = Long.parseLong(time[0]) * 60;
        }
        return timeBucketInterval;
    }

    // Convert the time intervel seconds to human readable format.
    public static String getTimeBucketReadableFormat(Long timeBucketInterval) {
        if (timeBucketInterval >= 3600 && timeBucketInterval % 3600 == 0) {
            // Convert to hours
            long hours = timeBucketInterval / 3600;
            return hours + (hours == 1 ? " hour" : " hours");
        } else if (timeBucketInterval >= 60 && timeBucketInterval % 60 == 0) {
            // Convert to minutes
            long minutes = timeBucketInterval / 60;
            return minutes + (minutes == 1 ? " minute" : " minutes");
        } else {
            // Return in seconds if it does not fit into hours or minutes
            return timeBucketInterval + (timeBucketInterval == 1 ? " second" : " seconds");
        }
    }
}
