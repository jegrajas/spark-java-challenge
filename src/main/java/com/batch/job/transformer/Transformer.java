package com.batch.job.transformer;

public interface Transformer<T,U,V> {
    V transform(final T Dataset, final U timeBucket);
}
