package org.catmq.client.common;

public interface ConsumeCallback<T> {
    void onSuccess(final T consumeValue);

    void onException(final Throwable e);
}
