package org.catmq.client.common;

public interface SendCallback {
    void onSuccess(final SendResult sendResult);

    void onException(final Throwable e);
}
