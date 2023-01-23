package org.catmq.grpc;

import io.grpc.Context;
import io.grpc.Metadata;

public class InterceptorConstants {
    public static final Context.Key<Metadata> METADATA = Context.key("rpc-metadata");

    /**
     * Remote address key in attributes of call
     */
    public static final Metadata.Key<String> REMOTE_ADDRESS
            = Metadata.Key.of("rpc-remote-address", Metadata.ASCII_STRING_MARSHALLER);

    /**
     * Local address key in attributes of call
     */
    public static final Metadata.Key<String> LOCAL_ADDRESS
            = Metadata.Key.of("rpc-local-address", Metadata.ASCII_STRING_MARSHALLER);

    public static final Metadata.Key<String> AUTHORIZATION
            = Metadata.Key.of("authorization", Metadata.ASCII_STRING_MARSHALLER);

    public static final Metadata.Key<String> NAMESPACE_ID
            = Metadata.Key.of("x-mq-namespace", Metadata.ASCII_STRING_MARSHALLER);

    public static final Metadata.Key<String> DATE_TIME
            = Metadata.Key.of("x-mq-date-time", Metadata.ASCII_STRING_MARSHALLER);

    public static final Metadata.Key<String> REQUEST_ID
            = Metadata.Key.of("x-mq-request-id", Metadata.ASCII_STRING_MARSHALLER);

    public static final Metadata.Key<String> LANGUAGE
            = Metadata.Key.of("x-mq-language", Metadata.ASCII_STRING_MARSHALLER);

    public static final Metadata.Key<String> CLIENT_VERSION
            = Metadata.Key.of("x-mq-client-version", Metadata.ASCII_STRING_MARSHALLER);

    public static final Metadata.Key<String> PROTOCOL_VERSION
            = Metadata.Key.of("x-mq-protocol", Metadata.ASCII_STRING_MARSHALLER);

    public static final Metadata.Key<String> RPC_NAME
            = Metadata.Key.of("x-mq-rpc-name", Metadata.ASCII_STRING_MARSHALLER);

    public static final Metadata.Key<String> SESSION_TOKEN
            = Metadata.Key.of("x-mq-session-token", Metadata.ASCII_STRING_MARSHALLER);

    public static final Metadata.Key<String> CLIENT_ID
            = Metadata.Key.of("x-mq-client-id", Metadata.ASCII_STRING_MARSHALLER);

    public static final Metadata.Key<String> AUTHORIZATION_AK
            = Metadata.Key.of("x-mq-authorization-ak", Metadata.ASCII_STRING_MARSHALLER);

    public static final Metadata.Key<String> TENANT_ID
            = Metadata.Key.of("tenant-id", Metadata.ASCII_STRING_MARSHALLER);

    public static final Metadata.Key<String> SEGMENT_ID
            = Metadata.Key.of("segment-id", Metadata.ASCII_STRING_MARSHALLER);

    public static final Metadata.Key<String> ENTRY_ID
            = Metadata.Key.of("entry-id", Metadata.ASCII_STRING_MARSHALLER);
}
