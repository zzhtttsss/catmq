package org.catmq.protocol.produce;

import static io.grpc.MethodDescriptor.generateFullMethodName;

/**
 */
@javax.annotation.Generated(
    value = "by gRPC proto compiler (version 1.51.0)",
    comments = "Source: Produce.proto")
@io.grpc.stub.annotations.GrpcGenerated
public final class ProduceServiceGrpc {

  private ProduceServiceGrpc() {}

  public static final String SERVICE_NAME = "ProduceService";

  // Static method descriptors that strictly reflect the proto.
  private static volatile io.grpc.MethodDescriptor<org.catmq.protocol.produce.SendMessage2BrokerRequest,
      org.catmq.protocol.produce.SendMessage2BrokerReply> getSendMessage2BrokerMethod;

  @io.grpc.stub.annotations.RpcMethod(
      fullMethodName = SERVICE_NAME + '/' + "SendMessage2Broker",
      requestType = org.catmq.protocol.produce.SendMessage2BrokerRequest.class,
      responseType = org.catmq.protocol.produce.SendMessage2BrokerReply.class,
      methodType = io.grpc.MethodDescriptor.MethodType.UNARY)
  public static io.grpc.MethodDescriptor<org.catmq.protocol.produce.SendMessage2BrokerRequest,
      org.catmq.protocol.produce.SendMessage2BrokerReply> getSendMessage2BrokerMethod() {
    io.grpc.MethodDescriptor<org.catmq.protocol.produce.SendMessage2BrokerRequest, org.catmq.protocol.produce.SendMessage2BrokerReply> getSendMessage2BrokerMethod;
    if ((getSendMessage2BrokerMethod = ProduceServiceGrpc.getSendMessage2BrokerMethod) == null) {
      synchronized (ProduceServiceGrpc.class) {
        if ((getSendMessage2BrokerMethod = ProduceServiceGrpc.getSendMessage2BrokerMethod) == null) {
          ProduceServiceGrpc.getSendMessage2BrokerMethod = getSendMessage2BrokerMethod =
              io.grpc.MethodDescriptor.<org.catmq.protocol.produce.SendMessage2BrokerRequest, org.catmq.protocol.produce.SendMessage2BrokerReply>newBuilder()
              .setType(io.grpc.MethodDescriptor.MethodType.UNARY)
              .setFullMethodName(generateFullMethodName(SERVICE_NAME, "SendMessage2Broker"))
              .setSampledToLocalTracing(true)
              .setRequestMarshaller(io.grpc.protobuf.ProtoUtils.marshaller(
                  org.catmq.protocol.produce.SendMessage2BrokerRequest.getDefaultInstance()))
              .setResponseMarshaller(io.grpc.protobuf.ProtoUtils.marshaller(
                  org.catmq.protocol.produce.SendMessage2BrokerReply.getDefaultInstance()))
              .setSchemaDescriptor(new ProduceServiceMethodDescriptorSupplier("SendMessage2Broker"))
              .build();
        }
      }
    }
    return getSendMessage2BrokerMethod;
  }

  /**
   * Creates a new async stub that supports all call types for the service
   */
  public static ProduceServiceStub newStub(io.grpc.Channel channel) {
    io.grpc.stub.AbstractStub.StubFactory<ProduceServiceStub> factory =
      new io.grpc.stub.AbstractStub.StubFactory<ProduceServiceStub>() {
        @java.lang.Override
        public ProduceServiceStub newStub(io.grpc.Channel channel, io.grpc.CallOptions callOptions) {
          return new ProduceServiceStub(channel, callOptions);
        }
      };
    return ProduceServiceStub.newStub(factory, channel);
  }

  /**
   * Creates a new blocking-style stub that supports unary and streaming output calls on the service
   */
  public static ProduceServiceBlockingStub newBlockingStub(
      io.grpc.Channel channel) {
    io.grpc.stub.AbstractStub.StubFactory<ProduceServiceBlockingStub> factory =
      new io.grpc.stub.AbstractStub.StubFactory<ProduceServiceBlockingStub>() {
        @java.lang.Override
        public ProduceServiceBlockingStub newStub(io.grpc.Channel channel, io.grpc.CallOptions callOptions) {
          return new ProduceServiceBlockingStub(channel, callOptions);
        }
      };
    return ProduceServiceBlockingStub.newStub(factory, channel);
  }

  /**
   * Creates a new ListenableFuture-style stub that supports unary calls on the service
   */
  public static ProduceServiceFutureStub newFutureStub(
      io.grpc.Channel channel) {
    io.grpc.stub.AbstractStub.StubFactory<ProduceServiceFutureStub> factory =
      new io.grpc.stub.AbstractStub.StubFactory<ProduceServiceFutureStub>() {
        @java.lang.Override
        public ProduceServiceFutureStub newStub(io.grpc.Channel channel, io.grpc.CallOptions callOptions) {
          return new ProduceServiceFutureStub(channel, callOptions);
        }
      };
    return ProduceServiceFutureStub.newStub(factory, channel);
  }

  /**
   */
  public static abstract class ProduceServiceImplBase implements io.grpc.BindableService {

    /**
     */
    public void sendMessage2Broker(org.catmq.protocol.produce.SendMessage2BrokerRequest request,
        io.grpc.stub.StreamObserver<org.catmq.protocol.produce.SendMessage2BrokerReply> responseObserver) {
      io.grpc.stub.ServerCalls.asyncUnimplementedUnaryCall(getSendMessage2BrokerMethod(), responseObserver);
    }

    @java.lang.Override public final io.grpc.ServerServiceDefinition bindService() {
      return io.grpc.ServerServiceDefinition.builder(getServiceDescriptor())
          .addMethod(
            getSendMessage2BrokerMethod(),
            io.grpc.stub.ServerCalls.asyncUnaryCall(
              new MethodHandlers<
                org.catmq.protocol.produce.SendMessage2BrokerRequest,
                org.catmq.protocol.produce.SendMessage2BrokerReply>(
                  this, METHODID_SEND_MESSAGE2BROKER)))
          .build();
    }
  }

  /**
   */
  public static final class ProduceServiceStub extends io.grpc.stub.AbstractAsyncStub<ProduceServiceStub> {
    private ProduceServiceStub(
        io.grpc.Channel channel, io.grpc.CallOptions callOptions) {
      super(channel, callOptions);
    }

    @java.lang.Override
    protected ProduceServiceStub build(
        io.grpc.Channel channel, io.grpc.CallOptions callOptions) {
      return new ProduceServiceStub(channel, callOptions);
    }

    /**
     */
    public void sendMessage2Broker(org.catmq.protocol.produce.SendMessage2BrokerRequest request,
        io.grpc.stub.StreamObserver<org.catmq.protocol.produce.SendMessage2BrokerReply> responseObserver) {
      io.grpc.stub.ClientCalls.asyncUnaryCall(
          getChannel().newCall(getSendMessage2BrokerMethod(), getCallOptions()), request, responseObserver);
    }
  }

  /**
   */
  public static final class ProduceServiceBlockingStub extends io.grpc.stub.AbstractBlockingStub<ProduceServiceBlockingStub> {
    private ProduceServiceBlockingStub(
        io.grpc.Channel channel, io.grpc.CallOptions callOptions) {
      super(channel, callOptions);
    }

    @java.lang.Override
    protected ProduceServiceBlockingStub build(
        io.grpc.Channel channel, io.grpc.CallOptions callOptions) {
      return new ProduceServiceBlockingStub(channel, callOptions);
    }

    /**
     */
    public org.catmq.protocol.produce.SendMessage2BrokerReply sendMessage2Broker(org.catmq.protocol.produce.SendMessage2BrokerRequest request) {
      return io.grpc.stub.ClientCalls.blockingUnaryCall(
          getChannel(), getSendMessage2BrokerMethod(), getCallOptions(), request);
    }
  }

  /**
   */
  public static final class ProduceServiceFutureStub extends io.grpc.stub.AbstractFutureStub<ProduceServiceFutureStub> {
    private ProduceServiceFutureStub(
        io.grpc.Channel channel, io.grpc.CallOptions callOptions) {
      super(channel, callOptions);
    }

    @java.lang.Override
    protected ProduceServiceFutureStub build(
        io.grpc.Channel channel, io.grpc.CallOptions callOptions) {
      return new ProduceServiceFutureStub(channel, callOptions);
    }

    /**
     */
    public com.google.common.util.concurrent.ListenableFuture<org.catmq.protocol.produce.SendMessage2BrokerReply> sendMessage2Broker(
        org.catmq.protocol.produce.SendMessage2BrokerRequest request) {
      return io.grpc.stub.ClientCalls.futureUnaryCall(
          getChannel().newCall(getSendMessage2BrokerMethod(), getCallOptions()), request);
    }
  }

  private static final int METHODID_SEND_MESSAGE2BROKER = 0;

  private static final class MethodHandlers<Req, Resp> implements
      io.grpc.stub.ServerCalls.UnaryMethod<Req, Resp>,
      io.grpc.stub.ServerCalls.ServerStreamingMethod<Req, Resp>,
      io.grpc.stub.ServerCalls.ClientStreamingMethod<Req, Resp>,
      io.grpc.stub.ServerCalls.BidiStreamingMethod<Req, Resp> {
    private final ProduceServiceImplBase serviceImpl;
    private final int methodId;

    MethodHandlers(ProduceServiceImplBase serviceImpl, int methodId) {
      this.serviceImpl = serviceImpl;
      this.methodId = methodId;
    }

    @java.lang.Override
    @java.lang.SuppressWarnings("unchecked")
    public void invoke(Req request, io.grpc.stub.StreamObserver<Resp> responseObserver) {
      switch (methodId) {
        case METHODID_SEND_MESSAGE2BROKER:
          serviceImpl.sendMessage2Broker((org.catmq.protocol.produce.SendMessage2BrokerRequest) request,
              (io.grpc.stub.StreamObserver<org.catmq.protocol.produce.SendMessage2BrokerReply>) responseObserver);
          break;
        default:
          throw new AssertionError();
      }
    }

    @java.lang.Override
    @java.lang.SuppressWarnings("unchecked")
    public io.grpc.stub.StreamObserver<Req> invoke(
        io.grpc.stub.StreamObserver<Resp> responseObserver) {
      switch (methodId) {
        default:
          throw new AssertionError();
      }
    }
  }

  private static abstract class ProduceServiceBaseDescriptorSupplier
      implements io.grpc.protobuf.ProtoFileDescriptorSupplier, io.grpc.protobuf.ProtoServiceDescriptorSupplier {
    ProduceServiceBaseDescriptorSupplier() {}

    @java.lang.Override
    public com.google.protobuf.Descriptors.FileDescriptor getFileDescriptor() {
      return org.catmq.protocol.produce.Produce.getDescriptor();
    }

    @java.lang.Override
    public com.google.protobuf.Descriptors.ServiceDescriptor getServiceDescriptor() {
      return getFileDescriptor().findServiceByName("ProduceService");
    }
  }

  private static final class ProduceServiceFileDescriptorSupplier
      extends ProduceServiceBaseDescriptorSupplier {
    ProduceServiceFileDescriptorSupplier() {}
  }

  private static final class ProduceServiceMethodDescriptorSupplier
      extends ProduceServiceBaseDescriptorSupplier
      implements io.grpc.protobuf.ProtoMethodDescriptorSupplier {
    private final String methodName;

    ProduceServiceMethodDescriptorSupplier(String methodName) {
      this.methodName = methodName;
    }

    @java.lang.Override
    public com.google.protobuf.Descriptors.MethodDescriptor getMethodDescriptor() {
      return getServiceDescriptor().findMethodByName(methodName);
    }
  }

  private static volatile io.grpc.ServiceDescriptor serviceDescriptor;

  public static io.grpc.ServiceDescriptor getServiceDescriptor() {
    io.grpc.ServiceDescriptor result = serviceDescriptor;
    if (result == null) {
      synchronized (ProduceServiceGrpc.class) {
        result = serviceDescriptor;
        if (result == null) {
          serviceDescriptor = result = io.grpc.ServiceDescriptor.newBuilder(SERVICE_NAME)
              .setSchemaDescriptor(new ProduceServiceFileDescriptorSupplier())
              .addMethod(getSendMessage2BrokerMethod())
              .build();
        }
      }
    }
    return result;
  }
}
