package org.catmq.protocol.service;

import static io.grpc.MethodDescriptor.generateFullMethodName;

/**
 */
@javax.annotation.Generated(
    value = "by gRPC proto compiler (version 1.51.0)",
    comments = "Source: service.proto")
@io.grpc.stub.annotations.GrpcGenerated
public final class BrokerServiceGrpc {

  private BrokerServiceGrpc() {}

  public static final String SERVICE_NAME = "BrokerService";

  // Static method descriptors that strictly reflect the proto.
  private static volatile io.grpc.MethodDescriptor<org.catmq.protocol.service.SendMessage2BrokerRequest,
      org.catmq.protocol.service.SendMessage2BrokerResponse> getSendMessage2BrokerMethod;

  @io.grpc.stub.annotations.RpcMethod(
      fullMethodName = SERVICE_NAME + '/' + "SendMessage2Broker",
      requestType = org.catmq.protocol.service.SendMessage2BrokerRequest.class,
      responseType = org.catmq.protocol.service.SendMessage2BrokerResponse.class,
      methodType = io.grpc.MethodDescriptor.MethodType.UNARY)
  public static io.grpc.MethodDescriptor<org.catmq.protocol.service.SendMessage2BrokerRequest,
      org.catmq.protocol.service.SendMessage2BrokerResponse> getSendMessage2BrokerMethod() {
    io.grpc.MethodDescriptor<org.catmq.protocol.service.SendMessage2BrokerRequest, org.catmq.protocol.service.SendMessage2BrokerResponse> getSendMessage2BrokerMethod;
    if ((getSendMessage2BrokerMethod = BrokerServiceGrpc.getSendMessage2BrokerMethod) == null) {
      synchronized (BrokerServiceGrpc.class) {
        if ((getSendMessage2BrokerMethod = BrokerServiceGrpc.getSendMessage2BrokerMethod) == null) {
          BrokerServiceGrpc.getSendMessage2BrokerMethod = getSendMessage2BrokerMethod =
              io.grpc.MethodDescriptor.<org.catmq.protocol.service.SendMessage2BrokerRequest, org.catmq.protocol.service.SendMessage2BrokerResponse>newBuilder()
              .setType(io.grpc.MethodDescriptor.MethodType.UNARY)
              .setFullMethodName(generateFullMethodName(SERVICE_NAME, "SendMessage2Broker"))
              .setSampledToLocalTracing(true)
              .setRequestMarshaller(io.grpc.protobuf.ProtoUtils.marshaller(
                  org.catmq.protocol.service.SendMessage2BrokerRequest.getDefaultInstance()))
              .setResponseMarshaller(io.grpc.protobuf.ProtoUtils.marshaller(
                  org.catmq.protocol.service.SendMessage2BrokerResponse.getDefaultInstance()))
              .setSchemaDescriptor(new BrokerServiceMethodDescriptorSupplier("SendMessage2Broker"))
              .build();
        }
      }
    }
    return getSendMessage2BrokerMethod;
  }

  private static volatile io.grpc.MethodDescriptor<org.catmq.protocol.service.GetMessageFromBrokerRequest,
      org.catmq.protocol.service.GetMessageFromBrokerResponse> getGetMessageFromBrokerMethod;

  @io.grpc.stub.annotations.RpcMethod(
      fullMethodName = SERVICE_NAME + '/' + "GetMessageFromBroker",
      requestType = org.catmq.protocol.service.GetMessageFromBrokerRequest.class,
      responseType = org.catmq.protocol.service.GetMessageFromBrokerResponse.class,
      methodType = io.grpc.MethodDescriptor.MethodType.UNARY)
  public static io.grpc.MethodDescriptor<org.catmq.protocol.service.GetMessageFromBrokerRequest,
      org.catmq.protocol.service.GetMessageFromBrokerResponse> getGetMessageFromBrokerMethod() {
    io.grpc.MethodDescriptor<org.catmq.protocol.service.GetMessageFromBrokerRequest, org.catmq.protocol.service.GetMessageFromBrokerResponse> getGetMessageFromBrokerMethod;
    if ((getGetMessageFromBrokerMethod = BrokerServiceGrpc.getGetMessageFromBrokerMethod) == null) {
      synchronized (BrokerServiceGrpc.class) {
        if ((getGetMessageFromBrokerMethod = BrokerServiceGrpc.getGetMessageFromBrokerMethod) == null) {
          BrokerServiceGrpc.getGetMessageFromBrokerMethod = getGetMessageFromBrokerMethod =
              io.grpc.MethodDescriptor.<org.catmq.protocol.service.GetMessageFromBrokerRequest, org.catmq.protocol.service.GetMessageFromBrokerResponse>newBuilder()
              .setType(io.grpc.MethodDescriptor.MethodType.UNARY)
              .setFullMethodName(generateFullMethodName(SERVICE_NAME, "GetMessageFromBroker"))
              .setSampledToLocalTracing(true)
              .setRequestMarshaller(io.grpc.protobuf.ProtoUtils.marshaller(
                  org.catmq.protocol.service.GetMessageFromBrokerRequest.getDefaultInstance()))
              .setResponseMarshaller(io.grpc.protobuf.ProtoUtils.marshaller(
                  org.catmq.protocol.service.GetMessageFromBrokerResponse.getDefaultInstance()))
              .setSchemaDescriptor(new BrokerServiceMethodDescriptorSupplier("GetMessageFromBroker"))
              .build();
        }
      }
    }
    return getGetMessageFromBrokerMethod;
  }

  /**
   * Creates a new async stub that supports all call types for the service
   */
  public static BrokerServiceStub newStub(io.grpc.Channel channel) {
    io.grpc.stub.AbstractStub.StubFactory<BrokerServiceStub> factory =
      new io.grpc.stub.AbstractStub.StubFactory<BrokerServiceStub>() {
        @java.lang.Override
        public BrokerServiceStub newStub(io.grpc.Channel channel, io.grpc.CallOptions callOptions) {
          return new BrokerServiceStub(channel, callOptions);
        }
      };
    return BrokerServiceStub.newStub(factory, channel);
  }

  /**
   * Creates a new blocking-style stub that supports unary and streaming output calls on the service
   */
  public static BrokerServiceBlockingStub newBlockingStub(
      io.grpc.Channel channel) {
    io.grpc.stub.AbstractStub.StubFactory<BrokerServiceBlockingStub> factory =
      new io.grpc.stub.AbstractStub.StubFactory<BrokerServiceBlockingStub>() {
        @java.lang.Override
        public BrokerServiceBlockingStub newStub(io.grpc.Channel channel, io.grpc.CallOptions callOptions) {
          return new BrokerServiceBlockingStub(channel, callOptions);
        }
      };
    return BrokerServiceBlockingStub.newStub(factory, channel);
  }

  /**
   * Creates a new ListenableFuture-style stub that supports unary calls on the service
   */
  public static BrokerServiceFutureStub newFutureStub(
      io.grpc.Channel channel) {
    io.grpc.stub.AbstractStub.StubFactory<BrokerServiceFutureStub> factory =
      new io.grpc.stub.AbstractStub.StubFactory<BrokerServiceFutureStub>() {
        @java.lang.Override
        public BrokerServiceFutureStub newStub(io.grpc.Channel channel, io.grpc.CallOptions callOptions) {
          return new BrokerServiceFutureStub(channel, callOptions);
        }
      };
    return BrokerServiceFutureStub.newStub(factory, channel);
  }

  /**
   */
  public static abstract class BrokerServiceImplBase implements io.grpc.BindableService {

    /**
     */
    public void sendMessage2Broker(org.catmq.protocol.service.SendMessage2BrokerRequest request,
        io.grpc.stub.StreamObserver<org.catmq.protocol.service.SendMessage2BrokerResponse> responseObserver) {
      io.grpc.stub.ServerCalls.asyncUnimplementedUnaryCall(getSendMessage2BrokerMethod(), responseObserver);
    }

    /**
     */
    public void getMessageFromBroker(org.catmq.protocol.service.GetMessageFromBrokerRequest request,
        io.grpc.stub.StreamObserver<org.catmq.protocol.service.GetMessageFromBrokerResponse> responseObserver) {
      io.grpc.stub.ServerCalls.asyncUnimplementedUnaryCall(getGetMessageFromBrokerMethod(), responseObserver);
    }

    @java.lang.Override public final io.grpc.ServerServiceDefinition bindService() {
      return io.grpc.ServerServiceDefinition.builder(getServiceDescriptor())
          .addMethod(
            getSendMessage2BrokerMethod(),
            io.grpc.stub.ServerCalls.asyncUnaryCall(
              new MethodHandlers<
                org.catmq.protocol.service.SendMessage2BrokerRequest,
                org.catmq.protocol.service.SendMessage2BrokerResponse>(
                  this, METHODID_SEND_MESSAGE2BROKER)))
          .addMethod(
            getGetMessageFromBrokerMethod(),
            io.grpc.stub.ServerCalls.asyncUnaryCall(
              new MethodHandlers<
                org.catmq.protocol.service.GetMessageFromBrokerRequest,
                org.catmq.protocol.service.GetMessageFromBrokerResponse>(
                  this, METHODID_GET_MESSAGE_FROM_BROKER)))
          .build();
    }
  }

  /**
   */
  public static final class BrokerServiceStub extends io.grpc.stub.AbstractAsyncStub<BrokerServiceStub> {
    private BrokerServiceStub(
        io.grpc.Channel channel, io.grpc.CallOptions callOptions) {
      super(channel, callOptions);
    }

    @java.lang.Override
    protected BrokerServiceStub build(
        io.grpc.Channel channel, io.grpc.CallOptions callOptions) {
      return new BrokerServiceStub(channel, callOptions);
    }

    /**
     */
    public void sendMessage2Broker(org.catmq.protocol.service.SendMessage2BrokerRequest request,
        io.grpc.stub.StreamObserver<org.catmq.protocol.service.SendMessage2BrokerResponse> responseObserver) {
      io.grpc.stub.ClientCalls.asyncUnaryCall(
          getChannel().newCall(getSendMessage2BrokerMethod(), getCallOptions()), request, responseObserver);
    }

    /**
     */
    public void getMessageFromBroker(org.catmq.protocol.service.GetMessageFromBrokerRequest request,
        io.grpc.stub.StreamObserver<org.catmq.protocol.service.GetMessageFromBrokerResponse> responseObserver) {
      io.grpc.stub.ClientCalls.asyncUnaryCall(
          getChannel().newCall(getGetMessageFromBrokerMethod(), getCallOptions()), request, responseObserver);
    }
  }

  /**
   */
  public static final class BrokerServiceBlockingStub extends io.grpc.stub.AbstractBlockingStub<BrokerServiceBlockingStub> {
    private BrokerServiceBlockingStub(
        io.grpc.Channel channel, io.grpc.CallOptions callOptions) {
      super(channel, callOptions);
    }

    @java.lang.Override
    protected BrokerServiceBlockingStub build(
        io.grpc.Channel channel, io.grpc.CallOptions callOptions) {
      return new BrokerServiceBlockingStub(channel, callOptions);
    }

    /**
     */
    public org.catmq.protocol.service.SendMessage2BrokerResponse sendMessage2Broker(org.catmq.protocol.service.SendMessage2BrokerRequest request) {
      return io.grpc.stub.ClientCalls.blockingUnaryCall(
          getChannel(), getSendMessage2BrokerMethod(), getCallOptions(), request);
    }

    /**
     */
    public org.catmq.protocol.service.GetMessageFromBrokerResponse getMessageFromBroker(org.catmq.protocol.service.GetMessageFromBrokerRequest request) {
      return io.grpc.stub.ClientCalls.blockingUnaryCall(
          getChannel(), getGetMessageFromBrokerMethod(), getCallOptions(), request);
    }
  }

  /**
   */
  public static final class BrokerServiceFutureStub extends io.grpc.stub.AbstractFutureStub<BrokerServiceFutureStub> {
    private BrokerServiceFutureStub(
        io.grpc.Channel channel, io.grpc.CallOptions callOptions) {
      super(channel, callOptions);
    }

    @java.lang.Override
    protected BrokerServiceFutureStub build(
        io.grpc.Channel channel, io.grpc.CallOptions callOptions) {
      return new BrokerServiceFutureStub(channel, callOptions);
    }

    /**
     */
    public com.google.common.util.concurrent.ListenableFuture<org.catmq.protocol.service.SendMessage2BrokerResponse> sendMessage2Broker(
        org.catmq.protocol.service.SendMessage2BrokerRequest request) {
      return io.grpc.stub.ClientCalls.futureUnaryCall(
          getChannel().newCall(getSendMessage2BrokerMethod(), getCallOptions()), request);
    }

    /**
     */
    public com.google.common.util.concurrent.ListenableFuture<org.catmq.protocol.service.GetMessageFromBrokerResponse> getMessageFromBroker(
        org.catmq.protocol.service.GetMessageFromBrokerRequest request) {
      return io.grpc.stub.ClientCalls.futureUnaryCall(
          getChannel().newCall(getGetMessageFromBrokerMethod(), getCallOptions()), request);
    }
  }

  private static final int METHODID_SEND_MESSAGE2BROKER = 0;
  private static final int METHODID_GET_MESSAGE_FROM_BROKER = 1;

  private static final class MethodHandlers<Req, Resp> implements
      io.grpc.stub.ServerCalls.UnaryMethod<Req, Resp>,
      io.grpc.stub.ServerCalls.ServerStreamingMethod<Req, Resp>,
      io.grpc.stub.ServerCalls.ClientStreamingMethod<Req, Resp>,
      io.grpc.stub.ServerCalls.BidiStreamingMethod<Req, Resp> {
    private final BrokerServiceImplBase serviceImpl;
    private final int methodId;

    MethodHandlers(BrokerServiceImplBase serviceImpl, int methodId) {
      this.serviceImpl = serviceImpl;
      this.methodId = methodId;
    }

    @java.lang.Override
    @java.lang.SuppressWarnings("unchecked")
    public void invoke(Req request, io.grpc.stub.StreamObserver<Resp> responseObserver) {
      switch (methodId) {
        case METHODID_SEND_MESSAGE2BROKER:
          serviceImpl.sendMessage2Broker((org.catmq.protocol.service.SendMessage2BrokerRequest) request,
              (io.grpc.stub.StreamObserver<org.catmq.protocol.service.SendMessage2BrokerResponse>) responseObserver);
          break;
        case METHODID_GET_MESSAGE_FROM_BROKER:
          serviceImpl.getMessageFromBroker((org.catmq.protocol.service.GetMessageFromBrokerRequest) request,
              (io.grpc.stub.StreamObserver<org.catmq.protocol.service.GetMessageFromBrokerResponse>) responseObserver);
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

  private static abstract class BrokerServiceBaseDescriptorSupplier
      implements io.grpc.protobuf.ProtoFileDescriptorSupplier, io.grpc.protobuf.ProtoServiceDescriptorSupplier {
    BrokerServiceBaseDescriptorSupplier() {}

    @java.lang.Override
    public com.google.protobuf.Descriptors.FileDescriptor getFileDescriptor() {
      return org.catmq.protocol.service.Service.getDescriptor();
    }

    @java.lang.Override
    public com.google.protobuf.Descriptors.ServiceDescriptor getServiceDescriptor() {
      return getFileDescriptor().findServiceByName("BrokerService");
    }
  }

  private static final class BrokerServiceFileDescriptorSupplier
      extends BrokerServiceBaseDescriptorSupplier {
    BrokerServiceFileDescriptorSupplier() {}
  }

  private static final class BrokerServiceMethodDescriptorSupplier
      extends BrokerServiceBaseDescriptorSupplier
      implements io.grpc.protobuf.ProtoMethodDescriptorSupplier {
    private final String methodName;

    BrokerServiceMethodDescriptorSupplier(String methodName) {
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
      synchronized (BrokerServiceGrpc.class) {
        result = serviceDescriptor;
        if (result == null) {
          serviceDescriptor = result = io.grpc.ServiceDescriptor.newBuilder(SERVICE_NAME)
              .setSchemaDescriptor(new BrokerServiceFileDescriptorSupplier())
              .addMethod(getSendMessage2BrokerMethod())
              .addMethod(getGetMessageFromBrokerMethod())
              .build();
        }
      }
    }
    return result;
  }
}
