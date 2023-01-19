package org.catmq.protocol.service;

import static io.grpc.MethodDescriptor.generateFullMethodName;

/**
 */
@javax.annotation.Generated(
    value = "by gRPC proto compiler (version 1.51.0)",
    comments = "Source: service.proto")
@io.grpc.stub.annotations.GrpcGenerated
public final class StorerServiceGrpc {

  private StorerServiceGrpc() {}

  public static final String SERVICE_NAME = "StorerService";

  // Static method descriptors that strictly reflect the proto.
  private static volatile io.grpc.MethodDescriptor<org.catmq.protocol.service.SendMessage2StorerRequest,
      org.catmq.protocol.service.SendMessage2StorerResponse> getSendMessage2StorerMethod;

  @io.grpc.stub.annotations.RpcMethod(
      fullMethodName = SERVICE_NAME + '/' + "SendMessage2Storer",
      requestType = org.catmq.protocol.service.SendMessage2StorerRequest.class,
      responseType = org.catmq.protocol.service.SendMessage2StorerResponse.class,
      methodType = io.grpc.MethodDescriptor.MethodType.UNARY)
  public static io.grpc.MethodDescriptor<org.catmq.protocol.service.SendMessage2StorerRequest,
      org.catmq.protocol.service.SendMessage2StorerResponse> getSendMessage2StorerMethod() {
    io.grpc.MethodDescriptor<org.catmq.protocol.service.SendMessage2StorerRequest, org.catmq.protocol.service.SendMessage2StorerResponse> getSendMessage2StorerMethod;
    if ((getSendMessage2StorerMethod = StorerServiceGrpc.getSendMessage2StorerMethod) == null) {
      synchronized (StorerServiceGrpc.class) {
        if ((getSendMessage2StorerMethod = StorerServiceGrpc.getSendMessage2StorerMethod) == null) {
          StorerServiceGrpc.getSendMessage2StorerMethod = getSendMessage2StorerMethod =
              io.grpc.MethodDescriptor.<org.catmq.protocol.service.SendMessage2StorerRequest, org.catmq.protocol.service.SendMessage2StorerResponse>newBuilder()
              .setType(io.grpc.MethodDescriptor.MethodType.UNARY)
              .setFullMethodName(generateFullMethodName(SERVICE_NAME, "SendMessage2Storer"))
              .setSampledToLocalTracing(true)
              .setRequestMarshaller(io.grpc.protobuf.ProtoUtils.marshaller(
                  org.catmq.protocol.service.SendMessage2StorerRequest.getDefaultInstance()))
              .setResponseMarshaller(io.grpc.protobuf.ProtoUtils.marshaller(
                  org.catmq.protocol.service.SendMessage2StorerResponse.getDefaultInstance()))
              .setSchemaDescriptor(new StorerServiceMethodDescriptorSupplier("SendMessage2Storer"))
              .build();
        }
      }
    }
    return getSendMessage2StorerMethod;
  }

  private static volatile io.grpc.MethodDescriptor<org.catmq.protocol.service.CreateSegmentRequest,
      org.catmq.protocol.service.CreateSegmentResponse> getCreateSegmentMethod;

  @io.grpc.stub.annotations.RpcMethod(
      fullMethodName = SERVICE_NAME + '/' + "CreateSegment",
      requestType = org.catmq.protocol.service.CreateSegmentRequest.class,
      responseType = org.catmq.protocol.service.CreateSegmentResponse.class,
      methodType = io.grpc.MethodDescriptor.MethodType.UNARY)
  public static io.grpc.MethodDescriptor<org.catmq.protocol.service.CreateSegmentRequest,
      org.catmq.protocol.service.CreateSegmentResponse> getCreateSegmentMethod() {
    io.grpc.MethodDescriptor<org.catmq.protocol.service.CreateSegmentRequest, org.catmq.protocol.service.CreateSegmentResponse> getCreateSegmentMethod;
    if ((getCreateSegmentMethod = StorerServiceGrpc.getCreateSegmentMethod) == null) {
      synchronized (StorerServiceGrpc.class) {
        if ((getCreateSegmentMethod = StorerServiceGrpc.getCreateSegmentMethod) == null) {
          StorerServiceGrpc.getCreateSegmentMethod = getCreateSegmentMethod =
              io.grpc.MethodDescriptor.<org.catmq.protocol.service.CreateSegmentRequest, org.catmq.protocol.service.CreateSegmentResponse>newBuilder()
              .setType(io.grpc.MethodDescriptor.MethodType.UNARY)
              .setFullMethodName(generateFullMethodName(SERVICE_NAME, "CreateSegment"))
              .setSampledToLocalTracing(true)
              .setRequestMarshaller(io.grpc.protobuf.ProtoUtils.marshaller(
                  org.catmq.protocol.service.CreateSegmentRequest.getDefaultInstance()))
              .setResponseMarshaller(io.grpc.protobuf.ProtoUtils.marshaller(
                  org.catmq.protocol.service.CreateSegmentResponse.getDefaultInstance()))
              .setSchemaDescriptor(new StorerServiceMethodDescriptorSupplier("CreateSegment"))
              .build();
        }
      }
    }
    return getCreateSegmentMethod;
  }

  /**
   * Creates a new async stub that supports all call types for the service
   */
  public static StorerServiceStub newStub(io.grpc.Channel channel) {
    io.grpc.stub.AbstractStub.StubFactory<StorerServiceStub> factory =
      new io.grpc.stub.AbstractStub.StubFactory<StorerServiceStub>() {
        @java.lang.Override
        public StorerServiceStub newStub(io.grpc.Channel channel, io.grpc.CallOptions callOptions) {
          return new StorerServiceStub(channel, callOptions);
        }
      };
    return StorerServiceStub.newStub(factory, channel);
  }

  /**
   * Creates a new blocking-style stub that supports unary and streaming output calls on the service
   */
  public static StorerServiceBlockingStub newBlockingStub(
      io.grpc.Channel channel) {
    io.grpc.stub.AbstractStub.StubFactory<StorerServiceBlockingStub> factory =
      new io.grpc.stub.AbstractStub.StubFactory<StorerServiceBlockingStub>() {
        @java.lang.Override
        public StorerServiceBlockingStub newStub(io.grpc.Channel channel, io.grpc.CallOptions callOptions) {
          return new StorerServiceBlockingStub(channel, callOptions);
        }
      };
    return StorerServiceBlockingStub.newStub(factory, channel);
  }

  /**
   * Creates a new ListenableFuture-style stub that supports unary calls on the service
   */
  public static StorerServiceFutureStub newFutureStub(
      io.grpc.Channel channel) {
    io.grpc.stub.AbstractStub.StubFactory<StorerServiceFutureStub> factory =
      new io.grpc.stub.AbstractStub.StubFactory<StorerServiceFutureStub>() {
        @java.lang.Override
        public StorerServiceFutureStub newStub(io.grpc.Channel channel, io.grpc.CallOptions callOptions) {
          return new StorerServiceFutureStub(channel, callOptions);
        }
      };
    return StorerServiceFutureStub.newStub(factory, channel);
  }

  /**
   */
  public static abstract class StorerServiceImplBase implements io.grpc.BindableService {

    /**
     */
    public void sendMessage2Storer(org.catmq.protocol.service.SendMessage2StorerRequest request,
        io.grpc.stub.StreamObserver<org.catmq.protocol.service.SendMessage2StorerResponse> responseObserver) {
      io.grpc.stub.ServerCalls.asyncUnimplementedUnaryCall(getSendMessage2StorerMethod(), responseObserver);
    }

    /**
     */
    public void createSegment(org.catmq.protocol.service.CreateSegmentRequest request,
        io.grpc.stub.StreamObserver<org.catmq.protocol.service.CreateSegmentResponse> responseObserver) {
      io.grpc.stub.ServerCalls.asyncUnimplementedUnaryCall(getCreateSegmentMethod(), responseObserver);
    }

    @java.lang.Override public final io.grpc.ServerServiceDefinition bindService() {
      return io.grpc.ServerServiceDefinition.builder(getServiceDescriptor())
          .addMethod(
            getSendMessage2StorerMethod(),
            io.grpc.stub.ServerCalls.asyncUnaryCall(
              new MethodHandlers<
                org.catmq.protocol.service.SendMessage2StorerRequest,
                org.catmq.protocol.service.SendMessage2StorerResponse>(
                  this, METHODID_SEND_MESSAGE2STORER)))
          .addMethod(
            getCreateSegmentMethod(),
            io.grpc.stub.ServerCalls.asyncUnaryCall(
              new MethodHandlers<
                org.catmq.protocol.service.CreateSegmentRequest,
                org.catmq.protocol.service.CreateSegmentResponse>(
                  this, METHODID_CREATE_SEGMENT)))
          .build();
    }
  }

  /**
   */
  public static final class StorerServiceStub extends io.grpc.stub.AbstractAsyncStub<StorerServiceStub> {
    private StorerServiceStub(
        io.grpc.Channel channel, io.grpc.CallOptions callOptions) {
      super(channel, callOptions);
    }

    @java.lang.Override
    protected StorerServiceStub build(
        io.grpc.Channel channel, io.grpc.CallOptions callOptions) {
      return new StorerServiceStub(channel, callOptions);
    }

    /**
     */
    public void sendMessage2Storer(org.catmq.protocol.service.SendMessage2StorerRequest request,
        io.grpc.stub.StreamObserver<org.catmq.protocol.service.SendMessage2StorerResponse> responseObserver) {
      io.grpc.stub.ClientCalls.asyncUnaryCall(
          getChannel().newCall(getSendMessage2StorerMethod(), getCallOptions()), request, responseObserver);
    }

    /**
     */
    public void createSegment(org.catmq.protocol.service.CreateSegmentRequest request,
        io.grpc.stub.StreamObserver<org.catmq.protocol.service.CreateSegmentResponse> responseObserver) {
      io.grpc.stub.ClientCalls.asyncUnaryCall(
          getChannel().newCall(getCreateSegmentMethod(), getCallOptions()), request, responseObserver);
    }
  }

  /**
   */
  public static final class StorerServiceBlockingStub extends io.grpc.stub.AbstractBlockingStub<StorerServiceBlockingStub> {
    private StorerServiceBlockingStub(
        io.grpc.Channel channel, io.grpc.CallOptions callOptions) {
      super(channel, callOptions);
    }

    @java.lang.Override
    protected StorerServiceBlockingStub build(
        io.grpc.Channel channel, io.grpc.CallOptions callOptions) {
      return new StorerServiceBlockingStub(channel, callOptions);
    }

    /**
     */
    public org.catmq.protocol.service.SendMessage2StorerResponse sendMessage2Storer(org.catmq.protocol.service.SendMessage2StorerRequest request) {
      return io.grpc.stub.ClientCalls.blockingUnaryCall(
          getChannel(), getSendMessage2StorerMethod(), getCallOptions(), request);
    }

    /**
     */
    public org.catmq.protocol.service.CreateSegmentResponse createSegment(org.catmq.protocol.service.CreateSegmentRequest request) {
      return io.grpc.stub.ClientCalls.blockingUnaryCall(
          getChannel(), getCreateSegmentMethod(), getCallOptions(), request);
    }
  }

  /**
   */
  public static final class StorerServiceFutureStub extends io.grpc.stub.AbstractFutureStub<StorerServiceFutureStub> {
    private StorerServiceFutureStub(
        io.grpc.Channel channel, io.grpc.CallOptions callOptions) {
      super(channel, callOptions);
    }

    @java.lang.Override
    protected StorerServiceFutureStub build(
        io.grpc.Channel channel, io.grpc.CallOptions callOptions) {
      return new StorerServiceFutureStub(channel, callOptions);
    }

    /**
     */
    public com.google.common.util.concurrent.ListenableFuture<org.catmq.protocol.service.SendMessage2StorerResponse> sendMessage2Storer(
        org.catmq.protocol.service.SendMessage2StorerRequest request) {
      return io.grpc.stub.ClientCalls.futureUnaryCall(
          getChannel().newCall(getSendMessage2StorerMethod(), getCallOptions()), request);
    }

    /**
     */
    public com.google.common.util.concurrent.ListenableFuture<org.catmq.protocol.service.CreateSegmentResponse> createSegment(
        org.catmq.protocol.service.CreateSegmentRequest request) {
      return io.grpc.stub.ClientCalls.futureUnaryCall(
          getChannel().newCall(getCreateSegmentMethod(), getCallOptions()), request);
    }
  }

  private static final int METHODID_SEND_MESSAGE2STORER = 0;
  private static final int METHODID_CREATE_SEGMENT = 1;

  private static final class MethodHandlers<Req, Resp> implements
      io.grpc.stub.ServerCalls.UnaryMethod<Req, Resp>,
      io.grpc.stub.ServerCalls.ServerStreamingMethod<Req, Resp>,
      io.grpc.stub.ServerCalls.ClientStreamingMethod<Req, Resp>,
      io.grpc.stub.ServerCalls.BidiStreamingMethod<Req, Resp> {
    private final StorerServiceImplBase serviceImpl;
    private final int methodId;

    MethodHandlers(StorerServiceImplBase serviceImpl, int methodId) {
      this.serviceImpl = serviceImpl;
      this.methodId = methodId;
    }

    @java.lang.Override
    @java.lang.SuppressWarnings("unchecked")
    public void invoke(Req request, io.grpc.stub.StreamObserver<Resp> responseObserver) {
      switch (methodId) {
        case METHODID_SEND_MESSAGE2STORER:
          serviceImpl.sendMessage2Storer((org.catmq.protocol.service.SendMessage2StorerRequest) request,
              (io.grpc.stub.StreamObserver<org.catmq.protocol.service.SendMessage2StorerResponse>) responseObserver);
          break;
        case METHODID_CREATE_SEGMENT:
          serviceImpl.createSegment((org.catmq.protocol.service.CreateSegmentRequest) request,
              (io.grpc.stub.StreamObserver<org.catmq.protocol.service.CreateSegmentResponse>) responseObserver);
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

  private static abstract class StorerServiceBaseDescriptorSupplier
      implements io.grpc.protobuf.ProtoFileDescriptorSupplier, io.grpc.protobuf.ProtoServiceDescriptorSupplier {
    StorerServiceBaseDescriptorSupplier() {}

    @java.lang.Override
    public com.google.protobuf.Descriptors.FileDescriptor getFileDescriptor() {
      return org.catmq.protocol.service.Service.getDescriptor();
    }

    @java.lang.Override
    public com.google.protobuf.Descriptors.ServiceDescriptor getServiceDescriptor() {
      return getFileDescriptor().findServiceByName("StorerService");
    }
  }

  private static final class StorerServiceFileDescriptorSupplier
      extends StorerServiceBaseDescriptorSupplier {
    StorerServiceFileDescriptorSupplier() {}
  }

  private static final class StorerServiceMethodDescriptorSupplier
      extends StorerServiceBaseDescriptorSupplier
      implements io.grpc.protobuf.ProtoMethodDescriptorSupplier {
    private final String methodName;

    StorerServiceMethodDescriptorSupplier(String methodName) {
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
      synchronized (StorerServiceGrpc.class) {
        result = serviceDescriptor;
        if (result == null) {
          serviceDescriptor = result = io.grpc.ServiceDescriptor.newBuilder(SERVICE_NAME)
              .setSchemaDescriptor(new StorerServiceFileDescriptorSupplier())
              .addMethod(getSendMessage2StorerMethod())
              .addMethod(getCreateSegmentMethod())
              .build();
        }
      }
    }
    return result;
  }
}
