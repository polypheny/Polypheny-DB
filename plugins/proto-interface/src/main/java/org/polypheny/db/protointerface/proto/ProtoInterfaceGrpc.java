package org.polypheny.db.protointerface.proto;

import static io.grpc.MethodDescriptor.generateFullMethodName;

/**
 */
@javax.annotation.Generated(
    value = "by gRPC proto compiler (version 1.55.1)",
    comments = "Source: Polypheny-DB/plugins/proto-interface/src/main/proto/protointerface.proto")
@io.grpc.stub.annotations.GrpcGenerated
public final class ProtoInterfaceGrpc {

  private ProtoInterfaceGrpc() {}

  public static final String SERVICE_NAME = "org.polypheny.db.protointerface.proto.ProtoInterface";

  // Static method descriptors that strictly reflect the proto.
  private static volatile io.grpc.MethodDescriptor<ConnectionRequest,
      ConnectionReply> getConnectMethod;

  @io.grpc.stub.annotations.RpcMethod(
      fullMethodName = SERVICE_NAME + '/' + "connect",
      requestType = ConnectionRequest.class,
      responseType = ConnectionReply.class,
      methodType = io.grpc.MethodDescriptor.MethodType.UNARY)
  public static io.grpc.MethodDescriptor<ConnectionRequest,
      ConnectionReply> getConnectMethod() {
    io.grpc.MethodDescriptor<ConnectionRequest, ConnectionReply> getConnectMethod;
    if ((getConnectMethod = ProtoInterfaceGrpc.getConnectMethod) == null) {
      synchronized (ProtoInterfaceGrpc.class) {
        if ((getConnectMethod = ProtoInterfaceGrpc.getConnectMethod) == null) {
          ProtoInterfaceGrpc.getConnectMethod = getConnectMethod =
              io.grpc.MethodDescriptor.<ConnectionRequest, ConnectionReply>newBuilder()
              .setType(io.grpc.MethodDescriptor.MethodType.UNARY)
              .setFullMethodName(generateFullMethodName(SERVICE_NAME, "connect"))
              .setSampledToLocalTracing(true)
              .setRequestMarshaller(io.grpc.protobuf.ProtoUtils.marshaller(
                  ConnectionRequest.getDefaultInstance()))
              .setResponseMarshaller(io.grpc.protobuf.ProtoUtils.marshaller(
                  ConnectionReply.getDefaultInstance()))
              .setSchemaDescriptor(new ProtoInterfaceMethodDescriptorSupplier("connect"))
              .build();
        }
      }
    }
    return getConnectMethod;
  }

  private static volatile io.grpc.MethodDescriptor<SimpleSqlQuery,
          QueryResult> getExecuteSimpleSqlQueryMethod;

  @io.grpc.stub.annotations.RpcMethod(
      fullMethodName = SERVICE_NAME + '/' + "executeSimpleSqlQuery",
      requestType = SimpleSqlQuery.class,
      responseType = QueryResult.class,
      methodType = io.grpc.MethodDescriptor.MethodType.UNARY)
  public static io.grpc.MethodDescriptor<SimpleSqlQuery,
          QueryResult> getExecuteSimpleSqlQueryMethod() {
    io.grpc.MethodDescriptor<SimpleSqlQuery, QueryResult> getExecuteSimpleSqlQueryMethod;
    if ((getExecuteSimpleSqlQueryMethod = ProtoInterfaceGrpc.getExecuteSimpleSqlQueryMethod) == null) {
      synchronized (ProtoInterfaceGrpc.class) {
        if ((getExecuteSimpleSqlQueryMethod = ProtoInterfaceGrpc.getExecuteSimpleSqlQueryMethod) == null) {
          ProtoInterfaceGrpc.getExecuteSimpleSqlQueryMethod = getExecuteSimpleSqlQueryMethod =
              io.grpc.MethodDescriptor.<SimpleSqlQuery, QueryResult>newBuilder()
              .setType(io.grpc.MethodDescriptor.MethodType.UNARY)
              .setFullMethodName(generateFullMethodName(SERVICE_NAME, "executeSimpleSqlQuery"))
              .setSampledToLocalTracing(true)
              .setRequestMarshaller(io.grpc.protobuf.ProtoUtils.marshaller(
                  SimpleSqlQuery.getDefaultInstance()))
              .setResponseMarshaller(io.grpc.protobuf.ProtoUtils.marshaller(
                  QueryResult.getDefaultInstance()))
              .setSchemaDescriptor(new ProtoInterfaceMethodDescriptorSupplier("executeSimpleSqlQuery"))
              .build();
        }
      }
    }
    return getExecuteSimpleSqlQueryMethod;
  }

  /**
   * Creates a new async stub that supports all call types for the service
   */
  public static ProtoInterfaceStub newStub(io.grpc.Channel channel) {
    io.grpc.stub.AbstractStub.StubFactory<ProtoInterfaceStub> factory =
      new io.grpc.stub.AbstractStub.StubFactory<ProtoInterfaceStub>() {
        @Override
        public ProtoInterfaceStub newStub(io.grpc.Channel channel, io.grpc.CallOptions callOptions) {
          return new ProtoInterfaceStub(channel, callOptions);
        }
      };
    return ProtoInterfaceStub.newStub(factory, channel);
  }

  /**
   * Creates a new blocking-style stub that supports unary and streaming output calls on the service
   */
  public static ProtoInterfaceBlockingStub newBlockingStub(
      io.grpc.Channel channel) {
    io.grpc.stub.AbstractStub.StubFactory<ProtoInterfaceBlockingStub> factory =
      new io.grpc.stub.AbstractStub.StubFactory<ProtoInterfaceBlockingStub>() {
        @Override
        public ProtoInterfaceBlockingStub newStub(io.grpc.Channel channel, io.grpc.CallOptions callOptions) {
          return new ProtoInterfaceBlockingStub(channel, callOptions);
        }
      };
    return ProtoInterfaceBlockingStub.newStub(factory, channel);
  }

  /**
   * Creates a new ListenableFuture-style stub that supports unary calls on the service
   */
  public static ProtoInterfaceFutureStub newFutureStub(
      io.grpc.Channel channel) {
    io.grpc.stub.AbstractStub.StubFactory<ProtoInterfaceFutureStub> factory =
      new io.grpc.stub.AbstractStub.StubFactory<ProtoInterfaceFutureStub>() {
        @Override
        public ProtoInterfaceFutureStub newStub(io.grpc.Channel channel, io.grpc.CallOptions callOptions) {
          return new ProtoInterfaceFutureStub(channel, callOptions);
        }
      };
    return ProtoInterfaceFutureStub.newStub(factory, channel);
  }

  /**
   */
  public interface AsyncService {

    /**
     * <pre>
     * Connect to server by sending a ConnectionRequest.
     * </pre>
     */
    default void connect(ConnectionRequest request,
        io.grpc.stub.StreamObserver<ConnectionReply> responseObserver) {
      io.grpc.stub.ServerCalls.asyncUnimplementedUnaryCall(getConnectMethod(), responseObserver);
    }

    /**
     */
    default void executeSimpleSqlQuery(SimpleSqlQuery request,
                                       io.grpc.stub.StreamObserver<QueryResult> responseObserver) {
      io.grpc.stub.ServerCalls.asyncUnimplementedUnaryCall(getExecuteSimpleSqlQueryMethod(), responseObserver);
    }
  }

  /**
   * Base class for the server implementation of the service ProtoInterface.
   */
  public static abstract class ProtoInterfaceImplBase
      implements io.grpc.BindableService, AsyncService {

    @Override public final io.grpc.ServerServiceDefinition bindService() {
      return ProtoInterfaceGrpc.bindService(this);
    }
  }

  /**
   * A stub to allow clients to do asynchronous rpc calls to service ProtoInterface.
   */
  public static final class ProtoInterfaceStub
      extends io.grpc.stub.AbstractAsyncStub<ProtoInterfaceStub> {
    private ProtoInterfaceStub(
        io.grpc.Channel channel, io.grpc.CallOptions callOptions) {
      super(channel, callOptions);
    }

    @Override
    protected ProtoInterfaceStub build(
        io.grpc.Channel channel, io.grpc.CallOptions callOptions) {
      return new ProtoInterfaceStub(channel, callOptions);
    }

    /**
     * <pre>
     * Connect to server by sending a ConnectionRequest.
     * </pre>
     */
    public void connect(ConnectionRequest request,
        io.grpc.stub.StreamObserver<ConnectionReply> responseObserver) {
      io.grpc.stub.ClientCalls.asyncUnaryCall(
          getChannel().newCall(getConnectMethod(), getCallOptions()), request, responseObserver);
    }

    /**
     */
    public void executeSimpleSqlQuery(SimpleSqlQuery request,
                                      io.grpc.stub.StreamObserver<QueryResult> responseObserver) {
      io.grpc.stub.ClientCalls.asyncUnaryCall(
          getChannel().newCall(getExecuteSimpleSqlQueryMethod(), getCallOptions()), request, responseObserver);
    }
  }

  /**
   * A stub to allow clients to do synchronous rpc calls to service ProtoInterface.
   */
  public static final class ProtoInterfaceBlockingStub
      extends io.grpc.stub.AbstractBlockingStub<ProtoInterfaceBlockingStub> {
    private ProtoInterfaceBlockingStub(
        io.grpc.Channel channel, io.grpc.CallOptions callOptions) {
      super(channel, callOptions);
    }

    @Override
    protected ProtoInterfaceBlockingStub build(
        io.grpc.Channel channel, io.grpc.CallOptions callOptions) {
      return new ProtoInterfaceBlockingStub(channel, callOptions);
    }

    /**
     * <pre>
     * Connect to server by sending a ConnectionRequest.
     * </pre>
     */
    public ConnectionReply connect(ConnectionRequest request) {
      return io.grpc.stub.ClientCalls.blockingUnaryCall(
          getChannel(), getConnectMethod(), getCallOptions(), request);
    }

    /**
     */
    public QueryResult executeSimpleSqlQuery(SimpleSqlQuery request) {
      return io.grpc.stub.ClientCalls.blockingUnaryCall(
          getChannel(), getExecuteSimpleSqlQueryMethod(), getCallOptions(), request);
    }
  }

  /**
   * A stub to allow clients to do ListenableFuture-style rpc calls to service ProtoInterface.
   */
  public static final class ProtoInterfaceFutureStub
      extends io.grpc.stub.AbstractFutureStub<ProtoInterfaceFutureStub> {
    private ProtoInterfaceFutureStub(
        io.grpc.Channel channel, io.grpc.CallOptions callOptions) {
      super(channel, callOptions);
    }

    @Override
    protected ProtoInterfaceFutureStub build(
        io.grpc.Channel channel, io.grpc.CallOptions callOptions) {
      return new ProtoInterfaceFutureStub(channel, callOptions);
    }

    /**
     * <pre>
     * Connect to server by sending a ConnectionRequest.
     * </pre>
     */
    public com.google.common.util.concurrent.ListenableFuture<ConnectionReply> connect(
        ConnectionRequest request) {
      return io.grpc.stub.ClientCalls.futureUnaryCall(
          getChannel().newCall(getConnectMethod(), getCallOptions()), request);
    }

    /**
     */
    public com.google.common.util.concurrent.ListenableFuture<QueryResult> executeSimpleSqlQuery(
        SimpleSqlQuery request) {
      return io.grpc.stub.ClientCalls.futureUnaryCall(
          getChannel().newCall(getExecuteSimpleSqlQueryMethod(), getCallOptions()), request);
    }
  }

  private static final int METHODID_CONNECT = 0;
  private static final int METHODID_EXECUTE_SIMPLE_SQL_QUERY = 1;

  private static final class MethodHandlers<Req, Resp> implements
      io.grpc.stub.ServerCalls.UnaryMethod<Req, Resp>,
      io.grpc.stub.ServerCalls.ServerStreamingMethod<Req, Resp>,
      io.grpc.stub.ServerCalls.ClientStreamingMethod<Req, Resp>,
      io.grpc.stub.ServerCalls.BidiStreamingMethod<Req, Resp> {
    private final AsyncService serviceImpl;
    private final int methodId;

    MethodHandlers(AsyncService serviceImpl, int methodId) {
      this.serviceImpl = serviceImpl;
      this.methodId = methodId;
    }

    @Override
    @SuppressWarnings("unchecked")
    public void invoke(Req request, io.grpc.stub.StreamObserver<Resp> responseObserver) {
      switch (methodId) {
        case METHODID_CONNECT:
          serviceImpl.connect((ConnectionRequest) request,
              (io.grpc.stub.StreamObserver<ConnectionReply>) responseObserver);
          break;
        case METHODID_EXECUTE_SIMPLE_SQL_QUERY:
          serviceImpl.executeSimpleSqlQuery((SimpleSqlQuery) request,
              (io.grpc.stub.StreamObserver<QueryResult>) responseObserver);
          break;
        default:
          throw new AssertionError();
      }
    }

    @Override
    @SuppressWarnings("unchecked")
    public io.grpc.stub.StreamObserver<Req> invoke(
        io.grpc.stub.StreamObserver<Resp> responseObserver) {
      switch (methodId) {
        default:
          throw new AssertionError();
      }
    }
  }

  public static final io.grpc.ServerServiceDefinition bindService(AsyncService service) {
    return io.grpc.ServerServiceDefinition.builder(getServiceDescriptor())
        .addMethod(
          getConnectMethod(),
          io.grpc.stub.ServerCalls.asyncUnaryCall(
            new MethodHandlers<
              ConnectionRequest,
              ConnectionReply>(
                service, METHODID_CONNECT)))
        .addMethod(
          getExecuteSimpleSqlQueryMethod(),
          io.grpc.stub.ServerCalls.asyncUnaryCall(
            new MethodHandlers<
                    SimpleSqlQuery,
                    QueryResult>(
                service, METHODID_EXECUTE_SIMPLE_SQL_QUERY)))
        .build();
  }

  private static abstract class ProtoInterfaceBaseDescriptorSupplier
      implements io.grpc.protobuf.ProtoFileDescriptorSupplier, io.grpc.protobuf.ProtoServiceDescriptorSupplier {
    ProtoInterfaceBaseDescriptorSupplier() {}

    @Override
    public com.google.protobuf.Descriptors.FileDescriptor getFileDescriptor() {
      return ProtoInterfaceProto.getDescriptor();
    }

    @Override
    public com.google.protobuf.Descriptors.ServiceDescriptor getServiceDescriptor() {
      return getFileDescriptor().findServiceByName("ProtoInterface");
    }
  }

  private static final class ProtoInterfaceFileDescriptorSupplier
      extends ProtoInterfaceBaseDescriptorSupplier {
    ProtoInterfaceFileDescriptorSupplier() {}
  }

  private static final class ProtoInterfaceMethodDescriptorSupplier
      extends ProtoInterfaceBaseDescriptorSupplier
      implements io.grpc.protobuf.ProtoMethodDescriptorSupplier {
    private final String methodName;

    ProtoInterfaceMethodDescriptorSupplier(String methodName) {
      this.methodName = methodName;
    }

    @Override
    public com.google.protobuf.Descriptors.MethodDescriptor getMethodDescriptor() {
      return getServiceDescriptor().findMethodByName(methodName);
    }
  }

  private static volatile io.grpc.ServiceDescriptor serviceDescriptor;

  public static io.grpc.ServiceDescriptor getServiceDescriptor() {
    io.grpc.ServiceDescriptor result = serviceDescriptor;
    if (result == null) {
      synchronized (ProtoInterfaceGrpc.class) {
        result = serviceDescriptor;
        if (result == null) {
          serviceDescriptor = result = io.grpc.ServiceDescriptor.newBuilder(SERVICE_NAME)
              .setSchemaDescriptor(new ProtoInterfaceFileDescriptorSupplier())
              .addMethod(getConnectMethod())
              .addMethod(getExecuteSimpleSqlQueryMethod())
              .build();
        }
      }
    }
    return result;
  }
}
