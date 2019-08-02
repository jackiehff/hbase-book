// Generated by the protocol buffer compiler.  DO NOT EDIT!
// source: ScanControlService.proto

package coprocessor.generated;

public final class ScanControlProtos {
  private ScanControlProtos() {}
  public static void registerAllExtensions(
      com.google.protobuf.ExtensionRegistry registry) {
  }
  public interface ScanControlRequestOrBuilder
      extends com.google.protobuf.MessageOrBuilder {
  }
  public static final class ScanControlRequest extends
      com.google.protobuf.GeneratedMessage
      implements ScanControlRequestOrBuilder {
    // Use ScanControlRequest.newBuilder() to construct.
    private ScanControlRequest(Builder builder) {
      super(builder);
    }
    private ScanControlRequest(boolean noInit) {}
    
    private static final ScanControlRequest defaultInstance;
    public static ScanControlRequest getDefaultInstance() {
      return defaultInstance;
    }
    
    public ScanControlRequest getDefaultInstanceForType() {
      return defaultInstance;
    }
    
    public static final com.google.protobuf.Descriptors.Descriptor
        getDescriptor() {
      return coprocessor.generated.ScanControlProtos.internal_static_ScanControlRequest_descriptor;
    }
    
    protected com.google.protobuf.GeneratedMessage.FieldAccessorTable
        internalGetFieldAccessorTable() {
      return coprocessor.generated.ScanControlProtos.internal_static_ScanControlRequest_fieldAccessorTable;
    }
    
    private void initFields() {
    }
    private byte memoizedIsInitialized = -1;
    public final boolean isInitialized() {
      byte isInitialized = memoizedIsInitialized;
      if (isInitialized != -1){
        return isInitialized == 1;
      }
      
      memoizedIsInitialized = 1;
      return true;
    }
    
    public void writeTo(com.google.protobuf.CodedOutputStream output)
                        throws java.io.IOException {
      getSerializedSize();
      getUnknownFields().writeTo(output);
    }
    
    private int memoizedSerializedSize = -1;
    public int getSerializedSize() {
      int size = memoizedSerializedSize;
      if (size != -1) {
        return size;
      }
    
      size = 0;
      size += getUnknownFields().getSerializedSize();
      memoizedSerializedSize = size;
      return size;
    }
    
    private static final long serialVersionUID = 0L;
    @java.lang.Override
    protected java.lang.Object writeReplace()
        throws java.io.ObjectStreamException {
      return super.writeReplace();
    }
    
    @java.lang.Override
    public boolean equals(final java.lang.Object obj) {
      if (obj == this) {
       return true;
      }
      if (!(obj instanceof coprocessor.generated.ScanControlProtos.ScanControlRequest)) {
        return super.equals(obj);
      }
      coprocessor.generated.ScanControlProtos.ScanControlRequest other = (coprocessor.generated.ScanControlProtos.ScanControlRequest) obj;
      
      boolean result = true;
      result = result &&
          getUnknownFields().equals(other.getUnknownFields());
      return result;
    }
    
    @java.lang.Override
    public int hashCode() {
      int hash = 41;
      hash = (19 * hash) + getDescriptorForType().hashCode();
      hash = (29 * hash) + getUnknownFields().hashCode();
      return hash;
    }
    
    public static coprocessor.generated.ScanControlProtos.ScanControlRequest parseFrom(
        com.google.protobuf.ByteString data)
        throws com.google.protobuf.InvalidProtocolBufferException {
      return newBuilder().mergeFrom(data).buildParsed();
    }
    public static coprocessor.generated.ScanControlProtos.ScanControlRequest parseFrom(
        com.google.protobuf.ByteString data,
        com.google.protobuf.ExtensionRegistryLite extensionRegistry)
        throws com.google.protobuf.InvalidProtocolBufferException {
      return newBuilder().mergeFrom(data, extensionRegistry)
               .buildParsed();
    }
    public static coprocessor.generated.ScanControlProtos.ScanControlRequest parseFrom(byte[] data)
        throws com.google.protobuf.InvalidProtocolBufferException {
      return newBuilder().mergeFrom(data).buildParsed();
    }
    public static coprocessor.generated.ScanControlProtos.ScanControlRequest parseFrom(
        byte[] data,
        com.google.protobuf.ExtensionRegistryLite extensionRegistry)
        throws com.google.protobuf.InvalidProtocolBufferException {
      return newBuilder().mergeFrom(data, extensionRegistry)
               .buildParsed();
    }
    public static coprocessor.generated.ScanControlProtos.ScanControlRequest parseFrom(java.io.InputStream input)
        throws java.io.IOException {
      return newBuilder().mergeFrom(input).buildParsed();
    }
    public static coprocessor.generated.ScanControlProtos.ScanControlRequest parseFrom(
        java.io.InputStream input,
        com.google.protobuf.ExtensionRegistryLite extensionRegistry)
        throws java.io.IOException {
      return newBuilder().mergeFrom(input, extensionRegistry)
               .buildParsed();
    }
    public static coprocessor.generated.ScanControlProtos.ScanControlRequest parseDelimitedFrom(java.io.InputStream input)
        throws java.io.IOException {
      Builder builder = newBuilder();
      if (builder.mergeDelimitedFrom(input)) {
        return builder.buildParsed();
      } else {
        return null;
      }
    }
    public static coprocessor.generated.ScanControlProtos.ScanControlRequest parseDelimitedFrom(
        java.io.InputStream input,
        com.google.protobuf.ExtensionRegistryLite extensionRegistry)
        throws java.io.IOException {
      Builder builder = newBuilder();
      if (builder.mergeDelimitedFrom(input, extensionRegistry)) {
        return builder.buildParsed();
      } else {
        return null;
      }
    }
    public static coprocessor.generated.ScanControlProtos.ScanControlRequest parseFrom(
        com.google.protobuf.CodedInputStream input)
        throws java.io.IOException {
      return newBuilder().mergeFrom(input).buildParsed();
    }
    public static coprocessor.generated.ScanControlProtos.ScanControlRequest parseFrom(
        com.google.protobuf.CodedInputStream input,
        com.google.protobuf.ExtensionRegistryLite extensionRegistry)
        throws java.io.IOException {
      return newBuilder().mergeFrom(input, extensionRegistry)
               .buildParsed();
    }
    
    public static Builder newBuilder() { return Builder.create(); }
    public Builder newBuilderForType() { return newBuilder(); }
    public static Builder newBuilder(coprocessor.generated.ScanControlProtos.ScanControlRequest prototype) {
      return newBuilder().mergeFrom(prototype);
    }
    public Builder toBuilder() { return newBuilder(this); }
    
    @java.lang.Override
    protected Builder newBuilderForType(
        com.google.protobuf.GeneratedMessage.BuilderParent parent) {
      Builder builder = new Builder(parent);
      return builder;
    }
    public static final class Builder extends
        com.google.protobuf.GeneratedMessage.Builder<Builder>
       implements coprocessor.generated.ScanControlProtos.ScanControlRequestOrBuilder {
      public static final com.google.protobuf.Descriptors.Descriptor
          getDescriptor() {
        return coprocessor.generated.ScanControlProtos.internal_static_ScanControlRequest_descriptor;
      }
      
      protected com.google.protobuf.GeneratedMessage.FieldAccessorTable
          internalGetFieldAccessorTable() {
        return coprocessor.generated.ScanControlProtos.internal_static_ScanControlRequest_fieldAccessorTable;
      }
      
      // Construct using coprocessor.generated.ScanControlProtos.ScanControlRequest.newBuilder()
      private Builder() {
        maybeForceBuilderInitialization();
      }
      
      private Builder(BuilderParent parent) {
        super(parent);
        maybeForceBuilderInitialization();
      }
      private void maybeForceBuilderInitialization() {
        if (com.google.protobuf.GeneratedMessage.alwaysUseFieldBuilders) {
        }
      }
      private static Builder create() {
        return new Builder();
      }
      
      public Builder clear() {
        super.clear();
        return this;
      }
      
      public Builder clone() {
        return create().mergeFrom(buildPartial());
      }
      
      public com.google.protobuf.Descriptors.Descriptor
          getDescriptorForType() {
        return coprocessor.generated.ScanControlProtos.ScanControlRequest.getDescriptor();
      }
      
      public coprocessor.generated.ScanControlProtos.ScanControlRequest getDefaultInstanceForType() {
        return coprocessor.generated.ScanControlProtos.ScanControlRequest.getDefaultInstance();
      }
      
      public coprocessor.generated.ScanControlProtos.ScanControlRequest build() {
        coprocessor.generated.ScanControlProtos.ScanControlRequest result = buildPartial();
        if (!result.isInitialized()) {
          throw newUninitializedMessageException(result);
        }
        return result;
      }
      
      private coprocessor.generated.ScanControlProtos.ScanControlRequest buildParsed()
          throws com.google.protobuf.InvalidProtocolBufferException {
        coprocessor.generated.ScanControlProtos.ScanControlRequest result = buildPartial();
        if (!result.isInitialized()) {
          throw newUninitializedMessageException(
            result).asInvalidProtocolBufferException();
        }
        return result;
      }
      
      public coprocessor.generated.ScanControlProtos.ScanControlRequest buildPartial() {
        coprocessor.generated.ScanControlProtos.ScanControlRequest result = new coprocessor.generated.ScanControlProtos.ScanControlRequest(this);
        onBuilt();
        return result;
      }
      
      public Builder mergeFrom(com.google.protobuf.Message other) {
        if (other instanceof coprocessor.generated.ScanControlProtos.ScanControlRequest) {
          return mergeFrom((coprocessor.generated.ScanControlProtos.ScanControlRequest)other);
        } else {
          super.mergeFrom(other);
          return this;
        }
      }
      
      public Builder mergeFrom(coprocessor.generated.ScanControlProtos.ScanControlRequest other) {
        if (other == coprocessor.generated.ScanControlProtos.ScanControlRequest.getDefaultInstance()) {
          return this;
        }
        this.mergeUnknownFields(other.getUnknownFields());
        return this;
      }
      
      public final boolean isInitialized() {
        return true;
      }
      
      public Builder mergeFrom(
          com.google.protobuf.CodedInputStream input,
          com.google.protobuf.ExtensionRegistryLite extensionRegistry)
          throws java.io.IOException {
        com.google.protobuf.UnknownFieldSet.Builder unknownFields =
          com.google.protobuf.UnknownFieldSet.newBuilder(
            this.getUnknownFields());
        while (true) {
          int tag = input.readTag();
          switch (tag) {
            case 0:
              this.setUnknownFields(unknownFields.build());
              onChanged();
              return this;
            default: {
              if (!parseUnknownField(input, unknownFields,
                                     extensionRegistry, tag)) {
                this.setUnknownFields(unknownFields.build());
                onChanged();
                return this;
              }
              break;
            }
          }
        }
      }
      
      
      // @@protoc_insertion_point(builder_scope:ScanControlRequest)
    }
    
    static {
      defaultInstance = new ScanControlRequest(true);
      defaultInstance.initFields();
    }
    
    // @@protoc_insertion_point(class_scope:ScanControlRequest)
  }
  
  public interface ScanControlResponseOrBuilder
      extends com.google.protobuf.MessageOrBuilder {
  }
  public static final class ScanControlResponse extends
      com.google.protobuf.GeneratedMessage
      implements ScanControlResponseOrBuilder {
    // Use ScanControlResponse.newBuilder() to construct.
    private ScanControlResponse(Builder builder) {
      super(builder);
    }
    private ScanControlResponse(boolean noInit) {}
    
    private static final ScanControlResponse defaultInstance;
    public static ScanControlResponse getDefaultInstance() {
      return defaultInstance;
    }
    
    public ScanControlResponse getDefaultInstanceForType() {
      return defaultInstance;
    }
    
    public static final com.google.protobuf.Descriptors.Descriptor
        getDescriptor() {
      return coprocessor.generated.ScanControlProtos.internal_static_ScanControlResponse_descriptor;
    }
    
    protected com.google.protobuf.GeneratedMessage.FieldAccessorTable
        internalGetFieldAccessorTable() {
      return coprocessor.generated.ScanControlProtos.internal_static_ScanControlResponse_fieldAccessorTable;
    }
    
    private void initFields() {
    }
    private byte memoizedIsInitialized = -1;
    public final boolean isInitialized() {
      byte isInitialized = memoizedIsInitialized;
      if (isInitialized != -1) {
        return isInitialized == 1;
      }
      
      memoizedIsInitialized = 1;
      return true;
    }
    
    public void writeTo(com.google.protobuf.CodedOutputStream output)
                        throws java.io.IOException {
      getSerializedSize();
      getUnknownFields().writeTo(output);
    }
    
    private int memoizedSerializedSize = -1;
    public int getSerializedSize() {
      int size = memoizedSerializedSize;
      if (size != -1) {
        return size;
      }
    
      size = 0;
      size += getUnknownFields().getSerializedSize();
      memoizedSerializedSize = size;
      return size;
    }
    
    private static final long serialVersionUID = 0L;
    @java.lang.Override
    protected java.lang.Object writeReplace()
        throws java.io.ObjectStreamException {
      return super.writeReplace();
    }
    
    @java.lang.Override
    public boolean equals(final java.lang.Object obj) {
      if (obj == this) {
       return true;
      }
      if (!(obj instanceof coprocessor.generated.ScanControlProtos.ScanControlResponse)) {
        return super.equals(obj);
      }
      coprocessor.generated.ScanControlProtos.ScanControlResponse other = (coprocessor.generated.ScanControlProtos.ScanControlResponse) obj;
      
      boolean result = true;
      result = result &&
          getUnknownFields().equals(other.getUnknownFields());
      return result;
    }
    
    @java.lang.Override
    public int hashCode() {
      int hash = 41;
      hash = (19 * hash) + getDescriptorForType().hashCode();
      hash = (29 * hash) + getUnknownFields().hashCode();
      return hash;
    }
    
    public static coprocessor.generated.ScanControlProtos.ScanControlResponse parseFrom(
        com.google.protobuf.ByteString data)
        throws com.google.protobuf.InvalidProtocolBufferException {
      return newBuilder().mergeFrom(data).buildParsed();
    }
    public static coprocessor.generated.ScanControlProtos.ScanControlResponse parseFrom(
        com.google.protobuf.ByteString data,
        com.google.protobuf.ExtensionRegistryLite extensionRegistry)
        throws com.google.protobuf.InvalidProtocolBufferException {
      return newBuilder().mergeFrom(data, extensionRegistry)
               .buildParsed();
    }
    public static coprocessor.generated.ScanControlProtos.ScanControlResponse parseFrom(byte[] data)
        throws com.google.protobuf.InvalidProtocolBufferException {
      return newBuilder().mergeFrom(data).buildParsed();
    }
    public static coprocessor.generated.ScanControlProtos.ScanControlResponse parseFrom(
        byte[] data,
        com.google.protobuf.ExtensionRegistryLite extensionRegistry)
        throws com.google.protobuf.InvalidProtocolBufferException {
      return newBuilder().mergeFrom(data, extensionRegistry)
               .buildParsed();
    }
    public static coprocessor.generated.ScanControlProtos.ScanControlResponse parseFrom(java.io.InputStream input)
        throws java.io.IOException {
      return newBuilder().mergeFrom(input).buildParsed();
    }
    public static coprocessor.generated.ScanControlProtos.ScanControlResponse parseFrom(
        java.io.InputStream input,
        com.google.protobuf.ExtensionRegistryLite extensionRegistry)
        throws java.io.IOException {
      return newBuilder().mergeFrom(input, extensionRegistry)
               .buildParsed();
    }
    public static coprocessor.generated.ScanControlProtos.ScanControlResponse parseDelimitedFrom(java.io.InputStream input)
        throws java.io.IOException {
      Builder builder = newBuilder();
      if (builder.mergeDelimitedFrom(input)) {
        return builder.buildParsed();
      } else {
        return null;
      }
    }
    public static coprocessor.generated.ScanControlProtos.ScanControlResponse parseDelimitedFrom(
        java.io.InputStream input,
        com.google.protobuf.ExtensionRegistryLite extensionRegistry)
        throws java.io.IOException {
      Builder builder = newBuilder();
      if (builder.mergeDelimitedFrom(input, extensionRegistry)) {
        return builder.buildParsed();
      } else {
        return null;
      }
    }
    public static coprocessor.generated.ScanControlProtos.ScanControlResponse parseFrom(
        com.google.protobuf.CodedInputStream input)
        throws java.io.IOException {
      return newBuilder().mergeFrom(input).buildParsed();
    }
    public static coprocessor.generated.ScanControlProtos.ScanControlResponse parseFrom(
        com.google.protobuf.CodedInputStream input,
        com.google.protobuf.ExtensionRegistryLite extensionRegistry)
        throws java.io.IOException {
      return newBuilder().mergeFrom(input, extensionRegistry)
               .buildParsed();
    }
    
    public static Builder newBuilder() { return Builder.create(); }
    public Builder newBuilderForType() { return newBuilder(); }
    public static Builder newBuilder(coprocessor.generated.ScanControlProtos.ScanControlResponse prototype) {
      return newBuilder().mergeFrom(prototype);
    }
    public Builder toBuilder() { return newBuilder(this); }
    
    @java.lang.Override
    protected Builder newBuilderForType(
        com.google.protobuf.GeneratedMessage.BuilderParent parent) {
      Builder builder = new Builder(parent);
      return builder;
    }
    public static final class Builder extends
        com.google.protobuf.GeneratedMessage.Builder<Builder>
       implements coprocessor.generated.ScanControlProtos.ScanControlResponseOrBuilder {
      public static final com.google.protobuf.Descriptors.Descriptor
          getDescriptor() {
        return coprocessor.generated.ScanControlProtos.internal_static_ScanControlResponse_descriptor;
      }
      
      protected com.google.protobuf.GeneratedMessage.FieldAccessorTable
          internalGetFieldAccessorTable() {
        return coprocessor.generated.ScanControlProtos.internal_static_ScanControlResponse_fieldAccessorTable;
      }
      
      // Construct using coprocessor.generated.ScanControlProtos.ScanControlResponse.newBuilder()
      private Builder() {
        maybeForceBuilderInitialization();
      }
      
      private Builder(BuilderParent parent) {
        super(parent);
        maybeForceBuilderInitialization();
      }
      private void maybeForceBuilderInitialization() {
        if (com.google.protobuf.GeneratedMessage.alwaysUseFieldBuilders) {
        }
      }
      private static Builder create() {
        return new Builder();
      }
      
      public Builder clear() {
        super.clear();
        return this;
      }
      
      public Builder clone() {
        return create().mergeFrom(buildPartial());
      }
      
      public com.google.protobuf.Descriptors.Descriptor
          getDescriptorForType() {
        return coprocessor.generated.ScanControlProtos.ScanControlResponse.getDescriptor();
      }
      
      public coprocessor.generated.ScanControlProtos.ScanControlResponse getDefaultInstanceForType() {
        return coprocessor.generated.ScanControlProtos.ScanControlResponse.getDefaultInstance();
      }
      
      public coprocessor.generated.ScanControlProtos.ScanControlResponse build() {
        coprocessor.generated.ScanControlProtos.ScanControlResponse result = buildPartial();
        if (!result.isInitialized()) {
          throw newUninitializedMessageException(result);
        }
        return result;
      }
      
      private coprocessor.generated.ScanControlProtos.ScanControlResponse buildParsed()
          throws com.google.protobuf.InvalidProtocolBufferException {
        coprocessor.generated.ScanControlProtos.ScanControlResponse result = buildPartial();
        if (!result.isInitialized()) {
          throw newUninitializedMessageException(
            result).asInvalidProtocolBufferException();
        }
        return result;
      }
      
      public coprocessor.generated.ScanControlProtos.ScanControlResponse buildPartial() {
        coprocessor.generated.ScanControlProtos.ScanControlResponse result = new coprocessor.generated.ScanControlProtos.ScanControlResponse(this);
        onBuilt();
        return result;
      }
      
      public Builder mergeFrom(com.google.protobuf.Message other) {
        if (other instanceof coprocessor.generated.ScanControlProtos.ScanControlResponse) {
          return mergeFrom((coprocessor.generated.ScanControlProtos.ScanControlResponse)other);
        } else {
          super.mergeFrom(other);
          return this;
        }
      }
      
      public Builder mergeFrom(coprocessor.generated.ScanControlProtos.ScanControlResponse other) {
        if (other == coprocessor.generated.ScanControlProtos.ScanControlResponse.getDefaultInstance()) {
          return this;
        }
        this.mergeUnknownFields(other.getUnknownFields());
        return this;
      }
      
      public final boolean isInitialized() {
        return true;
      }
      
      public Builder mergeFrom(
          com.google.protobuf.CodedInputStream input,
          com.google.protobuf.ExtensionRegistryLite extensionRegistry)
          throws java.io.IOException {
        com.google.protobuf.UnknownFieldSet.Builder unknownFields =
          com.google.protobuf.UnknownFieldSet.newBuilder(
            this.getUnknownFields());
        while (true) {
          int tag = input.readTag();
          switch (tag) {
            case 0:
              this.setUnknownFields(unknownFields.build());
              onChanged();
              return this;
            default: {
              if (!parseUnknownField(input, unknownFields,
                                     extensionRegistry, tag)) {
                this.setUnknownFields(unknownFields.build());
                onChanged();
                return this;
              }
              break;
            }
          }
        }
      }
      
      
      // @@protoc_insertion_point(builder_scope:ScanControlResponse)
    }
    
    static {
      defaultInstance = new ScanControlResponse(true);
      defaultInstance.initFields();
    }
    
    // @@protoc_insertion_point(class_scope:ScanControlResponse)
  }
  
  public static abstract class ScanControlService
      implements com.google.protobuf.Service {
    protected ScanControlService() {}
    
    public interface Interface {
      public abstract void resumeScan(
          com.google.protobuf.RpcController controller,
          coprocessor.generated.ScanControlProtos.ScanControlRequest request,
          com.google.protobuf.RpcCallback<coprocessor.generated.ScanControlProtos.ScanControlResponse> done);
      
    }
    
    public static com.google.protobuf.Service newReflectiveService(
        final Interface impl) {
      return new ScanControlService() {
        @java.lang.Override
        public  void resumeScan(
            com.google.protobuf.RpcController controller,
            coprocessor.generated.ScanControlProtos.ScanControlRequest request,
            com.google.protobuf.RpcCallback<coprocessor.generated.ScanControlProtos.ScanControlResponse> done) {
          impl.resumeScan(controller, request, done);
        }
        
      };
    }
    
    public static com.google.protobuf.BlockingService
        newReflectiveBlockingService(final BlockingInterface impl) {
      return new com.google.protobuf.BlockingService() {
        public final com.google.protobuf.Descriptors.ServiceDescriptor
            getDescriptorForType() {
          return getDescriptor();
        }
        
        public final com.google.protobuf.Message callBlockingMethod(
            com.google.protobuf.Descriptors.MethodDescriptor method,
            com.google.protobuf.RpcController controller,
            com.google.protobuf.Message request)
            throws com.google.protobuf.ServiceException {
          if (method.getService() != getDescriptor()) {
            throw new java.lang.IllegalArgumentException(
              "Service.callBlockingMethod() given method descriptor for " +
              "wrong service type.");
          }
          switch(method.getIndex()) {
            case 0:
              return impl.resumeScan(controller, (coprocessor.generated.ScanControlProtos.ScanControlRequest)request);
            default:
              throw new java.lang.AssertionError("Can't get here.");
          }
        }
        
        public final com.google.protobuf.Message
            getRequestPrototype(
            com.google.protobuf.Descriptors.MethodDescriptor method) {
          if (method.getService() != getDescriptor()) {
            throw new java.lang.IllegalArgumentException(
              "Service.getRequestPrototype() given method " +
              "descriptor for wrong service type.");
          }
          switch(method.getIndex()) {
            case 0:
              return coprocessor.generated.ScanControlProtos.ScanControlRequest.getDefaultInstance();
            default:
              throw new java.lang.AssertionError("Can't get here.");
          }
        }
        
        public final com.google.protobuf.Message
            getResponsePrototype(
            com.google.protobuf.Descriptors.MethodDescriptor method) {
          if (method.getService() != getDescriptor()) {
            throw new java.lang.IllegalArgumentException(
              "Service.getResponsePrototype() given method " +
              "descriptor for wrong service type.");
          }
          switch(method.getIndex()) {
            case 0:
              return coprocessor.generated.ScanControlProtos.ScanControlResponse.getDefaultInstance();
            default:
              throw new java.lang.AssertionError("Can't get here.");
          }
        }
        
      };
    }
    
    public abstract void resumeScan(
        com.google.protobuf.RpcController controller,
        coprocessor.generated.ScanControlProtos.ScanControlRequest request,
        com.google.protobuf.RpcCallback<coprocessor.generated.ScanControlProtos.ScanControlResponse> done);
    
    public static final
        com.google.protobuf.Descriptors.ServiceDescriptor
        getDescriptor() {
      return coprocessor.generated.ScanControlProtos.getDescriptor().getServices().get(0);
    }
    public final com.google.protobuf.Descriptors.ServiceDescriptor
        getDescriptorForType() {
      return getDescriptor();
    }
    
    public final void callMethod(
        com.google.protobuf.Descriptors.MethodDescriptor method,
        com.google.protobuf.RpcController controller,
        com.google.protobuf.Message request,
        com.google.protobuf.RpcCallback<
          com.google.protobuf.Message> done) {
      if (method.getService() != getDescriptor()) {
        throw new java.lang.IllegalArgumentException(
          "Service.callMethod() given method descriptor for wrong " +
          "service type.");
      }
      switch(method.getIndex()) {
        case 0:
          this.resumeScan(controller, (coprocessor.generated.ScanControlProtos.ScanControlRequest)request,
            com.google.protobuf.RpcUtil.<coprocessor.generated.ScanControlProtos.ScanControlResponse>specializeCallback(
              done));
          return;
        default:
          throw new java.lang.AssertionError("Can't get here.");
      }
    }
    
    public final com.google.protobuf.Message
        getRequestPrototype(
        com.google.protobuf.Descriptors.MethodDescriptor method) {
      if (method.getService() != getDescriptor()) {
        throw new java.lang.IllegalArgumentException(
          "Service.getRequestPrototype() given method " +
          "descriptor for wrong service type.");
      }
      switch(method.getIndex()) {
        case 0:
          return coprocessor.generated.ScanControlProtos.ScanControlRequest.getDefaultInstance();
        default:
          throw new java.lang.AssertionError("Can't get here.");
      }
    }
    
    public final com.google.protobuf.Message
        getResponsePrototype(
        com.google.protobuf.Descriptors.MethodDescriptor method) {
      if (method.getService() != getDescriptor()) {
        throw new java.lang.IllegalArgumentException(
          "Service.getResponsePrototype() given method " +
          "descriptor for wrong service type.");
      }
      switch(method.getIndex()) {
        case 0:
          return coprocessor.generated.ScanControlProtos.ScanControlResponse.getDefaultInstance();
        default:
          throw new java.lang.AssertionError("Can't get here.");
      }
    }
    
    public static Stub newStub(
        com.google.protobuf.RpcChannel channel) {
      return new Stub(channel);
    }
    
    public static final class Stub extends coprocessor.generated.ScanControlProtos.ScanControlService implements Interface {
      private Stub(com.google.protobuf.RpcChannel channel) {
        this.channel = channel;
      }
      
      private final com.google.protobuf.RpcChannel channel;
      
      public com.google.protobuf.RpcChannel getChannel() {
        return channel;
      }
      
      public  void resumeScan(
          com.google.protobuf.RpcController controller,
          coprocessor.generated.ScanControlProtos.ScanControlRequest request,
          com.google.protobuf.RpcCallback<coprocessor.generated.ScanControlProtos.ScanControlResponse> done) {
        channel.callMethod(
          getDescriptor().getMethods().get(0),
          controller,
          request,
          coprocessor.generated.ScanControlProtos.ScanControlResponse.getDefaultInstance(),
          com.google.protobuf.RpcUtil.generalizeCallback(
            done,
            coprocessor.generated.ScanControlProtos.ScanControlResponse.class,
            coprocessor.generated.ScanControlProtos.ScanControlResponse.getDefaultInstance()));
      }
    }
    
    public static BlockingInterface newBlockingStub(
        com.google.protobuf.BlockingRpcChannel channel) {
      return new BlockingStub(channel);
    }
    
    public interface BlockingInterface {
      public coprocessor.generated.ScanControlProtos.ScanControlResponse resumeScan(
          com.google.protobuf.RpcController controller,
          coprocessor.generated.ScanControlProtos.ScanControlRequest request)
          throws com.google.protobuf.ServiceException;
    }
    
    private static final class BlockingStub implements BlockingInterface {
      private BlockingStub(com.google.protobuf.BlockingRpcChannel channel) {
        this.channel = channel;
      }
      
      private final com.google.protobuf.BlockingRpcChannel channel;
      
      public coprocessor.generated.ScanControlProtos.ScanControlResponse resumeScan(
          com.google.protobuf.RpcController controller,
          coprocessor.generated.ScanControlProtos.ScanControlRequest request)
          throws com.google.protobuf.ServiceException {
        return (coprocessor.generated.ScanControlProtos.ScanControlResponse) channel.callBlockingMethod(
          getDescriptor().getMethods().get(0),
          controller,
          request,
          coprocessor.generated.ScanControlProtos.ScanControlResponse.getDefaultInstance());
      }
      
    }
  }
  
  private static com.google.protobuf.Descriptors.Descriptor
    internal_static_ScanControlRequest_descriptor;
  private static
    com.google.protobuf.GeneratedMessage.FieldAccessorTable
      internal_static_ScanControlRequest_fieldAccessorTable;
  private static com.google.protobuf.Descriptors.Descriptor
    internal_static_ScanControlResponse_descriptor;
  private static
    com.google.protobuf.GeneratedMessage.FieldAccessorTable
      internal_static_ScanControlResponse_fieldAccessorTable;
  
  public static com.google.protobuf.Descriptors.FileDescriptor
      getDescriptor() {
    return descriptor;
  }
  private static com.google.protobuf.Descriptors.FileDescriptor
      descriptor;
  static {
    java.lang.String[] descriptorData = {
      "\n\030ScanControlService.proto\"\024\n\022ScanContro" +
      "lRequest\"\025\n\023ScanControlResponse2M\n\022ScanC" +
      "ontrolService\0227\n\nresumeScan\022\023.ScanContro" +
      "lRequest\032\024.ScanControlResponseB2\n\025coproc" +
      "essor.generatedB\021ScanControlProtosH\001\210\001\001\240" +
      "\001\001"
    };
    com.google.protobuf.Descriptors.FileDescriptor.InternalDescriptorAssigner assigner =
      new com.google.protobuf.Descriptors.FileDescriptor.InternalDescriptorAssigner() {
        public com.google.protobuf.ExtensionRegistry assignDescriptors(
            com.google.protobuf.Descriptors.FileDescriptor root) {
          descriptor = root;
          internal_static_ScanControlRequest_descriptor =
            getDescriptor().getMessageTypes().get(0);
          internal_static_ScanControlRequest_fieldAccessorTable = new
            com.google.protobuf.GeneratedMessage.FieldAccessorTable(
              internal_static_ScanControlRequest_descriptor,
              new java.lang.String[] { },
              coprocessor.generated.ScanControlProtos.ScanControlRequest.class,
              coprocessor.generated.ScanControlProtos.ScanControlRequest.Builder.class);
          internal_static_ScanControlResponse_descriptor =
            getDescriptor().getMessageTypes().get(1);
          internal_static_ScanControlResponse_fieldAccessorTable = new
            com.google.protobuf.GeneratedMessage.FieldAccessorTable(
              internal_static_ScanControlResponse_descriptor,
              new java.lang.String[] { },
              coprocessor.generated.ScanControlProtos.ScanControlResponse.class,
              coprocessor.generated.ScanControlProtos.ScanControlResponse.Builder.class);
          return null;
        }
      };
    com.google.protobuf.Descriptors.FileDescriptor
      .internalBuildGeneratedFileFrom(descriptorData,
        new com.google.protobuf.Descriptors.FileDescriptor[] {
        }, assigner);
  }
  
  // @@protoc_insertion_point(outer_class_scope)
}
