// Generated by the protocol buffer compiler.  DO NOT EDIT!
// source: Produce.proto

package org.catmq.protocol.produce;

public interface SendMessage2BrokerRequestOrBuilder extends
    // @@protoc_insertion_point(interface_extends:SendMessage2BrokerRequest)
    com.google.protobuf.MessageOrBuilder {

  /**
   * <code>string topic = 1;</code>
   * @return The topic.
   */
  java.lang.String getTopic();
  /**
   * <code>string topic = 1;</code>
   * @return The bytes for topic.
   */
  com.google.protobuf.ByteString
      getTopicBytes();

  /**
   * <code>string message = 2;</code>
   * @return The message.
   */
  java.lang.String getMessage();
  /**
   * <code>string message = 2;</code>
   * @return The bytes for message.
   */
  com.google.protobuf.ByteString
      getMessageBytes();
}
