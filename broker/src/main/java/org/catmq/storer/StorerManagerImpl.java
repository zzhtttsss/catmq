package org.catmq.storer;

public class StorerManagerImpl {

//    public void sendMessage2Storer(MessageEntry messageEntry, TopicDetail topicDetail) {
//        ManagedChannel channel = GRPC_CONNECT_CACHE.get(topicDetail.getBrokerAddress());
//        Metadata metadata = new Metadata();
//        metadata.put(Metadata.Key.of("action", Metadata.ASCII_STRING_MARSHALLER), "sendMessage");
//        Channel headChannel = ClientInterceptors.intercept(channel, MetadataUtils.newAttachHeadersInterceptor(metadata));
//        BrokerServiceGrpc.BrokerServiceBlockingStub blockingStub = BrokerServiceGrpc.newBlockingStub(headChannel);
//
//        Message message = Message.newBuilder()
//                .setTopic(topicDetail.getCompleteTopicName())
//                .setBody(ByteString.copyFrom(messageEntry.getBody()))
//                .setType(MessageType.NORMAL)
//                .build();
//        SendMessage2BrokerRequest request = SendMessage2BrokerRequest.newBuilder()
//                .setMessage(message)
//                .setProducerId(this.producerId)
//                .build();
//        SendMessage2BrokerResponse response;
//        try {
//            response = blockingStub.sendMessage2Broker(request);
//        } catch (StatusRuntimeException e) {
//            log.warn("RPC failed: {}", e.getMessage());
//            return;
//        }
//        log.info("ack: {} \nresponse: {} \nstatus msg: {} \nstatus code: {}",
//                response.getAck(), response.getRes(), response.getStatus().getMessage(),
//                response.getStatus().getCode().getNumber());
//    }
//
//    public void sendMessage2Storer(Iterable<MessageEntry> messages) {
//        for (MessageEntry messageEntry : messages) {
//            sendMessage2Storer(messageEntry);
//        }
//    }
}
