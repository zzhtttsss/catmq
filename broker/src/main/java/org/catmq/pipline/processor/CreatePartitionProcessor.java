package org.catmq.pipline.processor;

import org.catmq.entity.TopicDetail;
import org.catmq.grpc.RequestContext;
import org.catmq.pipline.Processor;
import org.catmq.protocol.service.*;
import org.catmq.zk.ZkIdGenerator;

import static org.catmq.broker.Broker.BROKER;

public class CreatePartitionProcessor implements Processor<CreatePartitionRequest, CreatePartitionResponse> {
    @Override
    public CreatePartitionResponse process(RequestContext ctx, CreatePartitionRequest request) {
        TopicDetail.get(request.getTopic());
        BROKER.getTopicManager().createPartition(request.getTopic(), ctx.getBrokerPath());
        long segmentId = ZkIdGenerator.ZkIdGeneratorEnum.INSTANCE.getInstance().nextId(BROKER.getClient());
//        BROKER.getStorerManager().createSegment(segmentId);
        return CreatePartitionResponse
                .newBuilder()
                .setAck(true)
                .setRes("create success")
                .build();
    }

    public enum CreatePartitionProcessorEnum {
        INSTANCE;
        private final CreatePartitionProcessor instance;

        CreatePartitionProcessorEnum() {
            instance = new CreatePartitionProcessor();
        }

        public CreatePartitionProcessor getInstance() {
            return instance;
        }
    }
}
