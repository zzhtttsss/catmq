package org.catmq.pipline.preparer;

import lombok.extern.slf4j.Slf4j;
import org.catmq.broker.common.Consumer;
import org.catmq.broker.service.ClientManageService;
import org.catmq.grpc.RequestContext;
import org.catmq.pipline.Preparer;

@Slf4j
public class RegisterClientPreparer implements Preparer {
    public static final String REGISTER_CLIENT_PREPARER = "RegisterClientPreparer";

    private final ClientManageService clientService;

    @Override
    public void prepare(RequestContext ctx) {
        if (ctx.getConsumerId() != null && !clientService.isConsumerExist(ctx.getConsumerId())) {
            clientService.addConsumer(new Consumer(ctx.getConsumerId()));
        }
    }

    private RegisterClientPreparer() {
        clientService = ClientManageService.ClientManageServiceEnum.INSTANCE.getInstance();
    }

    public enum RegisterClientPreparerEnum {
        INSTANCE;
        private final RegisterClientPreparer registerClientPreparer;

        RegisterClientPreparerEnum() {
            registerClientPreparer = new RegisterClientPreparer();
        }

        public RegisterClientPreparer getInstance() {
            return registerClientPreparer;
        }
    }
}
