package org.catmq.constant;

import lombok.Getter;

/**
 * @author BYL
 */
public enum OperationCode {
    // send message to broker whose body is msg
    SEND_MESSAGE(101);
    @Getter
    private final int code;

    OperationCode(int code) {
        this.code = code;
    }
}
