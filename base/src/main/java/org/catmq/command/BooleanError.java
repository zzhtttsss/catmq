package org.catmq.command;

import lombok.Data;

/**
 * BooleanErrorWrapper is used to wrap the result of a boolean operation.
 * If the operation is successful, the result is true, and the error is null.
 * If the operation fails, the result is false, and the error is the error message.
 *
 * @author BYL
 */
@Data
public class BooleanError {
    private boolean success;
    private String error;

    private BooleanError(boolean success, String error) {
        this.success = success;
        this.error = error;
    }

    public static BooleanError ok() {
        return new BooleanError(true, null);
    }

    public static BooleanError fail(String error) {
        return new BooleanError(false, error);
    }
}
