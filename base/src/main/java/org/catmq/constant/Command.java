package org.catmq.constant;

/**
 * Enum Command contains all operations specified by users
 *
 * @author BYL
 */
public enum Command {
    // Put Command represents user will put msg into broker
    PUT,
    // Get Command represents user will get msg from broker
    GET,
    // Topic Command represents user will operate topic like creating,listing and so on
    TOPIC;

    public static Command valueOfIgnoreCase(String name) {
        return Command.valueOf(name.toUpperCase());
    }
}
