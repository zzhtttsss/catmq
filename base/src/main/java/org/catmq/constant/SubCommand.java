package org.catmq.constant;

import lombok.Getter;

/**
 * @author BYL
 */

public enum SubCommand {
    /**
     * Topic subcommand is used to specify the topic name where command will be sent to.
     * <strong>It does have arguments.</strong>
     */
    TOPIC("t", "topic"),

    /**
     * Message subcommand is used to specify the message content which usually follows the PUT, GET, DELETE command.
     * <strong>It does have arguments.</strong>
     */
    MESSAGE("m", "message"),

    /**
     * Create subcommand is used to create a topic.
     * <strong>It does not have arguments.</strong>
     */
    CREATE("c", "create"),

    /**
     * List subcommand is used to list all topics.
     * <strong>It does not have arguments.</strong>
     */
    LIST("l", "list");

    @Getter
    private final String opt;
    @Getter
    private final String longOpt;

    public static SubCommand valueOfIgnoreCase(String name) {
        return SubCommand.valueOf(name.toUpperCase());
    }

    SubCommand(String opt, String longOpt) {
        this.opt = opt;
        this.longOpt = longOpt;
    }
}
