package org.catmq.client;


import io.grpc.Grpc;
import io.grpc.InsecureChannelCredentials;
import io.grpc.ManagedChannel;
import lombok.extern.slf4j.Slf4j;
import org.apache.commons.cli.*;
import org.catmq.client.consumer.Consumer;
import org.catmq.client.consumer.ConsumerConfig;
import org.catmq.constant.Command;
import org.catmq.constant.SubCommand;
import org.catmq.util.StringUtil;

import java.net.InetSocketAddress;
import java.util.Arrays;
import java.util.HashMap;
import java.util.Map;
import java.util.Scanner;
import java.util.concurrent.TimeUnit;

import static org.catmq.client.ProducerStartup.PRODUCER_ID;
import static org.catmq.client.ProducerStartup.printHelp;

@Slf4j
public class ConsumerStartup {

    static final Map<String, Options> COMMAND_MAP = new HashMap<>();


    public static void main(String[] args) {
        initCommand();
        Consumer consumer = initConsumer();

        System.out.print(">");
        Scanner sc = new Scanner(System.in);
        while (sc.hasNext()) {
            String line = sc.nextLine();
            if ("exit".equals(line)) {
                break;
            }
            executeCommand(line.split(" "), consumer);
            System.out.print(">");
        }
    }

    private static void executeCommand(String[] args, Consumer consumer) {
        if (args.length == 0) {
            printHelp();
            return;
        }
        String command = args[0];
        if (!COMMAND_MAP.containsKey(command)) {
            printHelp();
            return;
        }
        CommandLineParser parser = new DefaultParser();
        Options options = COMMAND_MAP.get(command);
        try {
            switch (Command.valueOfIgnoreCase(command)) {
                case GET -> {
                    CommandLine putCmd = parser.parse(options, Arrays.copyOfRange(args, 1, args.length));
                    String topic = putCmd.getOptionValue("t");
                    if (StringUtil.isEmpty(topic)) {
                        printHelp();
                        return;
                    }
                    consumer.getMessageFromBroker(topic);
                }
                default -> printHelp();
            }
        } catch (ParseException e) {
            throw new RuntimeException(e);
        }
    }

    private static Consumer initConsumer() {
        ConsumerConfig config = ConsumerConfig.ConsumerConfigEnum.INSTANCE.getInstance();
        InetSocketAddress brokerAddress = new InetSocketAddress("127.0.0.1", 5432);
        config.setBrokerAddress(brokerAddress);
        // Access a service running on the local machine on port 5432
        int port = brokerAddress.getPort();
        String target = brokerAddress.getHostName() + ":" + port;

        // Create a communication channel to the server, known as a Channel. Channels are thread-safe
        // and reusable. It is common to create channels at the beginning of your application and reuse
        // them until the application shuts down.
        //
        // For the example we use plaintext insecure credentials to avoid needing TLS certificates. To
        // use TLS, use TlsChannelCredentials instead.
        ManagedChannel channel = Grpc.newChannelBuilder(target, InsecureChannelCredentials.create())
                .keepAliveTime(30, TimeUnit.SECONDS)
                .keepAliveTimeout(10, TimeUnit.SECONDS)
                .build();
        Consumer consumer = new Consumer(channel, PRODUCER_ID.incrementAndGet());
        Runtime.getRuntime().addShutdownHook(new Thread() {
            @Override
            public void run() {
                log.warn("*** shutting down gRPC client");
                consumer.close();
                log.warn("*** producer shut down");
            }
        });

        // ManagedChannels use resources like threads and TCP connections. To prevent leaking these
        // resources the channel should be shut down when it will no longer be used. If it may be used
        // again leave it running.
        // channel.shutdownNow().awaitTermination(5, TimeUnit.SECONDS);
        return consumer;
    }

    private static void initCommand() {
        // Define the "get" command and its options
        Options getOption = new Options();
        Option topicOption = Option.builder(SubCommand.TOPIC.getOpt())
                .longOpt(SubCommand.TOPIC.getLongOpt())
                .required(true)
                .hasArg(true)
                .argName(SubCommand.TOPIC.getLongOpt())
                .desc("Specified the topic which the message will be put into")
                .build();
        getOption.addOption(topicOption);
        COMMAND_MAP.put("get", getOption);

        // Define the "topic" command and its options
        Options topicOptions = new Options();
        Option createOption = Option.builder(SubCommand.CREATE.getOpt())
                .longOpt(SubCommand.CREATE.getLongOpt())
                .required(true)
                .hasArg(true)
                .argName(SubCommand.CREATE.getLongOpt())
                .desc("Create a new topic")
                .build();
        Option listOption = Option.builder(SubCommand.LIST.getOpt())
                .longOpt(SubCommand.LIST.getLongOpt())
                .desc("List all available topics")
                .build();
        topicOptions.addOption(createOption);
        topicOptions.addOption(listOption);
        COMMAND_MAP.put("topic", topicOptions);
    }
}
