package org.catmq.client;

import io.grpc.Grpc;
import io.grpc.InsecureChannelCredentials;
import io.grpc.ManagedChannel;
import lombok.extern.slf4j.Slf4j;
import org.apache.commons.cli.*;
import org.catmq.client.producer.Producer;
import org.catmq.client.producer.ProducerConfig;
import org.catmq.client.producer.ProducerProxy;
import org.catmq.constant.Command;
import org.catmq.constant.SubCommand;
import org.catmq.util.StringUtil;

import java.net.InetSocketAddress;
import java.util.Arrays;
import java.util.HashMap;
import java.util.Map;
import java.util.Scanner;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicLong;

@Slf4j
public class ProducerStartup {
    static final Map<String, Options> COMMAND_MAP = new HashMap<>();
    // only for test
    static final AtomicLong PRODUCER_ID = new AtomicLong(0);

    public static void main(String[] args) throws Exception {
        initCommand();
        ProducerProxy pp = new ProducerProxy(ProducerProxy.LoadBalanceEnum.LEAST_USED);
        Producer producer = initProducer(pp);

        System.out.print(">");
        Scanner sc = new Scanner(System.in);
        while (sc.hasNext()) {
            String line = sc.nextLine();
            if ("exit".equals(line)) {
                break;
            }
            executeCommand(line.split(" "), producer);
            System.out.print(">");
        }
        producer.close();

    }

    private static Producer initProducer(ProducerProxy pp) throws InterruptedException {
        ProducerConfig config = ProducerConfig.ProducerConfigEnum.INSTANCE.getInstance();
        InetSocketAddress brokerAddress = StringUtil.parseAddress(
                pp.selectBrokers(config.getZkAddress())
                        .orElseThrow(() -> new RuntimeException("no broker address")));
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
        Producer producer = new Producer(channel, PRODUCER_ID.incrementAndGet());
        Runtime.getRuntime().addShutdownHook(new Thread() {
            @Override
            public void run() {
                log.warn("*** shutting down gRPC client");
                producer.close();
                log.warn("*** producer shut down");
            }
        });

        // ManagedChannels use resources like threads and TCP connections. To prevent leaking these
        // resources the channel should be shut down when it will no longer be used. If it may be used
        // again leave it running.
        // channel.shutdownNow().awaitTermination(5, TimeUnit.SECONDS);
        return producer;
    }

    public static void executeCommand(String[] args, Producer producer) {
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
                case PUT -> {
                    CommandLine putCmd = parser.parse(options, Arrays.copyOfRange(args, 1, args.length));
                    String topic = putCmd.getOptionValue("t");
                    String msg = putCmd.getOptionValue("m");
                    if (StringUtil.isEmpty(topic) || StringUtil.isEmpty(msg)) {
                        printHelp();
                        return;
                    }
                    producer.sendMessage2Broker(topic, msg);
                }
                case TOPIC -> {
                    CommandLine topicCmd = parser.parse(options, Arrays.copyOfRange(args, 1, args.length));
                    if (topicCmd.hasOption("l")) {
                        log.info("list topic");
                    } else if (topicCmd.hasOption("c")) {
                        String topicName = topicCmd.getOptionValue("c");
                        if (StringUtil.isEmpty(topicName)) {
                            printHelp();
                            return;
                        }
                        producer.createTopic(topicName);
                    }
                }
                default -> printHelp();
            }
        } catch (ParseException e) {
            throw new RuntimeException(e);
        }

    }

    public static void initCommand() {
        // Define the "put" command and its options
        Options putOptions = new Options();
        Option topicOption = Option.builder(SubCommand.TOPIC.getOpt())
                .longOpt(SubCommand.TOPIC.getLongOpt())
                .required(true)
                .hasArg(true)
                .argName(SubCommand.TOPIC.getLongOpt())
                .desc("Specified the topic which the message will be put into")
                .build();
        Option messageOption = Option.builder(SubCommand.MESSAGE.getOpt())
                .longOpt(SubCommand.MESSAGE.getLongOpt())
                .required(true)
                .hasArg(true)
                .argName(SubCommand.MESSAGE.getLongOpt())
                .desc("Specified the message to the default topic")
                .build();
        putOptions.addOption(topicOption);
        putOptions.addOption(messageOption);
        COMMAND_MAP.put("put", putOptions);

        // Define the "get" command and its options
        Options getOptions = new Options();
        getOptions.addOption(topicOption);
        getOptions.addOption(messageOption);
        COMMAND_MAP.put("get", getOptions);

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

    public static void printHelp() {
        HelpFormatter formatter = new HelpFormatter();
        COMMAND_MAP.forEach(formatter::printHelp);
        System.exit(1);
    }
}
