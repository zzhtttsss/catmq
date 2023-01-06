package org.catmq;

import io.grpc.Grpc;
import io.grpc.InsecureChannelCredentials;
import io.grpc.ManagedChannel;
import lombok.extern.slf4j.Slf4j;
import org.apache.commons.cli.*;
import org.apache.curator.framework.CuratorFramework;
import org.catmq.constant.Command;
import org.catmq.constant.FileConstant;
import org.catmq.constant.SubCommand;
import org.catmq.constant.ZkConstant;
import org.catmq.producer.Producer;
import org.catmq.producer.ProducerConfig;
import org.catmq.util.StringUtil;
import org.catmq.zk.ZkUtil;

import java.net.InetSocketAddress;
import java.util.*;

@Slf4j
public class ProducerStartup {
    static final Map<String, Options> COMMAND_MAP = new HashMap<>();

    public static void main(String[] args) throws Exception {
        initCommand();
        Producer producer = initProducer();

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

    }

    private static Producer initProducer() throws InterruptedException {
        ProducerConfig config = ProducerConfig.ProducerConfigEnum.INSTANCE.getInstance();
        config.readConfig("/producer.properties");
        InetSocketAddress brokerAddress = StringUtil.parseAddress(
                getBrokerAddress(config.getZkAddress())
                        .orElseThrow(() -> new RuntimeException("no broker address")));
        // Access a service running on the local machine on port 5432
        int port = brokerAddress.getPort();
        String target = brokerAddress.getHostName() + ":" + port;

        // Create a communication channel to the server, known as a Channel. Channels are thread-safe
        // and reusable. It is common to create channels at the beginning of your application and reuse
        // them until the application shuts down.
        //
        // For the example we use plaintext insecure credentials to avoid needing TLS certificates. To
        // use TLS, use TlsChannelCredentials instead.
        ManagedChannel channel = Grpc.newChannelBuilder(target, InsecureChannelCredentials.create()).build();

        // ManagedChannels use resources like threads and TCP connections. To prevent leaking these
        // resources the channel should be shut down when it will no longer be used. If it may be used
        // again leave it running.
        // channel.shutdownNow().awaitTermination(5, TimeUnit.SECONDS);
        return new Producer(channel);
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
                case GET -> {
                    CommandLine getCmd = parser.parse(options, Arrays.copyOfRange(args, 1, args.length));
                    String topic1 = getCmd.getOptionValue("t");
                    if (StringUtil.isEmpty(topic1)) {
                        printHelp();
                        return;
                    }
                    log.info("topic: {}", topic1);
                }
                case TOPIC -> {
                    CommandLine topicCmd = parser.parse(options, Arrays.copyOfRange(args, 1, args.length));
                    String topic2 = topicCmd.getOptionValue("t");
                    String op = topicCmd.getOptionValue("o");
                    if (StringUtil.isEmpty(topic2) || StringUtil.isEmpty(op)) {
                        printHelp();
                        return;
                    }
                    log.info("topic: {}, op: {}", topic2, op);
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

    /**
     * Get optimal broker address from zookeeper using the least used algorithm
     *
     * @param zkAddress zookeeper address
     * @return broker address path like /address/broker/127.0.0.1:5432
     */
    private static Optional<String> getBrokerAddress(String zkAddress) {
        try (CuratorFramework client = ZkUtil.createClient(zkAddress)) {
            Map<String, Integer> map = new HashMap<>(4);

            List<String> paths = client.getChildren().forPath(ZkConstant.BROKER_ADDRESS);
            String addressDirectory = StringUtil.concatString(ZkConstant.BROKER_ADDRESS, FileConstant.LEFT_SLASH);
            for (String path : paths) {
                byte[] bytes = client.getData().forPath(StringUtil.concatString(addressDirectory, path));
                map.put(path, Integer.parseInt(new String(bytes)));
            }

            return map.entrySet().stream()
                    .min(Map.Entry.comparingByValue())
                    .map(stringIntegerEntry -> addressDirectory + stringIntegerEntry.getKey())
                    .orElseGet(() -> addressDirectory + paths.get(0))
                    .describeConstable();
        } catch (Exception e) {
            e.printStackTrace();
            return Optional.empty();
        }
    }
}
