package org.catmq.executer;

import com.alibaba.fastjson2.JSON;
import org.catmq.context.Context;
import org.catmq.finisher.ExampleFinisher;
import org.catmq.finisher.Finisher;
import org.catmq.preparer.AuthPreparer;
import org.catmq.preparer.CompressPreparer;
import org.catmq.preparer.Preparer;
import org.catmq.processor.ConsumeProcessor;
import org.catmq.processor.Processor;
import org.catmq.processor.ProduceProcessor;

import java.util.Map;

import static org.catmq.finisher.ExampleFinisher.EXAMPLE_FINISHER;
import static org.catmq.preparer.AuthPreparer.AUTH_PREPARER;
import static org.catmq.preparer.CompressPreparer.COMPRESS_PREPARER;
import static org.catmq.processor.ConsumeProcessor.CONSUME_PROCESSOR;
import static org.catmq.processor.ProduceProcessor.PRODUCE_PROCESSOR_NAME;

public class Executer {
    private String[] preparers;

    private String processor;

    private String[] finishers;

    public static Map<String, Preparer> preparerMap;
    public static Map<String, Processor> processorMap;
    public static Map<String, Finisher> finisherMap;

    static {
        preparerMap = Map.of(AUTH_PREPARER, new AuthPreparer(),
                COMPRESS_PREPARER, new CompressPreparer());
        processorMap = Map.of(PRODUCE_PROCESSOR_NAME, new ProduceProcessor(),
                CONSUME_PROCESSOR, new ConsumeProcessor());
        finisherMap = Map.of(EXAMPLE_FINISHER, new ExampleFinisher());
    }

    private Executer(String[] preparers, String processor, String[] finishers) {
        this.preparers = preparers;
        this.processor = processor;
        this.finishers = finishers;
    }

    public static Executer getExecuterByContext(Context ctx) {
        String config = "{\"preparers\":[\"AuthPreparer\",\"CompressPreparer\"],\"processor\":" +
                "\"ProduceProcessor\",\"finishers\":[\"ExampleFinisher\"]}";
        return JSON.parseObject(config, Executer.class);
    }

    public Context execute(Context context){
        for (String p: preparers) {
            context = preparerMap.get(p).prepare(context);
        }
        context = processorMap.get(processor).process(context);
        for (String f: finishers) {
            context = finisherMap.get(f).finish(context);
        }
        return context;
    }
}
