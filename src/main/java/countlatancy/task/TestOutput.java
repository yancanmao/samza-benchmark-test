package  countlatancy.task;

import java.util.HashMap;
import java.util.Map;
import java.util.Map.Entry;
import org.apache.samza.metrics.Counter;
import org.apache.samza.config.Config;
import org.apache.samza.system.IncomingMessageEnvelope;
import org.apache.samza.system.OutgoingMessageEnvelope;
import org.apache.samza.system.SystemStream;
import org.apache.samza.task.InitableTask;
import org.apache.samza.task.MessageCollector;
import org.apache.samza.task.StreamTask;
import org.apache.samza.task.TaskContext;
import org.apache.samza.task.TaskCoordinator;
import org.apache.samza.task.WindowableTask;
import org.json.JSONObject;

public class TestOutput implements StreamTask,InitableTask{
    Map<String,Integer> clickEvent = new HashMap<String,Integer>();
    Map<String,Integer> clickCount = new HashMap<String,Integer>();
    int total = 0;
    private static String TOPIC_NAME = "pageviews";

    private static SystemStream stream = new SystemStream("kafka",TOPIC_NAME);

    private Counter inputCol;
    public void process(IncomingMessageEnvelope envelope, MessageCollector collector,
            TaskCoordinator coodinator) throws Exception {
        Object msg = envelope.getMessage();
        //System.out.println(msg);
        inputCol.inc();
        collector.send(new OutgoingMessageEnvelope(stream, msg));
    }

    public void init(Config config, TaskContext task){
        this.inputCol = task.getMetricsRegistry().newCounter("input-counters", "input-col");
        System.out.println("----------------------------------------------------------------");
        System.out.println("hhhhhhhhhhhhhhhhhhhhhhhhhhhhh");
        System.out.println("----------------------------------------------------------------");

    }
}
