package  countlatancy.task;

import java.util.HashMap;
import java.util.Map;
import java.util.Map.Entry;

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

public class CountLatancy implements StreamTask,WindowableTask,InitableTask{
    Map<String,Integer> clickEvent = new HashMap<String,Integer>();
    Map<String,Integer> clickCount = new HashMap<String,Integer>();
    int total = 0;
    private static String TOPIC_NAME = "pageviews";

    private static SystemStream stream = new SystemStream("kafka",TOPIC_NAME);

    public void window(MessageCollector collector, TaskCoordinator coodinator)
            throws Exception {

        //int total = 0;
        for(Entry<String,Integer> entry:clickEvent.entrySet()){
            String name = entry.getKey();
            int latancy = entry.getValue();
            int count = clickCount.get(name);
            String json = "{\"name\" : \""+name+"\""+
                    ",\"total_latancyMs\" : "+latancy+
                    ",\"clickNum\" : "+count+
                    ",\"aver_lantancyMs\" : "+(latancy+0.0)/count+
                    "}";
            total+=count;
            collector.send(new OutgoingMessageEnvelope (stream,json));
        }
        collector.send(new OutgoingMessageEnvelope (stream,"Total messages per window : "+total+" ."));
        clickEvent = new HashMap<String,Integer>();
        clickCount = new HashMap<String,Integer>();
    }

    public void process(IncomingMessageEnvelope envelope, MessageCollector collector,
            TaskCoordinator coodinator) throws Exception {
        JSONObject json = new JSONObject(envelope.getMessage().toString());
        String name = json.getString("name");
        int latancy = json.getInt("latancyMs");
        if(clickEvent.containsKey(json.get("name").toString())){
            clickEvent.put(name, clickEvent.get(name)+latancy);
            clickCount.put(name, clickCount.get(name)+1);
        }else{
            clickEvent.put(name,latancy);
            clickCount.put(name,1);
        }
    }

    public void init(Config config, TaskContext task){
        System.out.println("----------------------------------------------------------------");
        System.out.println("hhhhhhhhhhhhhhhhhhhhhhhhhhhhh");
        System.out.println("----------------------------------------------------------------");

    }
}
