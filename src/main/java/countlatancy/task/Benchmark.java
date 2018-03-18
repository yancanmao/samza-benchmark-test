package  countlatancy.task;

import java.util.HashMap;
import java.util.Map;
import java.util.Map.Entry;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

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

public class Benchmark implements StreamTask,InitableTask{
    // Map<String,Integer> clickEvent = new HashMap<String,Integer>();
    // Map<String,Integer> clickCount = new HashMap<String,Integer>();
    //private Map<String, Integer> counts = new HashMap<String, Integer>();
    private static String TOPIC_NAME = "benchmark";

    private static SystemStream output_stream = new SystemStream("kafka",TOPIC_NAME);

    public void process(IncomingMessageEnvelope envelope, MessageCollector collector,TaskCoordinator coodinator) throws Exception {
        // System.out.println(envelope.getMessage());
        //Map<String, Object> Object = (Map<String, Object>) envelope.getMessage();
        Map<String, Object> counts = new HashMap<String, Object>();
        Object msg = envelope.getMessage();
        //System.out.println(msg.value());
        counts.put("string", msg);
        System.out.println(counts);
        collector.send(new OutgoingMessageEnvelope(output_stream, msg));
        String HLINE = "-------------------------------------------------------------\n";
        System.out.println(HLINE);
        System.out.println("STREAM Benchmark Test");
        System.out.println(HLINE);
        //ArrayList<Object> a = new ArrayList<Object>();
        //ArrayList<Object> b = new ArrayList<Object>();
        //ArrayList<Object> c = new ArrayList<Object>();
        int STREAM_ARRAY_SIZE = 1000000;
        double[] a = new double[STREAM_ARRAY_SIZE];
        double[] b = new double[STREAM_ARRAY_SIZE];
        double[] c = new double[STREAM_ARRAY_SIZE];
        for (int i = 0; i < STREAM_ARRAY_SIZE; i ++) {
            //a.add(1.0);
            //b.add(2.0);
            //c.add(0.0);
            a[i] = 1.0;
            b[i] = 2.0;
            c[i] = 0.0;
        }
        long t = System.currentTimeMillis();
        for (int i = 0; i < STREAM_ARRAY_SIZE; i ++) {
            //a.set(i, (double)a.get(i)*2.0E0);
            a[i] = a[i]*2.0E0;
        }
        t = System.currentTimeMillis() - t;
        System.out.println("Each test below will take on the order of " + t + "microseconds");
        double scalar = 3.0;
        int NTIMES = 10;
        int j;
        int k;
        long[][] times = new long[4][NTIMES];

        for (k=0; k < NTIMES; k++) {
            times[0][k] = System.currentTimeMillis();
            for (j=0; j < STREAM_ARRAY_SIZE; j++) {
                c[j] = a[j];
            }
            times[0][k] = System.currentTimeMillis() - times[0][k];
            times[1][k] = System.currentTimeMillis();
            for (j=0; j < STREAM_ARRAY_SIZE; j++) {
                b[j] = scalar * c[j];
            }
            times[1][k] = System.currentTimeMillis() - times[1][k];
            times[2][k] = System.currentTimeMillis();
            for (j=0; j < STREAM_ARRAY_SIZE; j++) {
                c[j] = a[j] + b[j];
            }
            times[2][k] = System.currentTimeMillis() - times[2][k];
            times[3][k] = System.currentTimeMillis();
            for (j=0; j < STREAM_ARRAY_SIZE; j++) {
                a[j] = b[j] + scalar * c[j];
            }
            times[3][k] = System.currentTimeMillis() - times[3][k];
        }

        double[] avgtime = new double[4];
        double[] mintime = {1.0E7, 1.0E7, 1.0E7, 1.0E7};
        double[] maxtime = new double[4];
        String[] label = {"Copy:      ", "Scale:     ", "Add:       ", "Triad:     "};
        double [] bytes = {
            2 * 16 * STREAM_ARRAY_SIZE,
            2 * 16 * STREAM_ARRAY_SIZE,
            3 * 16 * STREAM_ARRAY_SIZE,
            3 * 16 * STREAM_ARRAY_SIZE,
        };

        for (k=1; k < NTIMES; k++) {
            for (j=0; j < 4; j++) {
                avgtime[j] = avgtime[j] + times[j][k];
                mintime[j] = mintime[j] > times[j][k] ? times[j][k] : mintime[j];
                maxtime[j] = maxtime[j] > times[j][k] ? maxtime[j] : times[j][k];
            }
        }

        System.out.println("Function    Best Rate MB/s  Avg time     Min time     Max time");
        for (j=0; j < 4; j++) {
            avgtime[j] = avgtime[j]/(double)(NTIMES-1);
            double Rate = 1.0E-06 * bytes[j]/mintime[j];
            String str = label[j] + Rate + "  " + avgtime[j] + "  " + mintime[j] + "  " + maxtime[j];
            System.out.println(str);
        }
    }

    public void init(Config config, TaskContext task){
        System.out.println("----------------------------------------------------------------");
        System.out.println("hhhhhhhhhhhhhhhhhhhhhhhhhhhhh");
        System.out.println("----------------------------------------------------------------");

    }
}
