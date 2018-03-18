package countlatancy.task;

import org.apache.samza.application.StreamApplication;
import org.apache.samza.config.Config;
import org.apache.samza.operators.MessageStream;
import org.apache.samza.operators.OutputStream;
import org.apache.samza.operators.StreamGraph;
import org.apache.samza.operators.functions.FoldLeftFunction;
import org.apache.samza.operators.windows.WindowPane;
import org.apache.samza.operators.windows.Windows;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.time.Duration;
import java.util.Collection;
import java.util.function.Function;
import java.util.*;

// test
public class ShareCJApp implements StreamApplication {

  // private static final Logger LOG = LoggerFactory.getLogger(TumblingPageViewCounterApp.class);
  private static final String INPUT_TOPIC = "FileToStream";
  private static final String OUTPUT_TOPIC = "ShareCJ";
  private static final String FILTER_KEY1 = "D";

  @Override
  public void init(StreamGraph graph, Config config) {

    MessageStream<String> pageViews = graph.<String, String, String>getInputStream(INPUT_TOPIC, (k, v) -> v);

    OutputStream<String, String, String> outputStream = graph
        .getOutputStream(OUTPUT_TOPIC, m -> null, m -> m);
    //OutputStream<String, String, String> outputStream = graph
        //.getOutputStream(OUTPUT_TOPIC, m -> null, m -> m);

     Function<String, String> keyFn = pageView -> pageView;
    //Function<String, String> mapFn = pageView -> new withTime(pageView).getValue();

    pageViews
        .filter((order) -> {
            String[] orderList = order.split("\\|");
            return !FILTER_KEY1.equals(orderList[1]);
        })
        //.filter((order) -> order.split("\\|")[1] != "X")
        .sendTo(outputStream);
  }
}

