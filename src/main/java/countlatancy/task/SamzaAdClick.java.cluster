package countlatancy.task;

import org.apache.samza.application.StreamApplication;
import org.apache.samza.config.Config;
import org.apache.samza.operators.MessageStream;
import org.apache.samza.operators.OutputStream;
import org.apache.samza.operators.StreamGraph;
import org.apache.samza.operators.functions.JoinFunction;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.time.Duration;
import java.util.function.Function;

/**
 * function join adclick
 */
public class SamzaAdClick implements StreamApplication {

  // private static final Logger LOG = LoggerFactory.getLogger(PageViewAdClickJoiner.class);
  private static final String INPUT_TOPIC1 = "Advertisement";
  private static final String INPUT_TOPIC2 = "AdvClick";

  private static final String OUTPUT_TOPIC = "SamzaAdClick";

  @Override
  public void init(StreamGraph graph, Config config) {

    MessageStream<String> advertisement = graph.<String, String, String>getInputStream(INPUT_TOPIC1, (k, v) -> v);
    MessageStream<String> adClicks = graph.<String, String, String>getInputStream(INPUT_TOPIC2, (k, v) -> v);

    OutputStream<String, String, String> outputStream = graph
        .getOutputStream(OUTPUT_TOPIC, m -> "", m -> m);

    Function<String, String> adKeyFn = adMsg -> new withTime(adMsg).getId();
    Function<String, String> adClickKeyFn = adClick -> new withTime(adClick).getId();

    MessageStream<String> adRepartitioned = advertisement.partitionBy(adKeyFn);
    MessageStream<String> adClickRepartitioned = adClicks.partitionBy(adClickKeyFn);

    adRepartitioned.join(adClickRepartitioned, new JoinFunction<String, String, String, String>() {
    //advertisement.join(adClicks, new JoinFunction<String, String, String, String>() {

      @Override
      public String apply(String advertisementMsg, String adClickMsg) {
        withTime advertisement = new withTime(advertisementMsg);
        withTime adClick = new withTime(adClickMsg);
        String joinResult = String.format("%s,%s,%s", advertisement.getId(), advertisement.getTime(), adClick.getTime());
        return joinResult;
      }

      @Override
      public String getFirstKey(String msg) {
        return new withTime(msg).getId();
      }

      @Override
      public String getSecondKey(String msg) {
        return new withTime(msg).getId();
      }
    }, Duration.ofMinutes(3)).sendTo(outputStream);
  }
}
