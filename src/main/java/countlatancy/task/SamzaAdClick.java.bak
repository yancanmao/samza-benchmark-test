/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *   http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */
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

  private static final String OUTPUT_TOPIC = "SamzaAdvClick";

  @Override
  public void init(StreamGraph graph, Config config) {

    MessageStream<String> advertisement = graph.<String, String, String>getInputStream(INPUT_TOPIC1, (k, v) -> v);
    MessageStream<String> advClicks = graph.<String, String, String>getInputStream(INPUT_TOPIC2, (k, v) -> v);

    OutputStream<String, String, String> outputStream = graph
        .getOutputStream(OUTPUT_TOPIC, m -> "", m -> m);

    // Function<String, String> pageViewKeyFn = pageView -> new PageView(pageView).getPageId();
    // Function<String, String> adClickKeyFn = adClick -> new AdClick(adClick).getPageId();

    // MessageStream<String> pageViewRepartitioned = pageViews.partitionBy(pageViewKeyFn);
    // MessageStream<String> adClickRepartitioned = adClicks.partitionBy(adClickKeyFn);

    advertisement.join(advClicks, new JoinFunction<String, String, String, String>() {

      @Override
      public String apply(String advertisementMsg, String advClickMsg) {
        withTime advertisement = new withTime(advertisementMsg);
        withTime advClick = new withTime(advClickMsg);
        String joinResult = String.format("%s,%s,%s", advertisement.getId(), advertisement.getTime(), advClick.getTime());
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
