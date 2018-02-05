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
        .sendTo(outputStream);
  }
}

