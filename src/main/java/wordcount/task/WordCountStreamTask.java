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

package wordcount.task;

import java.security.Key;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;

import javafx.concurrent.Task;
import org.apache.samza.config.Config;
import org.apache.samza.metrics.Counter;
import org.apache.samza.storage.kv.KeyValueStore;
import org.apache.samza.system.IncomingMessageEnvelope;
import org.apache.samza.system.OutgoingMessageEnvelope;
import org.apache.samza.system.SystemStream;
import org.apache.samza.task.InitableTask;
import org.apache.samza.task.MessageCollector;
import org.apache.samza.task.StreamTask;
import org.apache.samza.task.TaskContext;
import org.apache.samza.task.TaskCoordinator;
import org.apache.samza.task.WindowableTask;

public class WordCountStreamTask implements InitableTask, StreamTask, WindowableTask {
    private static final SystemStream OUTPUT_STREAM = new SystemStream("kafka", "wordcount-count");
    private KeyValueStore<String, Integer> store;
    private Map<String, Integer> counts = new HashMap<String, Integer>();
    public void init(Config config, TaskContext taskContext) throws Exception {
        //this.store = (KeyValueStore<String, Integer>) taskContext.getStore("wordcount-count ");
        counts = new HashMap<String, Integer>();
    }
    @Override
    public void process(IncomingMessageEnvelope envelope, MessageCollector collector, TaskCoordinator coordinator) {
        Map<String, Object> token = (Map<String, Object>) envelope.getMessage();
        String word = (String)token.get("word");
        Integer count = counts.get(word);
        if (count == null) count = 0;
        count ++;
        counts.put(word, count);
    }
    @Override
    public void window(MessageCollector collector, TaskCoordinator coordinator) {
        collector.send(new OutgoingMessageEnvelope(OUTPUT_STREAM, counts));
        counts.clear();
    }
}
