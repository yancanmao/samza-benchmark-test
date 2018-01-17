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
