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
import java.util.HashMap;
import java.util.Map;
import org.apache.samza.system.IncomingMessageEnvelope;
import org.apache.samza.system.OutgoingMessageEnvelope;
import org.apache.samza.system.SystemStream;
import org.apache.samza.task.MessageCollector;
import org.apache.samza.task.StreamTask;
import org.apache.samza.task.TaskCoordinator;

/*

 */
public class WordParserStreamTask implements StreamTask {
    private static final SystemStream OUTPUT_STREAM = new SystemStream("kafka", "wordcount-word");
    @SuppressWarnings("unchecked")
    @Override
    public void process(IncomingMessageEnvelope envelope, MessageCollector collector, TaskCoordinator coordinator){
        Map<String, Object> jsonObject = (Map<String, Object>) envelope.getMessage();
        String raw = (String)jsonObject.get("raw");
        if(raw != null){
            String delims = "[ ]+";
            String[] tokens = raw.split(delims);
            for (String token: tokens){
                Map<String, Object> parsedjsonObject = new HashMap<String, Object>();
                parsedjsonObject.put("word",token);
                collector.send(new OutgoingMessageEnvelope(OUTPUT_STREAM, parsedjsonObject));
            }
        }
    }
}
