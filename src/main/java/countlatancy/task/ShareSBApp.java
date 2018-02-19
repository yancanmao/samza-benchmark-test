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
import java.io.*;

// test
public class ShareSBApp implements StreamApplication {

    // private static final Logger LOG = LoggerFactory.getLogger(TumblingPageViewCounterApp.class);
    private static final String INPUT_TOPIC = "FileToStream";
    private static final String OUTPUT_TOPIC = "ShareSB";
    private static final String FILTER_KEY1 = "D";
    private static final String FILTER_KEY2 = "X";
    
    // TODO: transaction
    public String transaction(List<Order> poolB, List<Order> poolS, Map<String, List<Order>> pool, Order order) {
        // hava a transaction
        int i = 0;
        int j = 0;
        // create a array to save dealed order to remove
        List<Integer> indexS = new ArrayList<>();
        List<Integer> indexB = new ArrayList<>();
        // List<String> complete = new ArrayList<>();
        StringBuilder messageBuilder = new StringBuilder();
        // List<Order> completeB = new ArrayList<>();
        while (poolS.get(j).getOrderPrice() <= poolB.get(i).getOrderPrice()) {
            if (poolB.get(i).getOrderVol() > poolS.get(j).getOrderVol()) {
                // B remains i-j
                poolB.get(i).updateOrder(poolS.get(j).getOrderVol());
                // S complete
                poolS.get(j).updateOrder(poolS.get(j).getOrderVol());
                // add j to complete list
                // complete.add(poolS.get(j).getOrderNo());
                messageBuilder.append(poolS.get(j).getOrderNo()).append(" ");
                // add chengjiao order to index
                indexS.add(j);
                // poolS.remove(j);
                if (j == poolS.size()-1) {
                    break;
                }
                j++;
                // TODO: output poolB poolS price etc
            } else {
                poolB.get(i).updateOrder(poolB.get(i).getOrderVol());
                poolS.get(j).updateOrder(poolB.get(i).getOrderVol());
                // add j to complete list
                // complete.add(poolB.get(i).getOrderNo());
                messageBuilder.append(poolB.get(i).getOrderNo()).append(" ");
                // add chengjiao order to index
                indexB.add(i);
                // poolB.remove(i);
                if (i == poolB.size()-1) {
                    break;
                }
                i++;
                // TODO: output poolB poolS price etc
            }
        }
        // remove dealed order
        if (!indexS.isEmpty()) {
            for (int i=0; i<indexS.size(); i++) {
                curIndex = indexS.get(i);
                poolS.remove(curIndex);
            }
        }
        if (!indexB.isEmpty()) {
            for (int i=0; i<indexB.size(); i++) {
                curIndex = indexB.get(i);
                poolB.remove(curIndex);
            }
        }
        pool.put(order.getSecCode()+"S", poolS);
        pool.put(order.getSecCode()+"B", poolB);
        // output complete order
        return messageBuilder.toString();
    }

    public List<Order> loadPool(String file) {
      FileReader stream = null;
      BufferedReader br = null;
      String sCurrentLine;
      List<Order> pool = new ArrayList<>();
      File textFile = new File(file);
      // if file exists
      if (!textFile.exists()) {
          return pool;
      }

      try{
        stream = new FileReader(file);

        br = new BufferedReader(stream);
        while ((sCurrentLine = br.readLine()) != null) {
            pool.add(new Order(sCurrentLine));
        }
      } catch (IOException e) {
          e.printStackTrace();
      } finally {
          try {
              if (stream != null) stream.close();
              if (br != null) br.close();
          } catch (IOException ex) {
              ex.printStackTrace();
          }
      }
      return pool;
    }

    @Override
    public void init(StreamGraph graph, Config config) {

        MessageStream<String> orderStream = graph.<String, String, String>getInputStream(INPUT_TOPIC, (k, v) -> v);

        OutputStream<String, String, String> outputStream = graph
            .getOutputStream(OUTPUT_TOPIC, m -> null, m -> m);

        // TODO: load pool into mem
        File dirFile = new File("/root/share/opening");
        String[] fileList = dirFile.list();
        Map<String, List<Order>> pool = new HashMap<String, List<Order>>();
        for (int i = 0; i < fileList.length; i++) {
          String fileName = fileList[i];
          File file = new File(dirFile.getPath(),fileName);
          List<Order> poolS = this.loadPool(file.getPath() +"/S.txt");
          List<Order> poolB = this.loadPool(file.getPath() +"/B.txt");
          pool.put(file.getName()+"S", poolS);
          pool.put(file.getName()+"B", poolB);
        }

        orderStream
          .map((tuple)->{
            // String[] orderList = tuple.split("\\|");
            Order order = new Order(tuple);
            return order;
          })
          .filter((order) -> order.getTranMaintCode() != "D")
          .filter((order) -> order.getTranMaintCode() != "X")
          .map((order)->{
              String complete = new String();
              if (order.getTradeDir() == "B") {
                  List<Order> poolS = pool.get(order.getSecCode()+"S");
                  List<Order> poolB = pool.get(order.getSecCode()+"B");
                  // if no elements in poolS or poolB, add poolB
                  if (poolS.isEmpty() || poolB.isEmpty()) {
                      poolB.add(order);
                      pool.put(order.getSecCode()+"B", poolB);
                      complete = "no transaction";
                      return complete;
                  }
                  float orderPrice = order.getOrderPrice();
                  // put into buy poolB
                  for (int i = 0; i < poolB.size(); i++) {
                      if (poolB.get(i).getOrderPrice() < orderPrice) {
                          poolB.add(i, order);
                          break;
                      }
                      if (i == poolB.size()-1) {
                          poolB.add(order);
                          break;
                      }
                  }
                  
                  // no satisfied price
                  if (poolS.get(0).getOrderPrice() > poolB.get(0).getOrderPrice()) {
                      // this.savepool();
                      pool.put(order.getSecCode()+"S", poolS);
                      pool.put(order.getSecCode()+"B", poolB);
                  } else {
                      complete = this.transaction(poolB, poolS, pool, order);
                   }
              } else {
                  List<Order> poolB = pool.get(order.getSecCode()+"B");
                  List<Order> poolS = pool.get(order.getSecCode()+"S");
                  // if no elements in poolS or poolB, add poolS
                  if (poolS.isEmpty() || poolB.isEmpty()) {
                      poolS.add(order);
                      pool.put(order.getSecCode()+"S", poolS);
                      complete = "no transaction";
                      return complete;
                  }
                  float orderPrice = order.getOrderPrice();
                  // put into buy poolS
                  for (int i = 0; i < poolS.size(); i++) {
                      if (poolS.get(i).getOrderPrice() > orderPrice) {
                          poolS.add(i, order);
                          break;
                      }
                      if (i == poolS.size()-1) {
                          poolS.add(order);
                          break;
                      }
                  }
                  
                  // no satisfied price
                  if (poolS.get(0).getOrderPrice() > poolB.get(0).getOrderPrice()) {
                      // order.savepool();
                      pool.put(order.getSecCode()+"S", poolS);
                      pool.put(order.getSecCode()+"B", poolB);
                  } else {
                      complete = this.transaction(poolB, poolS, pool, order);
                  }
              }
              return complete;
          })
          .sendTo(outputStream);
    }
}

