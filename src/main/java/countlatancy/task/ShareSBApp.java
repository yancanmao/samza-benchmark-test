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
    private static final String FILTER_KEY3 = "";
    
    /**
     * deal continous transaction
     * @param poolB,poolS,pool,order
     * @return output string 
     */
    public String transaction(List<Order> poolB, List<Order> poolS, Map<String, List<Order>> pool, Order order) {
        // hava a transaction
        int top = 0;
        int i = 0;
        int j = 0;
        // List<String> complete = new ArrayList<>();
        StringBuilder messageBuilder = new StringBuilder();
        messageBuilder.append("{\"deal\":{");
        // List<Order> completeB = new ArrayList<>();
        while (poolS.get(top).getOrderPrice() <= poolB.get(top).getOrderPrice()) {
            if (poolB.get(top).getOrderVol() > poolS.get(top).getOrderVol()) {
                // B remains B_top-S_top
                poolB.get(top).updateOrder(poolS.get(top).getOrderVol());
                // S complete
                poolS.get(top).updateOrder(poolS.get(top).getOrderVol());
                // add j to complete list
                // complete.add(poolS.get(j).getOrderNo());
                messageBuilder.append("\"").append(poolS.get(top).getOrderNo()).append("\"")
                              .append(":").append("\"").append(poolS.get(top).objToString()).append("\"").append(",");
                // remove top of poolS
                poolS.remove(top);
                // no order in poolS, transaction over
                if (poolS.isEmpty()) {
                    break;
                }
                // TODO: output poolB poolS price etc
            } else {
                poolB.get(top).updateOrder(poolB.get(top).getOrderVol());
                poolS.get(top).updateOrder(poolB.get(top).getOrderVol());
                // add top to complete list
                // complete.add(poolB.get(i).getOrderNo());
                // messageBuilder.append(poolB.get(i).getOrderNo()).append(" ");
                messageBuilder.append("\"").append(poolB.get(top).getOrderNo()).append("\"")
                              .append(":").append("\"").append(poolB.get(top).objToString()).append("\"").append(",");
                poolB.remove(top);
                // no order in poolB, transaction over
                if (poolB.isEmpty()) {
                    break;
                }
                // TODO: output poolB poolS price etc
            }
        }
        messageBuilder.deleteCharAt(messageBuilder.length()-1);
        messageBuilder.append("},");
        pool.put(order.getSecCode()+"S", poolS);
        pool.put(order.getSecCode()+"B", poolB);
        // put pool into messageBuilder
        messageBuilder.append("\"poolS\":{");
        if (!poolS.isEmpty()) {
            for (int p = 0; p < poolS.size(); p++) {
                messageBuilder.append("\"").append(poolS.get(p).getOrderNo()).append("\"")
                              .append(":").append("\"").append(poolS.get(p).getOrderVol()).append("\"").append(",");
            }
        }
        messageBuilder.deleteCharAt(messageBuilder.length()-1);
        messageBuilder.append("},");
        messageBuilder.append("\"poolB\":{");
        if (!poolB.isEmpty()) {
            for (int q = 0; q < poolB.size(); q++) {
                messageBuilder.append("\"").append(poolB.get(q).getOrderNo()).append("\"")
                              .append(":").append("\"").append(poolB.get(q).getOrderVol()).append("\"").append(",");
            }
        }
        messageBuilder.deleteCharAt(messageBuilder.length()-1);
        messageBuilder.append("}}");
        // output complete order
        return messageBuilder.toString();
    }

    /**
     * load file into buffer
     * @param file
     * @return List<Order>
     */
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
          // .filter((order) -> !FILTER_KEY1.equals(order.getTranMaintCode()))
          .filter((order) -> !FILTER_KEY2.equals(order.getTranMaintCode()))
          .filter((order) -> !FILTER_KEY3.equals(order.getTranMaintCode()))
          .map((order)->{
              String complete = new String();
              // load poolS poolB
              List<Order> poolS = pool.get(order.getSecCode()+"S");
              List<Order> poolB = pool.get(order.getSecCode()+"B");
              if (poolB == null) {
                  poolB = new ArrayList<>();
              }
              if (poolS == null) {
                  poolS = new ArrayList<>();
              }
              if (order.getTradeDir().equals("B")) {
                  // if order tran_maint_code is "D", delete from pool
                  if (FILTER_KEY1.equals(order.getTranMaintCode())) {
                      // if exist in order, remove from pool
                      String orderNo = order.getOrderNo();
                      for (int i=0; i < poolB.size(); i++) {
                          if (orderNo.equals(poolB.get(i).getOrderNo())) {
                              poolB.remove(i);
                              pool.put(order.getSecCode()+"B", poolB);
                              return "{\"result\":\"delete B order:" + orderNo+"\"}";
                          }
                      }
                      // else output no delete order exist
                      return "{\"result\":\"no such B order to delete:" + orderNo+"\"}";              
                  }
                  // if no elements in poolS, no transaction, add poolB
                  if (poolS.isEmpty()) {
                      poolB.add(order);
                      pool.put(order.getSecCode()+"B", poolB);
                      complete = "{\"result\":\"empty poolS, no transaction\"}";
                      return complete;
                  }
                  float orderPrice = order.getOrderPrice();
                  // put into buy poolB
                  if (!poolB.isEmpty()) {
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
                  } else {
                      poolB.add(order);
                  }
                  
                  // no satisfied price
                  if (poolS.get(0).getOrderPrice() > poolB.get(0).getOrderPrice()) {
                      // this.savepool();
                      pool.put(order.getSecCode()+"S", poolS);
                      pool.put(order.getSecCode()+"B", poolB);
                      complete = "{\"result\":\"no price match, no transaction\"}";
                  } else {
                      complete = this.transaction(poolB, poolS, pool, order);
                  }
              } else if (order.getTradeDir().equals("S")) {
                  // if order tran_maint_code is "D", delete from pool
                  if (FILTER_KEY1.equals(order.getTranMaintCode())) {
                      // if exist in order, remove from pool
                      String orderNo = order.getOrderNo();
                      for (int i=0; i < poolS.size(); i++) {
                          if (orderNo.equals(poolS.get(i).getOrderNo())) {
                              poolS.remove(i);
                              pool.put(order.getSecCode()+"S", poolS);
                              return "{\"result\":\"delete S order:" + orderNo+"\"}";
                          }
                      }
                      // else output no delete order exist
                      return "{\"result\":\"no such S order to delete:"+ orderNo+"\"}";              
                  }
                  // if no elements in poolB, no transaction, add poolS
                  if (poolB.isEmpty()) {
                      poolS.add(order);
                      pool.put(order.getSecCode()+"S", poolS);
                      complete = "{\"result\":\"empty poolB, no transaction\"}";
                      return complete;
                  }
                  float orderPrice = order.getOrderPrice();
                  // put into buy poolS
                  if (!poolS.isEmpty()) {
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
                  } else {
                      poolS.add(order);
                  }
                  
                  // no satisfied price
                  if (poolS.get(0).getOrderPrice() > poolB.get(0).getOrderPrice()) {
                      // order.savepool();
                      pool.put(order.getSecCode()+"S", poolS);
                      pool.put(order.getSecCode()+"B", poolB);
                      complete = "{\"result\":\"no price match, no transaction\"}";
                  } else {
                      complete = this.transaction(poolB, poolS, pool, order);
                  }
              } else {
                  return "{\"error\":\"wrong getTradeDir\"}";
              }
              return complete;
          })
          .sendTo(outputStream);
    }
}

