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
    public String transaction(Map<Float, List<Order>> poolB, Map<Float, List<Order>> poolS,
                              List<Float> poolPriceB, List<Float> poolPriceS, Map<String, List<Float>> poolPrice,
                              Map<String, Map<Float, List<Order>>> pool, Order order) {
        // hava a transaction
        int top = 0;
        int i = 0;
        int j = 0;
        int otherOrderVol;
        // List<String> complete = new ArrayList<>();
        StringBuilder messageBuilder = new StringBuilder();
        messageBuilder.append("{\"deal\":{");
        // List<Order> completeB = new ArrayList<>();
        while (poolPriceS.get(top) <= poolPriceB.get(top)) {
            if (poolB.get(poolPriceB.get(top)).get(top).getOrderVol() > poolS.get(poolPriceS.get(top)).get(top).getOrderVol()) {
                // B remains B_top-S_top
                otherOrderVol = poolS.get(poolPriceS.get(top)).get(top).getOrderVol();
                poolB.get(poolPriceB.get(top)).get(top).updateOrder(otherOrderVol);
                // S complete
                poolS.get(poolPriceS.get(top)).get(top).updateOrder(otherOrderVol);
                // add j to complete list
                // complete.add(poolS.get(j).getOrderNo());
                messageBuilder.append("\"").append(poolS.get(poolPriceS.get(top)).get(top).getOrderNo()).append("\"")
                              .append(":").append("\"").append(poolS.get(poolPriceS.get(top)).get(top).objToString())
                              .append("\"").append(",");
                messageBuilder.append("\"").append(poolB.get(poolPriceB.get(top)).get(top).getOrderNo()).append("\"")
                              .append(":").append("\"").append(poolB.get(poolPriceB.get(top)).get(top).objToString())
                              .append("\"").append(",");
                // remove top of poolS
                poolS.get(poolPriceS.get(top)).remove(top);
                // no order in poolS, transaction over
                if (poolS.get(poolPriceS.get(top)).isEmpty()) {
                    // find next price
                    poolS.remove(poolPriceS.get(top));
                    poolPriceS.remove(top);
                    if (poolPriceS.isEmpty()) {
                        break;
                    }
                }
                // TODO: output poolB poolS price etc
            } else {
                otherOrderVol = poolB.get(poolPriceB.get(top)).get(top).getOrderVol();
                poolB.get(poolPriceB.get(top)).get(top).updateOrder(otherOrderVol);
                poolS.get(poolPriceS.get(top)).get(top).updateOrder(otherOrderVol);
                // add top to complete list
                // complete.add(poolB.get(i).getOrderNo());
                // messageBuilder.append(poolB.get(i).getOrderNo()).append(" ");
                messageBuilder.append("\"").append(poolS.get(poolPriceS.get(top)).get(top).getOrderNo()).append("\"")
                              .append(":").append("\"").append(poolS.get(poolPriceS.get(top)).get(top).objToString())
                              .append("\"").append(",");
                messageBuilder.append("\"").append(poolB.get(poolPriceB.get(top)).get(top).getOrderNo()).append("\"")
                              .append(":").append("\"").append(poolB.get(poolPriceB.get(top)).get(top).objToString())
                              .append("\"").append(",");
                poolB.get(poolPriceB.get(top)).remove(top);
                // no order in poolB, transaction over
                if (poolB.get(poolPriceB.get(top)).isEmpty()) {
                    poolB.remove(poolPriceB.get(top));
                    poolPriceB.remove(top);
                    if (poolPriceB.isEmpty()) {
                        break;
                    }
                }
                // TODO: output poolB poolS price etc
            }
        }
        messageBuilder.deleteCharAt(messageBuilder.length()-1);
        messageBuilder.append("}");
        pool.put(order.getSecCode()+"S", poolS);
        pool.put(order.getSecCode()+"B", poolB);
        poolPrice.put(order.getSecCode()+"B", poolPriceB);
        poolPrice.put(order.getSecCode()+"S", poolPriceS);
        messageBuilder.append("}");
        // output complete order
        return messageBuilder.toString();
    }

    /**
     * load file into buffer
     * @param file
     * @return List<Order>
     */
    public Pool loadPool(String file) {
      FileReader stream = null;
      BufferedReader br = null;
      String sCurrentLine;
      Map<Float, List<Order>> poolI = new HashMap<Float, List<Order>>();
      List<Order> orderList = new ArrayList<>();
      List<Float> pricePoolI = new ArrayList<>();
      File textFile = new File(file);
      // if file exists
      if (!textFile.exists()) {
          return new Pool(poolI, pricePoolI);
      }

      try{
        stream = new FileReader(file);

        br = new BufferedReader(stream);
        while ((sCurrentLine = br.readLine()) != null) {
            Order order = new Order(sCurrentLine);
            orderList = poolI.get(order.getOrderPrice());
            if (orderList == null) {
                pricePoolI.add(order.getOrderPrice());
                orderList = new ArrayList<>();
            }
            orderList.add(order);
            poolI.put(order.getOrderPrice(), orderList);
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
      Pool poolThis = new Pool(poolI, pricePoolI);
      return poolThis;
    }

    /**
     * mapFunction
     * @param pool, order
     * @return String
     */
    public String mapFunction(Map<String, Map<Float, List<Order>>> pool, Map<String, List<Float>> poolPrice, Order order) {
        String complete = new String();
        // load poolS poolB
        Map<Float, List<Order>> poolS = pool.get(order.getSecCode()+"S");
        Map<Float, List<Order>> poolB = pool.get(order.getSecCode()+"B");
        List<Float> poolPriceB = poolPrice.get(order.getSecCode()+"B");
        List<Float> poolPriceS = poolPrice.get(order.getSecCode()+"S");
        if (poolB == null) {
            poolB = new HashMap<Float, List<Order>>();
        }
        if (poolS == null) {
            poolS = new HashMap<Float, List<Order>>();
        }
        if (poolPriceB == null) {
            poolPriceB = new ArrayList<>();
        }
        if (poolPriceS == null) {
            poolPriceS = new ArrayList<>();
        }
        if (order.getTradeDir().equals("B")) {
            float orderPrice = order.getOrderPrice();
            List<Order> BorderList = poolB.get(orderPrice);
            // if order tran_maint_code is "D", delete from pool
            if (FILTER_KEY1.equals(order.getTranMaintCode())) {
                // if exist in order, remove from pool
                String orderNo = order.getOrderNo();
                if (BorderList == null) {
                    return "{\"result\":\"no such B order to delete:" + orderNo+"\"}";
                }
                for (int i=0; i < BorderList.size(); i++) {
                    if (orderNo.equals(BorderList.get(i).getOrderNo())) {
                        BorderList.remove(i);
                        // if no other price delete poolPrice
                        if (BorderList.isEmpty()) {
                          for (int j=0; j < poolPriceB.size(); j++) {
                            if (poolPriceB.get(j) == orderPrice) {
                              poolPriceB.remove(j);
                              break;
                            }
                          }
                          poolB.remove(orderPrice);
                        } else {
                          poolB.put(orderPrice, BorderList);
                        }
                        poolPrice.put(order.getSecCode()+"B", poolPriceB);
                        pool.put(order.getSecCode()+"B", poolB);
                        return "{\"result\":\"delete B order:" + orderNo+"\"}";
                    }
                }
                // else output no delete order exist
                return "{\"result\":\"no such B order to delete:" + orderNo+"\"}";              
             }
            
            // put into buy poolB
            if (BorderList == null) {
                BorderList = new ArrayList<>();
                // price add a value
                if (poolB.isEmpty()) {
                    poolPriceB.add(orderPrice);
                } else {
                    for (int i = 0; i < poolPriceB.size(); i++) {
                        if (poolPriceB.get(i) < orderPrice) {
                            poolPriceB.add(i, orderPrice);
                            break;
                        }
                        if (i == poolPriceB.size()-1) {
                            poolPriceB.add(orderPrice);
                            break;
                        }
                    }
                }
            }
            BorderList.add(order);
            poolB.put(orderPrice, BorderList);

            // if no elements in poolS, no transaction, add poolB
            if (poolPriceS.isEmpty()) {
                pool.put(order.getSecCode()+"B", poolB);
                poolPrice.put(order.getSecCode()+"B", poolPriceB);
                complete = "{\"result\":\"empty poolS, no transaction\"}";
                return complete;
            }

            // no satisfied price
            if (poolPriceS.get(0) > poolPriceB.get(0)) {
                // this.savepool();
                pool.put(order.getSecCode()+"S", poolS);
                pool.put(order.getSecCode()+"B", poolB);
                poolPrice.put(order.getSecCode()+"S", poolPriceS);
                poolPrice.put(order.getSecCode()+"B", poolPriceB);
                complete = "{\"result\":\"no price match, no transaction\"}";
            } else {
                complete = this.transaction(poolB, poolS, poolPriceB, poolPriceS, poolPrice, pool, order);
            }
        } else if (order.getTradeDir().equals("S")) {
            float orderPrice = order.getOrderPrice();
            List<Order> SorderList = poolS.get(orderPrice);
            // if order tran_maint_code is "D", delete from pool
            if (FILTER_KEY1.equals(order.getTranMaintCode())) {
                // if exist in order, remove from pool
                String orderNo = order.getOrderNo();
                if (SorderList == null) {
                    return "{\"result\":\"no such S order to delete:" + orderNo+"\"}";
                }
                for (int i=0; i < SorderList.size(); i++) {
                    if (orderNo.equals(SorderList.get(i).getOrderNo())) {
                        SorderList.remove(i);
                        // if no other price delete poolPrice
                        if (SorderList.isEmpty()) {
                          for (int j=0; j < poolPriceS.size(); j++) {
                            if (poolPriceS.get(j) == orderPrice) {
                              poolPriceS.remove(j);
                              break;
                            }
                          }
                          poolS.remove(orderPrice);
                        } else {
                          poolS.put(orderPrice, SorderList);
                        }
                        poolPrice.put(order.getSecCode()+"S", poolPriceS);
                        pool.put(order.getSecCode()+"S", poolS);
                        return "{\"result\":\"delete S order:" + orderNo+"\"}";
                    }
                }
                // else output no delete order exist
                return "{\"result\":\"no such S order to delete:" + orderNo+"\"}";
            }
            
            // put into buy poolS
            if (SorderList == null) {
                SorderList = new ArrayList<>();
                // price add a value
                if (poolS.isEmpty()) {
                    poolPriceS.add(orderPrice);
                } else {
                    for (int i = 0; i < poolPriceS.size(); i++) {
                        if (poolPriceS.get(i) > orderPrice) {
                            poolPriceS.add(i, orderPrice);
                            break;
                        }
                        if (i == poolPriceS.size()-1) {
                            poolPriceS.add(orderPrice);
                            break;
                        }
                    }
                }
            }
            SorderList.add(order);
            poolS.put(orderPrice, SorderList);
            // if no elements in poolB, no transaction, add poolS
            if (poolPriceB.isEmpty()) {
                pool.put(order.getSecCode()+"S", poolS);
                poolPrice.put(order.getSecCode()+"S", poolPriceS);
                complete = "{\"result\":\"empty poolB, no transaction\"}";
                return complete;
            }
            
            // no satisfied price
            if (poolPriceS.get(0) > poolPriceB.get(0)) {
                // this.savepool();
                pool.put(order.getSecCode()+"S", poolS);
                pool.put(order.getSecCode()+"B", poolB);
                poolPrice.put(order.getSecCode()+"S", poolPriceS);
                poolPrice.put(order.getSecCode()+"B", poolPriceB);
                complete = "{\"result\":\"no price match, no transaction\"}";
            } else {
                complete = this.transaction(poolB, poolS, poolPriceB, poolPriceS, poolPrice, pool, order);
            }
        } else {
            return "{\"error\":\"wrong getTradeDir\"}";
        }
        return complete;
    }

    @Override
    public void init(StreamGraph graph, Config config) {

        MessageStream<String> orderStream = graph.<String, String, String>getInputStream(INPUT_TOPIC, (k, v) -> v);

        OutputStream<String, String, String> outputStream = graph
            .getOutputStream(OUTPUT_TOPIC, m -> null, m -> m);

        // TODO: load pool into mem
        // File dirFile = new File("/home/myc/workspace/share/opening");
        File dirFile = new File("/root/share/opening");
        String[] fileList = dirFile.list();
        Map<String, Map<Float, List<Order>>> pool = new HashMap<String, Map<Float, List<Order>>>();
        Map<String, List<Float>> poolPrice = new HashMap<String, List<Float>>();
        for (int i = 0; i < fileList.length; i++) {
          String fileName = fileList[i];
          File file = new File(dirFile.getPath(),fileName);
          Pool poolS = this.loadPool(file.getPath() +"/S.txt");
          Pool poolB = this.loadPool(file.getPath() +"/B.txt");
          pool.put(file.getName()+"S", poolS.getPool());
          pool.put(file.getName()+"B", poolB.getPool());
          poolPrice.put(file.getName()+"S", poolS.getPricePool());
          poolPrice.put(file.getName()+"B", poolB.getPricePool());
        }

        orderStream
          .map((tuple)->{
            // String[] orderList = tuple.split("\\|");
            Order order = new Order(tuple);
            return order;
          })
          //.partitionBy((order)->order.getSecCode())
          // .filter((order) -> !FILTER_KEY1.equals(order.getTranMaintCode()))
          .filter((order) -> !FILTER_KEY2.equals(order.getTranMaintCode()))
          .filter((order) -> !FILTER_KEY3.equals(order.getTranMaintCode()))
          .map((order)->{
              return this.mapFunction(pool, poolPrice, order);
          })
          .sendTo(outputStream);
    }
}

