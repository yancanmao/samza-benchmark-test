package countlatancy.task;

import org.apache.samza.application.StreamApplication;
import org.apache.samza.config.Config;
import org.apache.samza.operators.MessageStream;
import org.apache.samza.operators.OutputStream;
import org.apache.samza.operators.StreamGraph;
import org.apache.samza.operators.functions.FoldLeftFunction;
import org.apache.samza.operators.windows.WindowPane;
import org.apache.samza.operators.windows.Windows;
import org.apache.samza.task.TaskContext;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.time.Duration;
import java.util.Collection;
import java.util.function.Function;
import java.util.*;
import java.io.*;
import org.json.JSONObject;

// test
public class ShareSBApp implements StreamApplication {

    // private static final Logger LOG = LoggerFactory.getLogger(TumblingPageViewCounterApp.class);
    private static final String INPUT_TOPIC = "FileToStream";
    private static final String OUTPUT_TOPIC = "ShareSB";
    private static final String FILTER_KEY1 = "D";
    private static final String FILTER_KEY2 = "X";
    private static final String FILTER_KEY3 = "";

    @Override
    public void init(StreamGraph graph, Config config) {

        String windowInterval = config.get("windowInterval", "3");
        String groupByKey = config.get("groupByKey", "Sec_Code");

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
          .filter((completeOrder) -> !completeOrder.isEmpty())
          // .map((completeOrder) -> {
          //     return this.listToString(completeOrder);
          // })
          .window(Windows.tumblingWindow(Duration.ofSeconds(Long.parseLong(windowInterval)), stockStats::new, new stockStatsAggregator(groupByKey)))
          .map(this::formatOutput)
          .sendTo(outputStream);
    }
    
    /**
     * deal continous transaction
     * @param poolB,poolS,pool,order
     * @return output string 
     */
    public List<Order> transaction(Map<Float, List<Order>> poolB, Map<Float, List<Order>> poolS,
                              List<Float> poolPriceB, List<Float> poolPriceS, Map<String, List<Float>> poolPrice,
                              Map<String, Map<Float, List<Order>>> pool, Order order) {
        // hava a transaction
        int top = 0;
        int i = 0;
        int j = 0;
        int otherOrderVol;
        int totalVol = 0;
        List<Order> completeOrder = new ArrayList<>();
        // StringBuilder messageBuilder = new StringBuilder();
        // messageBuilder.append("{\"process_no\":\"0\", \"deal\":{");
        // List<Order> completeB = new ArrayList<>();
        while (poolPriceS.get(top) <= poolPriceB.get(top)) {
            if (poolB.get(poolPriceB.get(top)).get(top).getOrderVol() > poolS.get(poolPriceS.get(top)).get(top).getOrderVol()) {
                // B remains B_top-S_top
                otherOrderVol = poolS.get(poolPriceS.get(top)).get(top).getOrderVol();
                // totalVol sum
                // totalVol += otherOrderVol;
                poolB.get(poolPriceB.get(top)).get(top).updateOrder(otherOrderVol);
                // S complete
                poolS.get(poolPriceS.get(top)).get(top).updateOrder(otherOrderVol);
                // add j to complete list
                // complete.add(poolS.get(j).getOrderNo());
                // messageBuilder.append("\"").append(poolS.get(poolPriceS.get(top)).get(top).getOrderNo()).append("\"")
                //               .append(":").append("\"").append(poolS.get(poolPriceS.get(top)).get(top).objToString())
                //               .append("\"").append(",");
                // messageBuilder.append("\"").append(poolB.get(poolPriceB.get(top)).get(top).getOrderNo()).append("\"")
                //               .append(":").append("\"").append(poolB.get(poolPriceB.get(top)).get(top).objToString())
                //               .append("\"").append(",");
                completeOrder.add(poolS.get(poolPriceS.get(top)).get(top));
                // completeOrder.add(poolB.get(poolPriceB.get(top)).get(top));
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
                // totalVol sum
                // totalVol += otherOrderVol;
                poolB.get(poolPriceB.get(top)).get(top).updateOrder(otherOrderVol);
                poolS.get(poolPriceS.get(top)).get(top).updateOrder(otherOrderVol);
                // add top to complete list
                // complete.add(poolB.get(i).getOrderNo());
                // messageBuilder.append(poolB.get(i).getOrderNo()).append(" ");
                // messageBuilder.append("\"").append(poolS.get(poolPriceS.get(top)).get(top).getOrderNo()).append("\"")
                //               .append(":").append("\"").append(poolS.get(poolPriceS.get(top)).get(top).objToString())
                //               .append("\"").append(",");
                // messageBuilder.append("\"").append(poolB.get(poolPriceB.get(top)).get(top).getOrderNo()).append("\"")
                //               .append(":").append("\"").append(poolB.get(poolPriceB.get(top)).get(top).objToString())
                //               .append("\"").append(",");
                // completeOrder.add(poolS.get(poolPriceS.get(top)).get(top));
                completeOrder.add(poolB.get(poolPriceB.get(top)).get(top));
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
        // messageBuilder.deleteCharAt(messageBuilder.length()-1);
        // messageBuilder.append("},");
        pool.put(order.getSecCode()+"S", poolS);
        pool.put(order.getSecCode()+"B", poolB);
        poolPrice.put(order.getSecCode()+"B", poolPriceB);
        poolPrice.put(order.getSecCode()+"S", poolPriceS);
        // add totalVol
        // messageBuilder.append("\"total_vol\":").append(totalVol);
        // messageBuilder.append("}");
        // output complete order
        // return messageBuilder.toString();
        return completeOrder;
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
    public List<Order> mapFunction(Map<String, Map<Float, List<Order>>> pool, Map<String, List<Float>> poolPrice, Order order) {
        // String complete = new String();
        List<Order> completeOrder = new ArrayList<>();
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
                    // return "{\"process_no\":\"11\", \"result\":\"no such B order to delete:" + orderNo+"\"}";
                    return completeOrder;
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
                        // return "{\"process_no\":\"10\", \"result\":\"delete B order:" + orderNo+"\"}";
                        return completeOrder;
                    }
                }
                // else output no delete order exist
                // return "{\"process_no\":\"11\", \"result\":\"no such B order to delete:" + orderNo+"\"}";
                return completeOrder;             
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
                // complete = "{\"process_no\":\"2\", \"result\":\"empty poolS, no transaction\"}";
                // return complete;
                return completeOrder;             
            }

            // no satisfied price
            if (poolPriceS.get(0) > poolPriceB.get(0)) {
                // this.savepool();
                pool.put(order.getSecCode()+"S", poolS);
                pool.put(order.getSecCode()+"B", poolB);
                poolPrice.put(order.getSecCode()+"S", poolPriceS);
                poolPrice.put(order.getSecCode()+"B", poolPriceB);
                // complete = "{\"process_no\":\"3\", \"result\":\"no price match, no transaction\"}";
                return completeOrder;
            } else {
                completeOrder = this.transaction(poolB, poolS, poolPriceB, poolPriceS, poolPrice, pool, order);
            }
        } else if (order.getTradeDir().equals("S")) {
            float orderPrice = order.getOrderPrice();
            List<Order> SorderList = poolS.get(orderPrice);
            // if order tran_maint_code is "D", delete from pool
            if (FILTER_KEY1.equals(order.getTranMaintCode())) {
                // if exist in order, remove from pool
                String orderNo = order.getOrderNo();
                if (SorderList == null) {
                    // return "{\"process_no\":\"11\", \"result\":\"no such S order to delete:" + orderNo+"\"}";
                    return completeOrder;
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
                        // return "{\"process_no\":\"10\", \"result\":\"delete S order:" + orderNo+"\"}";
                        return completeOrder;
                    }
                }
                // else output no delete order exist
                // return "{\"process_no\":\"11\", \"result\":\"no such S order to delete:" + orderNo+"\"}";
                return completeOrder;
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
                // complete = "{\"process_no\":\"2\", \"result\":\"empty poolB, no transaction\"}";
                // return complete;
                return completeOrder;
            }
            
            // no satisfied price
            if (poolPriceS.get(0) > poolPriceB.get(0)) {
                // this.savepool();
                pool.put(order.getSecCode()+"S", poolS);
                pool.put(order.getSecCode()+"B", poolB);
                poolPrice.put(order.getSecCode()+"S", poolPriceS);
                poolPrice.put(order.getSecCode()+"B", poolPriceB);
                // complete = "{\"process_no\":\"3\", \"result\":\"no price match, no transaction\"}";
                return completeOrder;
            } else {
                completeOrder = this.transaction(poolB, poolS, poolPriceB, poolPriceS, poolPrice, pool, order);
            }
        }
        return completeOrder;
    }

    /**
     * listToString
     * @param completeOrder
     * @return String
     */
    public String listToString(List<Order> completeOrder) {
        StringBuilder messageBuilder = new StringBuilder();
        messageBuilder.append("{\"deal\":{");
        for (int i=0; i < completeOrder.size(); i++) {
            messageBuilder.append("\"").append(completeOrder.get(i).getOrderNo()).append("\"")
                          .append(":").append("\"").append(completeOrder.get(i).objToString())
                          .append("\"").append(",");
        }
        messageBuilder.deleteCharAt(messageBuilder.length()-1);
        messageBuilder.append("}");
        messageBuilder.append("}");
        // output complete order
        return messageBuilder.toString();
    }

    /**
     * A few statistics about the incoming messages.
     */
    private static class stockStats {
        int totalTradeNum = 0;
        float minimum = 0;
        // String tradeOrder = new String();
        // String deleteOrder = new String();
        Map<String, Integer> countList = new HashMap<String, Integer>();
        Map<String, Float> avgPriceList = new HashMap<String, Float>();
        // String countList = new String(); 
        // String avgPriceList = new String(); 
        @Override
        public String toString() {
            // TODO: format output
            return String.format("Stats {totalTradeNum:%d\n, countList:%s\n, averageOrder:%s}",
              totalTradeNum, mapToString1(countList), mapToString2(avgPriceList));
        }
        public String mapToString1(Map<String, Integer> map) {
            StringBuilder stringBuilder = new StringBuilder();
            for (String key : map.keySet()) {
              if (stringBuilder.length() > 0) {
                  stringBuilder.append(",");
              }
              String value = String.valueOf(map.get(key));
              stringBuilder.append(key);
              stringBuilder.append(":");
              stringBuilder.append(value);
            }
            return stringBuilder.toString();
        }
        public String mapToString2(Map<String, Float> map) {
            StringBuilder stringBuilder = new StringBuilder();
            for (String key : map.keySet()) {
              if (stringBuilder.length() > 0) {
                  stringBuilder.append(",");
              }
              String value = String.valueOf(map.get(key));
              stringBuilder.append(key);
              stringBuilder.append(":");
              stringBuilder.append(value);
            }
            return stringBuilder.toString();
        }
    }

    /**
     * aggregate statistics
     * 1. group by key
     * 2. count transaction order
     * 3. sum transaction number
     * 4. compute average price
     * 5. get minimum price\trade number
     */
    private class stockStatsAggregator implements FoldLeftFunction<List<Order>, stockStats> {

        private String key = new String();
        
        stockStatsAggregator(String groupByKey) {
            key = groupByKey;
        }
        
        @Override
        public void init(Config config, TaskContext context) {
            // TODO: analyse if need these factor
            // store = (KeyValueStore<String, Integer>) context.getStore(STATS_STORE_NAME);
            // repeatEdits = context.getMetricsRegistry().newCounter("edit-counters", "repeat-edits");
        }

        @Override
        public stockStats apply(List<Order> completeOrder, stockStats stats) {
            // TODO: group method most to traverse the whole completeOrder
            // Map<String, List<Order>> groupRes = groupBy(completeOrder, this.key, stats);
            // TODO: according to group result, do statistics
            // stats.totalTradeNum += tradeNum(groupRes);
            // stats.countList = count(groupRes);
            // stats.avgPriceList = averagePrice(groupRes);
            // stats.minimum = minimum(groupRes);
            String orderKey = new String();
            int count = 0;
            //int tradeNum = 0;
            float averagePrice = 0;
            for (int i=0; i < completeOrder.size(); i++) {
                if ((orderKey = completeOrder.get(i).getKey(key)) == null) {
                    continue;
                }
                // order count by key
                if (stats.countList.get(orderKey) != null) {
                    count = stats.countList.get(orderKey);
                } else {
                    count = 0;
                }
                count++;
                stats.countList.put(orderKey, count);
                // tradeNum aggregate
                if (completeOrder.get(i).getOrderVol() == 0) {
                    stats.totalTradeNum += completeOrder.get(i).getOrderExecVol();
                }
                // average order price
                if (stats.avgPriceList.get(orderKey) != null) {
                    averagePrice = stats.avgPriceList.get(orderKey);
                } else {
                    averagePrice = 0;
                }
                averagePrice += completeOrder.get(i).getOrderPrice();
                stats.avgPriceList.put(orderKey, averagePrice);
            }
            return stats;
        }
        /**
         * do count\average\minimum\etc in this area,and update stats
         */
        // public boolean groupBy(List<Order> completeOrder, String key, stockStats stats){
        //     int count = 0;
        //     int tradeNum = 0;
        //     float averagePrice = 0;
        //     for (int i=0; i < completeOrder.size(); i++) {
        //         if ((orderKey = completeOrder.get(i).getKey(key)) == null) {
        //             continue;
        //         }
        //         // order count by key
        //         if (stats.countList.get(orderKey) != null) {
        //             count = stats.countList.get(orderKey);
        //         } else {
        //             count = 0;
        //         }
        //         count++;
        //         stats.countList.put(orderKey, count);
        //         // tradeNum aggregate
        //         if (completeOrder.get(i).getOrderVol() == 0) {
        //             stats.tradeNum += completeOrder.get(i).getOrderExecVol();
        //         }
        //         // average order price
        //         if (stats.avgPriceList.get(orderKey) != null) {
        //             averagePrice = stats.avgPriceList.get(orderKey);
        //         } else {
        //             averagePrice = 0;
        //         }
        //         averagePrice += completeOrder.get(i).getOrderPrice();
        //         stats.avgPriceList.put(orderKey, averagePrice);
        //     }
        // }

        // public Map<String, List<Order>> groupBy(List<Order> completeOrder, String key, stockStats stats){
        //     Map<String, List<Order>> groupMap = new HashMap<String, List<Order>>();
        //     String orderKey = new String();
        //     List<Order> orderList = new ArrayList<>();
        //     //  construct gourpMap
        //     for (int i=0; i < completeOrder.size(); i++) {
        //         if ((orderKey = completeOrder.get(i).getKey(key)) == null) {
        //             continue;
        //         }
        //         if (groupMap.get(orderKey) != null) {
        //             orderList = groupMap.get(orderKey);
        //         } else {
        //             orderList = new ArrayList<>();
        //         }
        //         orderList.add(completeOrder.get(i));
        //         groupMap.put(orderKey, orderList);
        //     }
        //     return groupMap;
        // }

        // public String count(Map<String, List<Order>> groupRes){
        //     // Map<String, Integer> countList = new HashMap<String, Integer>();
        //     StringBuilder messageBuilder = new StringBuilder();
        //     for (Map.Entry<String, List<Order>> entry : groupRes.entrySet()) {
        //         // countList.put(entry.getKey(), entry.getValue().size());
        //         messageBuilder.append(entry.getKey()).append(":").append(String.valueOf(entry.getValue().size())).append(";");
        //     }
        //     return messageBuilder.toString();
        // }

        // public Integer tradeNum(Map<String, List<Order>> groupRes){
        //     int orderNum = 0;
        //     for (Map.Entry<String, List<Order>> entry : groupRes.entrySet()) {
        //         for (int i=0; i < entry.getValue().size(); i++) {
        //             if (entry.getValue().get(i).getOrderVol() != 0) {
        //                 continue;
        //             }
        //             orderNum += entry.getValue().get(i).getOrderExecVol();
        //         }
        //     }
        //     return orderNum;
        // }

        // public float minimum(Map<String, List<Order>> groupRes){
        //     float min = new Float(10000);
        //     for (Map.Entry<String, List<Order>> entry : groupRes.entrySet()) {
        //         for (int i=0; i < entry.getValue().size(); i++) {
        //             if (entry.getValue().get(i).getOrderPrice() < min) {
        //                 min = entry.getValue().get(i).getOrderPrice();
        //             }
        //         }
        //     }
        //     return min;
        // }

        // TODO: orderVol logic is not good enough to represent transaction volume
        // public String averagePrice(Map<String, List<Order>> groupRes){
        //     // Map<String, Double> avgPriceList = new HashMap<String, Double>();
        //     StringBuilder messageBuilder = new StringBuilder();
        //     double totalOrderPrice = 0;
        //     int totalOrderVol = 0;
        //     for (Map.Entry<String, List<Order>> entry : groupRes.entrySet()) {
        //         totalOrderPrice = 0;
        //         totalOrderVol = 0;
        //         for (int i=0; i < entry.getValue().size(); i++) {
        //             if (entry.getValue().get(i).getOrderVol() != 0) {
        //                 continue;
        //             }
        //             totalOrderVol += entry.getValue().get(i).getOrderExecVol();
        //             totalOrderPrice += entry.getValue().get(i).getOrderPrice() * entry.getValue().get(i).getOrderExecVol();
        //         }
        //         // avgPriceList.put(entry.getKey() , totalOrderPrice/totalOrderVol);
        //         messageBuilder.append(entry.getKey()).append(":").append(String.valueOf(totalOrderPrice/totalOrderVol)).append(";");
        //     }
        //     return messageBuilder.toString();
        // }
    }

    /**
     * Format the stats for output to Kafka.
     */
    private String formatOutput(WindowPane<Void, stockStats> statsWindowPane) {

        stockStats stats = statsWindowPane.getMessage();
        return stats.toString();
        // TODO: construct stats
    }
}

