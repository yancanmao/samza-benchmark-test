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
        String stockId = config.get("stockId", "600300");

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
          .filter((tradeResult) -> !tradeResult.isEmpty())
          .filter((tradeResult) -> tradeResult.get(0).equals(stockId))
          .window(Windows.tumblingWindow(Duration.ofSeconds(Long.parseLong(windowInterval)), stockStats::new, new stockStatsAggregator(groupByKey)))
          .map(this::formatOutput)
          .sendTo(outputStream);
    }
    
    /**
     * deal continous transaction
     * @param poolB,poolS,pool,order
     * @return output string 
     */
    public List<String> transaction(Map<Float, List<Order>> poolB, Map<Float, List<Order>> poolS,
                              List<Float> poolPriceB, List<Float> poolPriceS, Map<String, List<Float>> poolPrice,
                              Map<String, Map<Float, List<Order>>> pool, Order order) {
        // hava a transaction
        int top = 0;
        int i = 0;
        int j = 0;
        int otherOrderVol;
        int totalVol = 0;
        float tradePrice = 0;
        List<String> tradeResult = new ArrayList<>();
        while (poolPriceS.get(top) <= poolPriceB.get(top)) {
            tradePrice = poolPriceS.get(top);
            if (poolB.get(poolPriceB.get(top)).get(top).getOrderVol() > poolS.get(poolPriceS.get(top)).get(top).getOrderVol()) {
                // B remains B_top-S_top
                otherOrderVol = poolS.get(poolPriceS.get(top)).get(top).getOrderVol();
                // totalVol sum
                totalVol += otherOrderVol;
                poolB.get(poolPriceB.get(top)).get(top).updateOrder(otherOrderVol);
                // S complete
                poolS.get(poolPriceS.get(top)).get(top).updateOrder(otherOrderVol);
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
                totalVol += otherOrderVol;
                poolB.get(poolPriceB.get(top)).get(top).updateOrder(otherOrderVol);
                poolS.get(poolPriceS.get(top)).get(top).updateOrder(otherOrderVol);
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
        pool.put(order.getSecCode()+"S", poolS);
        pool.put(order.getSecCode()+"B", poolB);
        poolPrice.put(order.getSecCode()+"B", poolPriceB);
        poolPrice.put(order.getSecCode()+"S", poolPriceS);
        tradeResult.add(order.getSecCode());
        tradeResult.add(String.valueOf(totalVol));
        tradeResult.add(String.valueOf(tradePrice));
        // return tradeResult;
        return tradeResult;
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
    public List<String> mapFunction(Map<String, Map<Float, List<Order>>> pool, Map<String, List<Float>> poolPrice, Order order) {
        // String complete = new String();
        List<String> tradeResult = new ArrayList<>();
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
                    return tradeResult;
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
                        return tradeResult;
                    }
                }
                // else output no delete order exist
                return tradeResult;             
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
                // return complete;
                return tradeResult;             
            }

            // no satisfied price
            if (poolPriceS.get(0) > poolPriceB.get(0)) {
                // this.savepool();
                pool.put(order.getSecCode()+"S", poolS);
                pool.put(order.getSecCode()+"B", poolB);
                poolPrice.put(order.getSecCode()+"S", poolPriceS);
                poolPrice.put(order.getSecCode()+"B", poolPriceB);
                return tradeResult;
            } else {
                tradeResult = this.transaction(poolB, poolS, poolPriceB, poolPriceS, poolPrice, pool, order);
            }
        } else if (order.getTradeDir().equals("S")) {
            float orderPrice = order.getOrderPrice();
            List<Order> SorderList = poolS.get(orderPrice);
            // if order tran_maint_code is "D", delete from pool
            if (FILTER_KEY1.equals(order.getTranMaintCode())) {
                // if exist in order, remove from pool
                String orderNo = order.getOrderNo();
                if (SorderList == null) {
                    return tradeResult;
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
                        return tradeResult;
                    }
                }
                // else output no delete order exist
                return tradeResult;
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
                return tradeResult;
            }
            
            // no satisfied price
            if (poolPriceS.get(0) > poolPriceB.get(0)) {
                // this.savepool();
                pool.put(order.getSecCode()+"S", poolS);
                pool.put(order.getSecCode()+"B", poolB);
                poolPrice.put(order.getSecCode()+"S", poolPriceS);
                poolPrice.put(order.getSecCode()+"B", poolPriceB);
                return tradeResult;
            } else {
                tradeResult = this.transaction(poolB, poolS, poolPriceB, poolPriceS, poolPrice, pool, order);
            }
        }
        return tradeResult;
    }

    /**
     * A few statistics about the incoming messages.
     */
    private static class stockStats {
        String stockId = new String();
        int totalVol = 0;
        float tradePrice = 0;
        float totalPrice = 0;
        @Override
        public String toString() {
            // TODO: format output
            StringBuilder messageBuilder = new StringBuilder();
            messageBuilder.append("{\"stockId\":\"").append(stockId).append("\",")
                          .append("\"totalVol\":\"").append(String.valueOf(totalVol)).append("\",")
                          .append("\"tradePrice\":\"").append(String.valueOf(tradePrice)).append("\"}");
            return messageBuilder.toString();
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
    private class stockStatsAggregator implements FoldLeftFunction<List<String>, stockStats> {

        private String key = new String();
        
        stockStatsAggregator(String groupByKey) {
            key = groupByKey;
        }
        
        @Override
        public void init(Config config, TaskContext context) {
            // TODO: analyse if need these factor
        }

        @Override
        public stockStats apply(List<String> tradeResult, stockStats stats) {
            stats.stockId = tradeResult.get(0);
            stats.totalVol += Integer.parseInt(tradeResult.get(1));
            stats.totalPrice += Integer.parseInt(tradeResult.get(1))*Float.parseFloat(tradeResult.get(2));
            stats.tradePrice = stats.totalPrice/stats.totalVol;
            return stats;
        }
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

