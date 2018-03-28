package countlatancy.task;  

import java.util.Map;
import java.util.HashMap;;

/**
 * Author by Mao
 * kmeans data structure, and some operator
 */

class Order {
  /**
   * The user that viewed the page
   */
  public String[] orderList;
  private static final int Order_No = 0;
  private static final int Tran_Maint_Code = 1;
  private static final int Order_Price = 8;
  private static final int Order_Exec_Vol = 9;
  private static final int Order_Vol = 10;
  private static final int Sec_Code = 11;
  private static final int Trade_Dir = 22;

  private String orderNo = new String();
  private String tranMaintCode = new String();
  private String orderPrice = new String();
  private String orderExecVol = new String();
  private String orderVol = new String();
  private String secCode = new String();
  private String tradeDir = new String();
  // private Map<String, String> orderMap = new HashMap<String, String>();

  Order(String tuple) {
    //String[] orderList = tuple.split("\\|");
    orderNo = new String(tuple.split("\\|")[Order_No]);
    tranMaintCode = new String(tuple.split("\\|")[Tran_Maint_Code]);
    if (!tranMaintCode.equals("")) {
      orderPrice = new String(tuple.split("\\|")[Order_Price]);
      orderExecVol = new String(tuple.split("\\|")[Order_Exec_Vol]);
      orderVol = new String(tuple.split("\\|")[Order_Vol]);
      secCode = new String(tuple.split("\\|")[Sec_Code]);
      tradeDir = new String(tuple.split("\\|")[Trade_Dir]);
      //orderMap.put("Order_No", orderNo);
      //orderMap.put("Tran_Maint_Code", tranMaintCode);
      //orderMap.put("Order_Price", orderPrice);
      //orderMap.put("Order_Exec_Vol", orderExecVol);
      //orderMap.put("Order_Vol", orderVol);
      //orderMap.put("Sec_Code", secCode);
      //orderMap.put("Trade_Dir", tradeDir);
    }
  }

  String getOrderNo() {
    return orderNo;
  }
  String getTranMaintCode() {
    return tranMaintCode;
  }
  float getOrderPrice() {
    return Float.parseFloat(orderPrice);
  }
  int getOrderExecVol() {
    Float interOrderExecVol = Float.parseFloat(orderExecVol);
    return interOrderExecVol.intValue();
  }
  int getOrderVol() {
    Float interOrderVol = Float.parseFloat(orderVol);
    return interOrderVol.intValue();
  }
  String getSecCode() {
    return secCode;
  }
  String getTradeDir() {
    return tradeDir;
  }

  //String getKey(String key) {
    //return orderMap.get(key);
  //}

  String objToString() {
    StringBuilder messageBuilder = new StringBuilder();
    messageBuilder.append(orderNo).append("|");
    messageBuilder.append(tranMaintCode).append("|");
    messageBuilder.append(orderPrice).append("|");
    messageBuilder.append(orderExecVol).append("|");
    messageBuilder.append(orderVol).append("|");
    messageBuilder.append(secCode).append("|");
    messageBuilder.append(tradeDir);
    return messageBuilder.toString();
    // return String.join("|", this.orderList);
  }

  public boolean updateOrder(int otherOrderVol) {
    orderVol = (this.getOrderVol() - otherOrderVol) + "";
    orderExecVol = (this.getOrderExecVol() + otherOrderVol) + "";
    return true;
  }
}
