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

  private String orderNo;
  private String tranMaintCode;
  private String orderPrice;
  private String orderExecVol;
  private String orderVol;
  private String secCode;
  private String tradeDir;

  Order(String tuple) {
    String[] orderList = tuple.split("\\|");
    orderNo = orderList[Order_No];
    tranMaintCode = orderList[Tran_Maint_Code];
    orderPrice = orderList[Order_Price];
    orderExecVol = orderList[Order_Exec_Vol];
    orderVol = orderList[Order_Vol];
    secCode = orderList[Sec_Code];
    tradeDir = orderList[Trade_Dir];
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
