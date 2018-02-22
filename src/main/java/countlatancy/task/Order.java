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

  Order(String tuple) {
    orderList = tuple.split("\\|");

  }

  String getOrderNo() {
    return this.orderList[Order_No];
  }
  String getTranMaintCode() {
    return this.orderList[Tran_Maint_Code];
  }
  float getOrderPrice() {
    return Float.parseFloat(this.orderList[Order_Price]);
  }
  int getOrderExecVol() {
    Float orderExecVol = Float.parseFloat(this.orderList[Order_Exec_Vol]);
    return orderExecVol.intValue();
  }
  int getOrderVol() {
    Float orderVol = Float.parseFloat(this.orderList[Order_Vol]);
    return orderVol.intValue();
  }
  String getSecCode() {
    return this.orderList[Sec_Code];
  }
  String getTradeDir() {
    return this.orderList[Trade_Dir];
  }

  public boolean updateOrder(int otherOrderVol) {
    this.orderList[Order_Vol] = (this.getOrderVol() - otherOrderVol) + "";
    this.orderList[Order_Exec_Vol] = (this.getOrderVol() + otherOrderVol) + "";
    return true;
  }
}
