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
 */

class Point {
  /**
   * The user that viewed the page
   */
  public double[] location;
  /**
   * A trackingId for the page
   */
  public int minIndex;

  Point() {
    this.location = new double[2];
    this.minIndex = -1;
  }

  Point(double[] l) {
    this.location = l;
    this.minIndex = -1;
  }

  Point(int min, double[] l) {
    this.location = l;
    this.minIndex = min;
  }

  double[] getLocation() {
    return this.location;
  }

  int getMinIndex() {
    return this.minIndex;
  }

  public double euclideanDistance(Point other) {
      return Math.sqrt(distanceSquaredTo(other));
  }

  public double distanceSquaredTo(Point other) {
      double squareSum = 0;
      for (int i = 0; i < this.location.length; ++i) {
          squareSum += Math.pow(this.location[i] - other.location[i], 2);
      }
      return squareSum;
  }

}
