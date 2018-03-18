package countlatancy.task;  

/**
 * Author by Mao
 * kmeans data structure, and some operator
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
