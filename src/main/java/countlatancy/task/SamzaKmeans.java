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
import org.apache.samza.task.TaskContext;
import org.apache.samza.storage.kv.KeyValueStore;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.time.Duration;
import java.util.Collection;
import java.util.function.Function;
import java.util.*;
import java.io.*;

// test
public class SamzaKmeans implements StreamApplication {

  // private static final Logger LOG = LoggerFactory.getLogger(TumblingPageViewCounterApp.class);
  private static final String INPUT_TOPIC = "KMeans";
  private static final String OUTPUT_TOPIC = "SamzaKmeans";
  private static final int dimension = 2;
  private static final int centroidsNumber = 96;

private List<Point> loadInitCentroids() {
    InputStream stream = null;
    BufferedReader br = null;
    List<Point> centroids = new ArrayList<>();
    try{
      String sCurrentLine;
      stream = this.getClass().getClassLoader().getResourceAsStream("init-centroids.txt");

      br = new BufferedReader(new InputStreamReader(stream));
      while ((sCurrentLine = br.readLine()) != null) {
          String[] strs = sCurrentLine.split(",");
          double[] position = new double[dimension];
          for (int i = 0; i < dimension; i++) {
              position[i] = Double.valueOf(strs[i]);
          }
          centroids.add(new Point(position));
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
    return centroids;
}

  @Override
  public void init(StreamGraph graph, Config config) {

    MessageStream<String> tuples = graph.<String, String, String>getInputStream(INPUT_TOPIC, (k, v) -> v);

    OutputStream<Void, String, String> outputStream = graph
        .getOutputStream(OUTPUT_TOPIC, m -> null, m -> m);

    Function<String, String> keyFn = pageView -> pageView;

    tuples
        .map((tuple) -> {
            List<Point> centroids = this.loadInitCentroids();
            String[] list = tuple.split("\\|");
            String[] strs = list[0].split("\\t");
            double[] position = new double[dimension];
            for (int i = 0; i < dimension; i++) {
                position[i] = Double.valueOf(strs[i]);
            }
            Point testData = new Point(position);
            int minIndex = -1;
            double minDistance = Double.MAX_VALUE;
            for (int i = 0; i < centroids.size(); i++) {
                double distance = testData.euclideanDistance(centroids.get(i));
                if (distance < minDistance) {
                    minDistance = distance;
                    minIndex = i;
                }
            }
            return new Point(minIndex, testData.location);
        })
        .window(Windows.tumblingWindow(Duration.ofSeconds(3), Centroid::new, new centroidAggregator()))
        .map(this::formatOutput)
        .sendTo(outputStream);
  }

   /**
   * A few statistics about the incoming messages.
   */
  private static class Centroid {
    Map<Integer, Integer> counts = new HashMap<Integer, Integer>();
    Map<Integer, Point> list = new HashMap<Integer, Point>();

    @Override
    public String toString() {
      return list.toString();
    }
  }

  private class centroidAggregator implements FoldLeftFunction<Point, Centroid> {

    // private KeyValueStore<String, Integer> store;

    // Example metric. Running counter of the number of repeat edits of the same title within a single window.
    // private Counter repeatEdits;

    /**
     * {@inheritDoc}
     * Override {@link org.apache.samza.operators.functions.InitableFunction#init(Config, TaskContext)} to
     * get a KeyValueStore for persistence and the MetricsRegistry for metrics.
     */
    @Override
    public void init(Config config, TaskContext context) {
      // store = (KeyValueStore<String, Integer>) context.getStore(STATS_STORE_NAME);
      // repeatEdits = context.getMetricsRegistry().newCounter("edit-counters", "repeat-edits");
    }

    public Centroid apply(Point point, Centroid centroid) {
      Integer count = centroid.counts.get(point.minIndex);
      Point storedPoint = centroid.list.get(point.minIndex);
      if (count == null) {
          storedPoint = new Point();
          count = 0;
      }
      double[] location = new double[2];
      for (int i = 0; i < location.length; i++) {
          location[i] = storedPoint.location[i] + point.location[i];
      }
      count++;
      point.location = location;
      centroid.list.put(point.minIndex, point);
      centroid.counts.put(point.minIndex, count);
      
      return centroid;
    }
  }

  /**
   * Format the stats for output to Kafka.
   */
  private String formatOutput(WindowPane<Void, Centroid> centroidWindowPane) {

    Centroid centroids = centroidWindowPane.getMessage();

    //Map<Integer, String> outputPoints = new HashMap<Integer, String>();
    String outputPoints = new String();

    Integer count = 0;
    Point point = new Point();
    for (Map.Entry<Integer, Point> entry : centroids.list.entrySet()) {  
      // System.out.println("Key = " + entry.getKey() + ", Value = " + entry.getValue());
      count = centroids.counts.get(entry.getKey());
      point = entry.getValue();
      point.location[0] /= count;
      point.location[1] /= count;

      outputPoints += point.minIndex +" "+ "("+point.location[0]+", "+point.location[1]+")\n";
    }
    System.out.println(outputPoints);
    return outputPoints;
  }
}

