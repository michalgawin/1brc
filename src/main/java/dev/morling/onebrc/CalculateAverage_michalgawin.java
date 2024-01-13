/*
 *  Copyright 2023 The original authors
 *
 *  Licensed under the Apache License, Version 2.0 (the "License");
 *  you may not use this file except in compliance with the License.
 *  You may obtain a copy of the License at
 *
 *      http://www.apache.org/licenses/LICENSE-2.0
 *
 *  Unless required by applicable law or agreed to in writing, software
 *  distributed under the License is distributed on an "AS IS" BASIS,
 *  WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 *  See the License for the specific language governing permissions and
 *  limitations under the License.
 */
package dev.morling.onebrc;

import java.io.FileInputStream;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.channels.FileChannel;
import java.text.DecimalFormat;
import java.util.Arrays;
import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ForkJoinPool;
import java.util.concurrent.ForkJoinTask;
import java.util.concurrent.RecursiveTask;
import java.util.stream.Collectors;

public class CalculateAverage_michalgawin {

    private static final String FILE = "./measurements.txt";
    public static final int FILE_BUFFER_SIZE = Integer.MAX_VALUE;
    public static final int THREADS = Runtime.getRuntime().availableProcessors() + 1;
    public static final KeyCache KEY_CACHE = new KeyCache();

    public static void main(String[] args) throws IOException {
        CalculateAverage_michalgawin.readFile();
    }

    private static void readFile() throws IOException {
        Map<String, StationMetrics> map = new HashMap<>(1024);
        int bufferSize = FILE_BUFFER_SIZE / THREADS;

        try (FileInputStream fileInputStream = new FileInputStream(CalculateAverage_michalgawin.FILE);
                FileChannel channel = fileInputStream.getChannel()) {
            ByteBuffer bb = ByteBuffer.allocateDirect(FILE_BUFFER_SIZE);
            while (channel.read(bb) > 0) {
                bb.flip();

                int lastLfIdx = ByteBufferUtil.findLastLF(bb);
                if (lastLfIdx >= 0) {
                    channel.position(channel.position() - (bb.limit() - lastLfIdx) + 1); // position after last LF

                    Map<ByteBuffer, StationMetrics> results = new ChunkReader(bb.slice(0, lastLfIdx + 1), bufferSize)
                            .execute();

                    for (Map.Entry<ByteBuffer, StationMetrics> entry : results.entrySet()) {
                        map.merge(KEY_CACHE.get(entry.getKey()), entry.getValue(), StationMetrics::merge);
                    }
                }

                bb.clear();
            }
        }

        System.out.println(Stringify.toString(map));
    }

    public static class ChunkReader extends RecursiveTask<Map<ByteBuffer, StationMetrics>> {

        static ForkJoinPool FORK_JOIN_POOL = ForkJoinPool.commonPool();
        private static final TempCache TEMP_CACHE = new TempCache();

        private final ByteBuffer bb;
        private final int bufferSize;

        public ChunkReader(ByteBuffer bb, int bufferSize) {
            this.bb = bb;
            this.bufferSize = bufferSize;
        }

        public Map<ByteBuffer, StationMetrics> execute() {
            FORK_JOIN_POOL.execute(this);
            return this.join();
        }

        @Override
        protected Map<ByteBuffer, StationMetrics> compute() {
            if (bb.limit() < bufferSize) {
                return process();
            }

            Map<ByteBuffer, StationMetrics> map = new HashMap<>(1024);

            int midLF = ByteBufferUtil.findMidLF(bb) + 1;
            ChunkReader t1 = new ChunkReader(bb.slice(0, midLF), bufferSize);
            ChunkReader t2 = new ChunkReader(bb.slice(midLF, bb.limit() - midLF), bufferSize);

            ForkJoinTask.invokeAll(t1, t2);

            Map<ByteBuffer, StationMetrics> t1Results = t1.join();
            for (Map.Entry<ByteBuffer, StationMetrics> entry : t1Results.entrySet()) {
                map.merge(entry.getKey(), entry.getValue(), StationMetrics::merge);
            }
            Map<ByteBuffer, StationMetrics> t2Results = t2.join();
            for (Map.Entry<ByteBuffer, StationMetrics> entry : t2Results.entrySet()) {
                map.merge(entry.getKey(), entry.getValue(), StationMetrics::merge);
            }

            return map;
        }

        private Map<ByteBuffer, StationMetrics> process() {
            Map<ByteBuffer, StationMetrics> map = new HashMap<>(512);

            for (int i = 0, j = i; i < bb.limit();) {
                if (bb.get(i) == '\n') {
                    ByteBuffer slice = bb.slice(j, i - j + 1);
                    Map.Entry<ByteBuffer, ByteBuffer> entry = getEntry(slice);
                    float temp = TEMP_CACHE.get(entry.getValue());
                    map.computeIfAbsent(entry.getKey(), bb -> new StationMetrics())
                            .add(temp);
                    j = i + 1; // first byte of new line
                    i += 6; // minimal number of bytes to next new line
                }
                else {
                    i++;
                }
            }

            return map;
        }

        /**
         * @param bb one line with measurement which ends by LF
         * @return temperature (value) measured in the city (key)
         */
        private Map.Entry<ByteBuffer, ByteBuffer> getEntry(ByteBuffer bb) {
            int limit = bb.limit();
            // [a-zA-Z ..., ; [0-9] 0-9 . 0-9 \n]
            for (int i = limit - 5; i > 0; i--) {
                if (bb.get(i) == ';') {
                    ByteBuffer key = bb.slice(0, i);
                    ++i;
                    ByteBuffer value = bb.slice(i, limit - i);
                    return Map.entry(key, value);
                }
            }
            throw new UnsupportedOperationException(
                    String.format("Invalid entry: %s", ByteBufferUtil.toString(bb)));
        }
    }

    public static class StationMetrics {

        private static final DecimalFormat DF = new DecimalFormat("0.0");
        private float min = Float.MAX_VALUE;
        private float max = Float.MIN_VALUE;
        private float[] metrics = new float[8196];
        private int index = 0;

        public float getMin() {
            return min;
        }

        public float getMax() {
            return max;
        }

        public double getMean() {
            float sum = 0.0f;
            for (int i = 0; i < index; i++) {
                sum += metrics[i];
            }
            return sum / index;
        }

        public StationMetrics add(float value) {
            if (value < min) {
                min = value;
            }
            if (value > max) {
                max = value;
            }
            if (index >= this.metrics.length) {
                metrics = Arrays.copyOf(metrics, metrics.length * 2);
            }
            metrics[index] = value;
            index++;

            return this;
        }

        public StationMetrics merge(StationMetrics stationMetrics) {
            if (stationMetrics.min < min) {
                this.min = stationMetrics.min;
            }
            if (stationMetrics.max > max) {
                this.max = stationMetrics.max;
            }
            if (index + stationMetrics.index >= metrics.length) {
                metrics = Arrays.copyOf(metrics, metrics.length + stationMetrics.metrics.length);
                System.arraycopy(stationMetrics.metrics, 0, metrics, index, stationMetrics.index);
            }

            return this;
        }

        @Override
        public String toString() {
            return getMin() + "/" + DF.format(getMean()) + "/" + getMax();
        }
    }

    public static class ByteBufferUtil {
        private static final char LF = '\n';

        public static int findLastLF(ByteBuffer bb) {
            for (int i = bb.limit() - 1; i >= 0; i--) {
                if (bb.get(i) == LF) {
                    return i;
                }
            }
            return -1;
        }

        public static int findMidLF(ByteBuffer bb) {
            for (int i = bb.limit() / 2; i > 0; i--) {
                if (bb.get(i) == LF) {
                    return i;
                }
            }
            return -1;
        }

        public static String toString(ByteBuffer bb) {
            byte[] data = new byte[bb.limit()];
            bb.get(data);
            return new String(data);
        }

        public static float toFloat(ByteBuffer bb) {
            return Float.parseFloat(toString(bb));
        }
    }

    public static class Stringify {

        public static String toString(Map<String, StationMetrics> map) {
            return map.entrySet().stream()
                    .sorted(Map.Entry.comparingByKey())
                    .map(Stringify::toString)
                    .collect(Collectors.joining(", ", "{", "}"));
        }

        private static String toString(Map.Entry<String, StationMetrics> entry) {
            return entry.getKey() + "=" + entry.getValue().toString();
        }
    }

    public static class TempCache {
        private final Map<Integer, Float> cache = new ConcurrentHashMap<>(8196);

        public float get(ByteBuffer bbTemp) {
            return cache.computeIfAbsent(bbTemp.hashCode(), k -> ByteBufferUtil.toFloat(bbTemp));
        }
    }

    public static class KeyCache {
        private final Map<Integer, String> cache = new HashMap<>(512);

        public String get(ByteBuffer bbTemp) {
            return cache.computeIfAbsent(bbTemp.hashCode(), (k) -> ByteBufferUtil.toString(bbTemp));
        }
    }
}
