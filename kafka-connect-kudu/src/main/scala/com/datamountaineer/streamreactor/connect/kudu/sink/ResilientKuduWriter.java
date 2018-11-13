package com.datamountaineer.streamreactor.connect.kudu.sink;

import com.datamountaineer.kcql.Kcql;
import org.apache.kafka.connect.data.Schema.Type;
import org.apache.kafka.connect.sink.SinkRecord;
import org.apache.kudu.client.KuduClient;
import org.apache.kudu.client.KuduException;
import org.apache.kudu.client.KuduSession;
import org.apache.kudu.client.Upsert;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ArrayBlockingQueue;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;

public class ResilientKuduWriter {

    private static int QUEUE_SIZE = 10 * 1024;
    private static int MAX_CONCURRENT = 16;

    private ExecutorService POOL = Executors.newFixedThreadPool(MAX_CONCURRENT + 1);
    private BlockingQueue<Upsert> upserts = new ArrayBlockingQueue<>(QUEUE_SIZE);
    private volatile boolean running = true;
    private State currentState = State.SINGLE_THREAD;
    private KuduClient kuduClient;
    private Kcql kcql;
    private Map<String, Type> primaryKeyTypes;

    private KuduSession singleThreadSession;
    private List<BlockingQueue<Upsert>> buckets = new ArrayList<>();

    private Runnable consumerThread = () -> {
        while (running || upserts.size() > 0) {
            try {
                Upsert upsert = upserts.poll(1, TimeUnit.SECONDS);
                if (upsert == null) {
                    continue;
                }

                int currentQueueSize = upserts.size();

                switch (currentState) {
                    case SINGLE_THREAD:
                        if (currentQueueSize > State.MULTI_THREAD.threshold) {
                            transitionToMultiThreadState();
                            appendToMultiThread(upsert);
                        } else {
                            appendToSingleThread(upsert);
                        }
                        break;
                    case MULTI_THREAD:
                        if (currentQueueSize < State.SINGLE_THREAD.threshold) {
                            transitionToSingleThreadState();
                            appendToSingleThread(upsert);
                        } else {
                            appendToMultiThread(upsert);
                        }
                        break;
                    default:
                        System.out.println("will never get here");
                        break;
                }
            } catch (InterruptedException | KuduException e) {
                e.printStackTrace();
            }
        }
    };

    public ResilientKuduWriter(KuduClient kuduClient, Kcql kcql) {
        this.kuduClient = kuduClient;
        this.singleThreadSession = kuduClient.newSession();
        this.kcql = kcql;

        POOL.submit(consumerThread);
    }

    public void write(Upsert upsert, SinkRecord sinkRecord) {
        try {
            upserts.put(upsert);
            updatePrimaryTypeIfNecessary(sinkRecord);
        } catch (InterruptedException e) {
            e.printStackTrace();
        }
    }

    public void close() {
        this.running = false;
        POOL.shutdown();
        try {
            POOL.awaitTermination(10, TimeUnit.SECONDS);
        } catch (InterruptedException e) {
            e.printStackTrace();
        }
    }

    private void updatePrimaryTypeIfNecessary(SinkRecord sinkRecord) {
        if (primaryKeyTypes == null) {
            synchronized (this) {
                if (primaryKeyTypes == null) {
                    this.primaryKeyTypes = new HashMap<>();
                    this.kcql.getBucketing().getBucketNames().forEachRemaining(field -> primaryKeyTypes.put(field, null));
                    sinkRecord.valueSchema().fields().forEach(field -> {
                        if (primaryKeyTypes.containsKey(field.name())) {
                            primaryKeyTypes.put(field.name(), field.schema().type());
                        }
                    });
                }
            }
        }
    }

    private void appendToSingleThread(Upsert upsert) throws KuduException {
        this.singleThreadSession.apply(upsert);
    }

    private void appendToMultiThread(Upsert upsert) throws InterruptedException {
        StringBuilder stringBuilder = new StringBuilder();
        for (String field : primaryKeyTypes.keySet()) {
            switch (primaryKeyTypes.get(field)) {
                case INT32:
                    stringBuilder.append(upsert.getRow().getInt(field));
                    break;
                case INT64:
                    stringBuilder.append(upsert.getRow().getLong(field));
                    break;
                case FLOAT32:
                    stringBuilder.append(upsert.getRow().getFloat(field));
                    break;
                case FLOAT64:
                    stringBuilder.append(upsert.getRow().getDouble(field));
                    break;
                case STRING:
                    stringBuilder.append(upsert.getRow().getString(field));
                    break;
                default:
                    // expect no primary key type
                    System.out.println("got exception primary key type");
                    break;
            }
        }

        int bucket = stringBuilder.toString().hashCode() % MAX_CONCURRENT;
        // hashCode returns int, which can be negative.
        if (bucket < 0) {
            bucket += MAX_CONCURRENT;
        }
        this.buckets.get(bucket).put(upsert);
    }

    private void transitionToSingleThreadState() throws InterruptedException {
        System.out.println(System.currentTimeMillis() + " transition to single thread mode");
        // TODO (use more precise condition)
        for (int i = 0; i < MAX_CONCURRENT; ++i) {
            while (this.buckets.get(i).size() > 0) {
                Thread.sleep(100);
            }
        }

        this.currentState = State.SINGLE_THREAD;
    }

    private void transitionToMultiThreadState() {
        System.out.println(System.currentTimeMillis() + " transition to multi thread mode");
        if (this.buckets.size() == 0) {
            initBuckets();
        }

        this.currentState = State.MULTI_THREAD;

        for (int i = 0; i < MAX_CONCURRENT; ++i) {
            final int bucket = i;
            POOL.submit(() -> {
                KuduSession session = this.kuduClient.newSession();
                BlockingQueue<Upsert> queue = this.buckets.get(bucket);

                try {
                    // TODO (use manual mode to speed up)
                    while (this.currentState == State.MULTI_THREAD || queue.size() != 0) {
                        Upsert upsert = queue.poll(1, TimeUnit.SECONDS);
                        if (upsert != null) {
                            session.apply(upsert);
                        }
                    }
                    session.close();
                } catch (InterruptedException | KuduException e) {
                    e.printStackTrace();
                }
            });
        }
    }

    private void initBuckets() {
        for (int i = 0; i < MAX_CONCURRENT; ++i) {
            BlockingQueue<Upsert> queue = new ArrayBlockingQueue<>(100);
            this.buckets.add(queue);
        }
    }

    public static void main(String args[]) {
        System.out.println("hello resilient kudu writer");
    }

    private enum State {
        SINGLE_THREAD(100),
        MULTI_THREAD(10000);

        private int threshold;

        State(int threshold) {
            this.threshold = threshold;
        }
    }
}
