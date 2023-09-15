/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements. See the NOTICE file distributed with this
 * work for additional information regarding copyright ownership. The ASF
 * licenses this file to You under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS, WITHOUT
 * WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied. See the
 * License for the specific language governing permissions and limitations
 * under the License.
 */

package org.apache.hugegraph.util;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Queue;
import java.util.concurrent.ArrayBlockingQueue;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.Callable;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Future;
import java.util.concurrent.ThreadPoolExecutor;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.function.Consumer;

import org.apache.hugegraph.HugeException;
import org.apache.hugegraph.config.CoreOptions;
import org.apache.hugegraph.task.TaskManager.ContextCallable;
import org.slf4j.Logger;

public final class Consumers<V> {

    public static final int THREADS = 4 + CoreOptions.CPUS / 4;
    public static final int QUEUE_WORKER_SIZE = 1000;
    public static final long CONSUMER_WAKE_PERIOD = 1;

    private static final Logger LOG = Log.logger(Consumers.class);

    private final ExecutorService executor;
    private final Consumer<V> consumer;
    private final Runnable done;
    private final Consumer<Throwable> exceptionHandle;

    private final int workers;
    private final List<Future> runnings;
    private final int queueSize;
    private final CountDownLatch latch;//Java 并发 && 锁类型对比
    private final BlockingQueue<VWrapper<V>> queue;

    private final VWrapper<V> queueEnd = new VWrapper(null);
    private volatile Throwable exception = null;

    public Consumers(ExecutorService executor, Consumer<V> consumer) {
        this(executor, consumer, null);
    }

    public Consumers(ExecutorService executor,
                     Consumer<V> consumer, Runnable done) {
        this(executor, consumer, done, QUEUE_WORKER_SIZE);
    }

    public Consumers(ExecutorService executor,
                     Consumer<V> consumer,
                     Runnable done,
                     Consumer<Throwable> handle,
                     int queueWorkerSize) {
        this.executor = executor;
        this.consumer = consumer;
        this.done = done;
        this.exceptionHandle = handle;

        int workers = THREADS;
        if (this.executor instanceof ThreadPoolExecutor) {
            workers = ((ThreadPoolExecutor) this.executor).getCorePoolSize();
        }
        this.workers = workers;
        this.runnings = new ArrayList<>(workers);
        this.queueSize = queueWorkerSize * workers + 1;
        this.latch = new CountDownLatch(workers);
        this.queue = new ArrayBlockingQueue<>(this.queueSize);
    }

    public Consumers(ExecutorService executor,
                     Consumer<V> consumer,
                     Runnable done,
                     int queueWorkerSize) {
        this(executor, consumer, done, null, queueWorkerSize);
    }

    /**
     * @param executor
     * @param totalThreads
     * @param callback
     * @throws InterruptedException 这个方法有一定概率会造成死锁，导致所有线程卡死
     */
    @Deprecated
    public static void executeOncePerThread(ExecutorService executor,
                                            int totalThreads,
                                            Runnable callback)
        throws InterruptedException {
        // Ensure callback execute at least once for every thread
        final Map<Thread, Integer> threadsTimes = new ConcurrentHashMap<>();
        final List<Callable<Void>> tasks = new ArrayList<>();
        final Callable<Void> task = () -> {
            Thread current = Thread.currentThread();
            threadsTimes.putIfAbsent(current, 0);
            int times = threadsTimes.get(current);
            if (times == 0) {
                callback.run();
                // Let other threads run
                Thread.yield();
            } else {
                assert times < totalThreads;
                assert threadsTimes.size() < totalThreads;
                E.checkState(tasks.size() == totalThreads,
                    "Bad tasks size: %s", tasks.size());
                // Let another thread run and wait for it
                executor.submit(tasks.get(0)).get();
            }
            threadsTimes.put(current, ++times);
            return null;
        };

        // NOTE: expect each task thread to perform a close operation
        for (int i = 0; i < totalThreads; i++) {
            tasks.add(task);
        }
        executor.invokeAll(tasks, 5, TimeUnit.SECONDS);
    }

    public static ExecutorService newThreadPool(String prefix, int workers) {
        if (workers == 0) {
            return null;
        } else {
            if (workers < 0) {
                assert workers == -1;
                workers = Consumers.THREADS;
            } else if (workers > CoreOptions.CPUS * 2) {
                workers = CoreOptions.CPUS * 2;
            }
            String name = prefix + "-worker-%d";
            return ExecutorUtil.newFixedThreadPool(workers, name);
        }
    }

    public static ExecutorPool newExecutorPool(String prefix, int workers) {
        return new ExecutorPool(prefix, workers);
    }

    public static RuntimeException wrapException(Throwable e) {
        if (e instanceof RuntimeException) {
            throw (RuntimeException) e;
        }
        throw new HugeException("Error when running task: %s",
                                HugeException.rootCause(e).getMessage(), e);
    }

    public void start(String name) {
        this.exception = null;
        if (this.executor == null) {
            return;
        }
        LOG.info("Starting {} workers[{}] with queue size {}...",
                 this.workers, name, this.queueSize);
        for (int i = 0; i < this.workers; i++) {
            this.runnings.add(this.executor.submit(new ContextCallable<>(this::runAndDone)));//多线程运行
        }
    }
    //TODO: 此段代码如何执行？
    private Void runAndDone() {
        try {
            this.run();
        } catch (Throwable e) {
            if (e instanceof StopExecution) {
                // clear data and wake up other threads
                this.queue.clear();
                putEnd();
            } else {
                // Only the first exception of one thread can be stored
                this.exception = e;
                LOG.error("Error when running task", e);
            }
            exceptionHandle(e);
        } finally {//此段代码在 this.run() 执行结束后会执行，主要是 CountDownLatch 进行计数，然后latch.await() 在侦听完成状态
            this.done();
            this.latch.countDown();
        }
        return null;
    }

    private void run() {//多线程执行
        LOG.debug("Start to work...");

        while (this.consume()) {
        }

        LOG.debug("Worker finished");
    }

    private boolean consume() {//多个线程在使用
        VWrapper<V> elem = null;
        while (elem == null) {
            try {
                elem = this.queue.poll(CONSUMER_WAKE_PERIOD, TimeUnit.SECONDS);
            } catch (InterruptedException e) {
                // ignore
                return false;
            }
        }

        if (elem == queueEnd) {
            putEnd();
            return false;
        }
        // do job
        // 此处看代码时，一开始就要看到传参的路径，多层继承后，不要找不到，这个方法最耗时
        this.consumer.accept(elem.v);//消费队列中的数据
        return true;
    }

    private void exceptionHandle(Throwable e) {
        if (this.exceptionHandle == null) {
            return;
        }

        try {
            this.exceptionHandle.accept(e);
        } catch (Throwable ex) {
            if (this.exception == null) {
                this.exception = ex;
            } else {
                LOG.warn("Error while calling exceptionHandle()", ex);
            }
        }
    }

    private void done() {
        if (this.done == null) {
            return;
        }

        try {
            this.done.run();
        } catch (Throwable e) {
            if (this.exception == null) {
                this.exception = e;
            } else {
                LOG.warn("Error while calling done()", e);
            }
        }
    }

    private Throwable throwException() {
        assert this.exception != null;
        Throwable e = this.exception;
        this.exception = null;
        return e;
    }

    public void provide(V v) throws Throwable {
        if (this.executor == null) {
            assert this.exception == null;
            // do job directly if without thread pool
            this.consumer.accept(v);//此处应该是传递的迭代器，如何反序列化
        } else if (this.exception != null) {
            throw this.throwException();
        } else {
            try {
                this.queue.put(new VWrapper(v));
            } catch (InterruptedException e) {
                LOG.warn("Interrupted while enqueue", e);
            }
        }
    }

    private void putEnd() {
        if (this.executor != null) {
            try {
                this.queue.put(queueEnd);
            } catch (InterruptedException e) {
                LOG.warn("Interrupted while enqueue", e);
            }
        }
    }

    public void await() throws Throwable {
        if (this.executor == null) {
            // call done() directly if without thread pool
            this.done();
        } else {
            try {
                putEnd();//在queue 中提交一个结束标志
                this.latch.await();//通知所有线程执行完毕，
            } catch (InterruptedException e) {
                String error = "Interrupted while waiting for consumers";
                for (Future f : this.runnings) {
                    f.cancel(true);
                }
                this.exception = new HugeException(error, e);
                LOG.warn(error, e);
            }
        }

        if (this.exception != null) {
            throw this.throwException();
        }
    }

    public ExecutorService executor() {
        return this.executor;
    }

    public static class ExecutorPool {

        private final static int POOL_CAPACITY = 2 * CoreOptions.CPUS;

        private final String threadNamePrefix;
        private final int executorWorkers;
        private final AtomicInteger count;

        private final Queue<ExecutorService> executors;

        public ExecutorPool(String prefix, int workers) {
            this.threadNamePrefix = prefix;
            this.executorWorkers = workers;
            this.count = new AtomicInteger();
            this.executors = new ArrayBlockingQueue<>(POOL_CAPACITY);
        }

        public synchronized ExecutorService getExecutor() {
            ExecutorService executor = this.executors.poll();
            if (executor == null) {
                int count = this.count.incrementAndGet();
                String prefix = this.threadNamePrefix + "-" + count;
                executor = newThreadPool(prefix, this.executorWorkers);
            }
            return executor;
        }

        public synchronized void returnExecutor(ExecutorService executor) {
            if (executor != null) {
                if (!this.executors.offer(executor)) {
                    try {
                        executor.shutdownNow();
                    } catch (Exception e) {
                        LOG.warn("close ExecutorService with error:", e);
                    }
                }
            }
        }

        public synchronized void destroy() {
            for (ExecutorService executor : this.executors) {
                try {
                    executor.shutdownNow();
                } catch (Exception e) {
                    LOG.warn("close ExecutorService with error:", e);
                }
            }
            this.executors.clear();
        }
    }

    public static class StopExecution extends HugeException {

        private static final long serialVersionUID = -371829356182454517L;

        public StopExecution(String message) {
            super(message);
        }

        public StopExecution(String message, Object... args) {
            super(message, args);
        }
    }

    public static class VWrapper<V> {
        public V v;

        public VWrapper(V v) {
            this.v = v;
        }
    }
}
