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

package org.apache.hugegraph.traversal.algorithm;

import static org.apache.hugegraph.traversal.algorithm.HugeTraverser.NO_LIMIT;

import java.util.ArrayList;
import java.util.Collection;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.function.BiConsumer;
import java.util.function.Consumer;

import org.apache.hugegraph.HugeGraph;
import org.apache.hugegraph.backend.id.EdgeId;
import org.apache.hugegraph.backend.id.Id;
import org.apache.hugegraph.backend.query.Query;
import org.apache.hugegraph.config.CoreOptions;
import org.apache.hugegraph.iterator.CIter;
import org.apache.hugegraph.traversal.algorithm.steps.EdgeStep;
import org.apache.hugegraph.traversal.algorithm.strategy.TraverseStrategy;
import org.apache.hugegraph.util.Consumers;
import org.apache.hugegraph.util.Log;
import org.apache.tinkerpop.gremlin.structure.util.CloseableIterator;
import org.slf4j.Logger;

public abstract class PathTraverser {

    public static final Logger LOG = Log.logger(PathTraverser.class);
    private static final String EXECUTOR_NAME = "path";
    private static Consumers.ExecutorPool executors;
    protected final HugeTraverser traverser;
    protected final long capacity;
    protected final long limit;
    protected int stepCount;
    protected int totalSteps; // TODO: delete or implement abstract method

    protected Map<Id, List<HugeTraverser.Node>> sources;
    protected Map<Id, List<HugeTraverser.Node>> sourcesAll;
    protected Map<Id, List<HugeTraverser.Node>> targets;
    protected Map<Id, List<HugeTraverser.Node>> targetsAll;

    protected Map<Id, List<HugeTraverser.Node>> newVertices;

    protected Set<HugeTraverser.Path> paths;

    protected TraverseStrategy traverseStrategy;

    public PathTraverser(HugeTraverser traverser, TraverseStrategy strategy,
                         Collection<Id> sources, Collection<Id> targets,
                         long capacity, long limit) {
        this.traverser = traverser;
        this.traverseStrategy = strategy;

        this.capacity = capacity;
        this.limit = limit;

        this.stepCount = 0;

        this.sources = this.newMultiValueMap();
        this.sourcesAll = this.newMultiValueMap();
        this.targets = this.newMultiValueMap();
        this.targetsAll = this.newMultiValueMap();

        for (Id id : sources) {
            this.addNode(this.sources, id, new HugeTraverser.Node(id));
        }
        for (Id id : targets) {
            this.addNode(this.targets, id, new HugeTraverser.Node(id));
        }
        this.sourcesAll.putAll(this.sources);
        this.targetsAll.putAll(this.targets);

        this.paths = this.newPathSet();

        this.initExecutor();
    }

    protected void initExecutor() {
        if (executors != null) {
            return;
        }
        synchronized (OltpTraverser.class) {
            if (executors != null) {
                return;
            }
            int workers = this.graph().option(CoreOptions.OLTP_CONCURRENT_THREADS);
            if (workers > 0) {
                executors = new Consumers.ExecutorPool(EXECUTOR_NAME, workers);
            }
        }
    }

    protected HugeGraph graph() {
        return this.traverser.graph();
    }

    public void forward() {
        EdgeStep currentStep = this.nextStep(true);
        if (currentStep == null) {
            return;
        }

        this.beforeTraverse(true);

        // Traversal vertices of previous level
        // this.traverseOneLayer(this.sources, currentStep, this::forward);
        // 批量计算
        this.traverseOneLayerBatch(this.sources, currentStep, this::forwardBatch);

        this.afterTraverse(currentStep, true);
    }

    public void backward() {
        EdgeStep currentStep = this.nextStep(false);
        if (currentStep == null) {
            return;
        }

        this.beforeTraverse(false);

        currentStep.swithDirection();
        // Traversal vertices of previous level
        // this.traverseOneLayer(this.targets, currentStep, this::backward);
        // 批量计算
        this.traverseOneLayerBatch(this.targets, currentStep, this::backwardBatch);
        currentStep.swithDirection();

        this.afterTraverse(currentStep, false);
    }

    public abstract EdgeStep nextStep(boolean forward);

    public void beforeTraverse(boolean forward) {
        this.clearNewVertices();
    }

    public void traverseOneLayerBatch(
        Map<Id, List<HugeTraverser.Node>> vertices,
        EdgeStep step,
        BiConsumer<Iterator<Id>, EdgeStep> consumer) {
        this.traverseStrategy.traverseOneLayerBatch(vertices, step, consumer);
    }

    public void afterTraverse(EdgeStep step, boolean forward) {
        this.reInitCurrentStepIfNeeded(step, forward);
        this.stepCount++;
    }

    private void processOne(Id source, Id target, boolean forward) {
        if (forward) {
            this.processOneForForward(source, target);
        } else {
            this.processOneForBackward(source, target);
        }
    }

    private void forwardBatch(Iterator<Id> ites, EdgeStep step) {
        this.traverseBatch(ites, step, true);
    }

    private void backwardBatch(Iterator<Id> ites, EdgeStep step) {
        this.traverseBatch(ites, step, false);
    }

    private void traverseBatch(Iterator<Id> v, EdgeStep step, boolean forward) {
        if (this.reachLimit()) {
            return;
        }

        List<Id> labelIds = new ArrayList(step.labels().keySet());

        HugeTraverser.EdgeIdsIterator edgeIts =
            this.traverser.edgeIdsOfVertices(v, step.direction(), labelIds,
                step.degree(), step.skipDegree(),
                Query.OrderType.ORDER_NONE);

        AdjacentVerticesBatchConsumerTemplatePaths consumer1 =
            new AdjacentVerticesBatchConsumerTemplatePaths(this, forward);

        //使用单线程来做
        this.traverseBatchCurrentThread(edgeIts, consumer1, "traverse-ite-edge", 1);
    }

    protected abstract void processOneForForward(Id source, Id target);

    protected abstract void processOneForBackward(Id source, Id target);

    protected abstract void reInitCurrentStepIfNeeded(EdgeStep step,
                                                      boolean forward);

    public void clearNewVertices() {
        this.newVertices = this.newMultiValueMap();
    }

    public void addNodeToNewVertices(Id id, HugeTraverser.Node node) {
        this.addNode(this.newVertices, id, node);
    }

    public Map<Id, List<HugeTraverser.Node>> newMultiValueMap() {
        return this.traverseStrategy.newMultiValueMap();
    }

    public Set<HugeTraverser.Path> newPathSet() {
        return this.traverseStrategy.newPathSet();
    }

    public void addNode(Map<Id, List<HugeTraverser.Node>> vertices, Id id,
                        HugeTraverser.Node node) {
        this.traverseStrategy.addNode(vertices, id, node);
    }

    public void addNewVerticesToAll(Map<Id, List<HugeTraverser.Node>> targets) {
        this.traverseStrategy.addNewVerticesToAll(this.newVertices, targets);
    }

    public Set<HugeTraverser.Path> paths() {
        return this.paths;
    }

    public int pathCount() {
        return this.paths.size();
    }

    protected boolean finished() {
        return this.stepCount >= this.totalSteps || this.reachLimit();
    }

    protected boolean reachLimit() {
        HugeTraverser.checkCapacity(this.capacity, this.accessedNodes(),
            "template paths");
        return this.limit != NO_LIMIT && this.pathCount() >= this.limit;
    }

    protected int accessedNodes() {
        int size = 0;
        for (List<HugeTraverser.Node> value : this.sourcesAll.values()) {
            size += value.size();
        }
        for (List<HugeTraverser.Node> value : this.targetsAll.values()) {
            size += value.size();
        }
        return size;
    }

    protected <K> long traverseBatchCurrentThread(Iterator<CIter<K>> iterator,
                                                  Consumer<CIter<K>> consumer,
                                                  String name,
                                                  int queueWorkerSize) {
        if (!iterator.hasNext()) {
            CloseableIterator.closeIterator(iterator);
            return 0L;
        }

        AtomicBoolean done = new AtomicBoolean(false);
        Consumers<CIter<K>> consumers = new Consumers<>(null,
            consumer, () -> {
            done.set(true);
        }, queueWorkerSize);
        consumers.start(name);
        long total = 0L;
        try {
            while (iterator.hasNext() && !done.get()) {
                total++;
                CIter<K> v = iterator.next();
                consumers.provide(v);
            }
        } catch (Consumers.StopExecution e) {
            // pass
        } catch (Throwable e) {
            throw Consumers.wrapException(e);
        } finally {
            try {
                consumers.await();
            } catch (Throwable e) {
                throw Consumers.wrapException(e);
            } finally {
                executors.returnExecutor(consumers.executor());
                CloseableIterator.closeIterator(iterator);
            }
        }
        return total;
    }

    static class AdjacentVerticesBatchConsumerTemplatePaths implements Consumer<CIter<EdgeId>> {

        private final PathTraverser pathTraverser;
        private final boolean forward;

        public AdjacentVerticesBatchConsumerTemplatePaths(PathTraverser pathTraverser,
                                                          boolean forward) {
            this.pathTraverser = pathTraverser;
            this.forward = forward;
        }

        @Override
        public void accept(CIter<EdgeId> edges) {
            if (this.reachLimit()) {
                return;
            }
            long edgesCount = 0;
            while (!this.reachLimit() && edges.hasNext()) {
                ++edgesCount;
                EdgeId edgeId = edges.next();

                Id source = edgeId.ownerVertexId();
                Id target = edgeId.otherVertexId();
                this.pathTraverser.processOne(source, target, this.forward);
            }
            this.pathTraverser.traverser.edgeIterCounter.addAndGet(edgesCount);
        }

        @Override
        public Consumer<CIter<EdgeId>> andThen(Consumer<? super CIter<EdgeId>> after) {
            java.util.Objects.requireNonNull(after);
            return (CIter<EdgeId> t) -> {
                accept(t);
                after.accept(t);
            };
        }

        private boolean reachLimit() {
            return this.pathTraverser.reachLimit();
        }
    }
}
