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

import java.util.HashSet;
import java.util.Iterator;
import java.util.List;
import java.util.Set;
import java.util.function.Consumer;

import org.apache.hugegraph.HugeException;
import org.apache.hugegraph.HugeGraph;
import org.apache.hugegraph.backend.id.EdgeId;
import org.apache.hugegraph.backend.id.Id;
import org.apache.hugegraph.backend.query.Query;
import org.apache.hugegraph.exception.ErrorCode;
import org.apache.hugegraph.structure.HugeEdge;
import org.apache.hugegraph.traversal.algorithm.records.KoutRecords;
import org.apache.hugegraph.traversal.algorithm.records.record.RecordType;
import org.apache.hugegraph.traversal.algorithm.steps.Steps;
import org.apache.hugegraph.type.define.Directions;
import org.apache.hugegraph.util.E;
import org.apache.hugegraph.util.collection.JniIdSet;
import org.apache.tinkerpop.gremlin.structure.Edge;
import org.apache.tinkerpop.gremlin.structure.util.CloseableIterator;

public class KoutTraverser extends OltpTraverser {

    public KoutTraverser(HugeGraph graph) {
        super(graph);
    }

    public Set<Id> kout(Id sourceV, Directions dir, String label,
                        int maxDepth, boolean nearest, boolean concurrent,
                        long degree, long capacity, long limit) {
        E.checkNotNull(sourceV, "source vertex id");
        //    this.checkVertexExist(sourceV, "source vertex");
        E.checkNotNull(dir, "direction");
        checkPositive(maxDepth, "k-out max_depth");
        checkDegree(degree);
        checkCapacity(capacity);
        checkLimit(limit);
        if (capacity != NO_LIMIT) {
            // Capacity must > limit because sourceV is counted in capacity
            E.checkArgument(capacity >= limit && limit != NO_LIMIT,
                "Capacity can't be less than limit, " +
                    "but got capacity '%s' and limit '%s'",
                capacity, limit);
        }

        LOG.info(
            "kout label {}, maxDepth {}, nearest {}, concurrent {}, degree {}, capacity {}, " +
                "limit {}",
            label, maxDepth, nearest, concurrent, degree, capacity, limit);

        Id labelId = this.getEdgeLabelId(label);

        Set<Id> sources = newJniIdSet();
        Set<Id> neighbors = newJniIdSet();
        Set<Id> visited = nearest ? newJniIdSet() : null;

        neighbors.add(sourceV);

        ConcurrentVerticesConsumer consumer;

        long remaining = capacity == NO_LIMIT ? NO_LIMIT : capacity - 1;

        int depth = maxDepth;
        while (depth-- > 0) {
            // Just get limit nodes in last layer if limit < remaining capacity
            if (depth == 0 && limit != NO_LIMIT && (limit < remaining || remaining == NO_LIMIT)) {
                remaining = limit;
            }

            if (visited != null) {
                visited.addAll(neighbors);
            }

            // 交换 sources与ne
            Set<Id> temp = neighbors;
            neighbors = sources;
            sources = temp;

            // 以上层的邻居点为起点。 最多为剩下的capacity
            consumer = new ConcurrentVerticesConsumer(sourceV, visited, remaining, neighbors);

            this.vertexIterCounter.addAndGet(sources.size());
            // skipDegree 默认设为 NO_LIMIT，以便获取全部顶点
            bfsQuery(sources.iterator(), dir, labelId, degree, NO_LIMIT, capacity,
                consumer, Query.OrderType.ORDER_NONE);

            sources.clear();

            if (capacity != NO_LIMIT) {
                // Update 'remaining' value to record remaining capacity
                remaining -= neighbors.size();

                if (remaining <= 0 && depth > 0) {
                    throw new HugeException(ErrorCode.REACH_CAPACITY_WITH_DEPTH,
                        capacity, depth);
                }
            }
        }

        closeSet(visited);
        closeSet(sources);

        if (limit != NO_LIMIT && neighbors.size() > limit) {
            Set<Id> ids = subset(neighbors, limit);
            return ids;
        }
        return neighbors;
    }

    public KoutRecords customizedKout(Id source, Steps steps, int maxDepth,
                                      boolean nearest, long capacity,
                                      long limit, boolean withEdge) {
        E.checkNotNull(source, "source vertex id");
        this.checkVertexExist(source, "source");
        checkPositive(maxDepth, "k-out max_depth");
        checkCapacity(capacity);
        checkLimit(limit);
        long[] depth = new long[1];
        depth[0] = maxDepth;
        // 核心数据结构，记录每一跳点边结果
        KoutRecords records = new KoutRecords(RecordType.INT, true,
                                              source, nearest, 0);

        // 每一个元素反序列化逻辑，执行accept 的时候，进行序列化
        Consumer<EdgeId> consumer = edgeId -> {
            if (this.reachLimit(limit, depth[0], records.size())) {
                return;
            }
            records.addPath(edgeId.ownerVertexId(), edgeId.otherVertexId());
            if (withEdge) {
                records.addEdgeId(edgeId);
            }
        };

        while (depth[0]-- > 0) {//BFS : 每一圈进行批量下传，并发获取结果
            List<Id> sources = records.ids(Query.NO_LIMIT);
            records.startOneLayer(true);// 核心数据结构，记录图的每一层节点 && 边
            bfsQuery(sources.iterator(), steps, capacity, consumer, Query.OrderType.ORDER_NONE);
            this.vertexIterCounter.addAndGet(sources.size());
            records.finishOneLayer();
            checkCapacity(capacity, records.accessed(), depth[0]);
        }

        if (withEdge) {
            // we should filter out unused-edge for breadth first algorithm.
            records.filterUnusedEdges(limit);
        }

        return records;
    }

    public KoutRecords deepFirstKout(Id sourceV, Steps steps, int depth,
                                     boolean nearest, long capacity,
                                     long limit, boolean withEdge) {
        E.checkNotNull(sourceV, "source vertex id");
        this.checkVertexExist(sourceV, "source");
        checkPositive(depth, "k-out max_depth");
        checkCapacity(capacity);
        checkLimit(limit);

        Set<Id> all = newIdSet();
        all.add(sourceV);

        KoutRecords records = new KoutRecords(RecordType.INT, false, sourceV, nearest, depth);

        Iterator<Edge> it = this.createNestedIterator(sourceV, steps, depth, all, false);
        long edgesCount = 0;
        try {
            while (it.hasNext()) {
                edgesCount += 1;
                HugeEdge edge = (HugeEdge) it.next();
                Id target = edge.id().otherVertexId();
                if (!nearest || !all.contains(target)) {
                    records.addFullPath(HugeTraverser.getPathEdges(it, edge),
                        withEdge);
                }

                if (limit != NO_LIMIT && records.size() >= limit ||
                    capacity != NO_LIMIT && all.size() > capacity) {
                    break;
                }
            }
        } finally {
            CloseableIterator.closeIterator(it);
        }
        this.edgeIterCounter.addAndGet(edgesCount);
        this.vertexIterCounter.addAndGet(1);
        return records;
    }

    private void checkCapacity(long capacity, long accessed, long depth) {
        if (capacity == NO_LIMIT) {
            return;
        }
        if (accessed >= capacity && depth > 0) {
            throw new HugeException(
                ErrorCode.REACH_CAPACITY_WITH_DEPTH,
                capacity, depth);
        }
    }

    private boolean reachLimit(long limit, long depth, long size) {
        return limit != NO_LIMIT && depth <= 0 && size >= limit;
    }
}
