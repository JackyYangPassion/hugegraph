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

import java.util.*;
import java.util.function.Consumer;
import java.util.function.Function;

import org.apache.hugegraph.HugeGraph;
import org.apache.hugegraph.backend.id.EdgeId;
import org.apache.hugegraph.backend.id.Id;
import org.apache.hugegraph.backend.query.Query;
import org.apache.hugegraph.traversal.algorithm.records.PathsRecords;
import org.apache.hugegraph.traversal.algorithm.steps.EdgeStep;
import org.apache.hugegraph.type.define.Directions;
import org.apache.hugegraph.util.Consumers;

public class PathCommonTraverser extends OltpTraverser {

    private final PathsRecords record;
    private final Directions direction;
    private final List<Id> labels;
    private final long degree;
    private final long skipDegree;
    private final long capacity;
    private final boolean shortest;
    private final boolean withEdge;
    private EdgeStep step;
    private volatile int shortestPathLen = Integer.MAX_VALUE;

    public PathCommonTraverser(HugeGraph graph,
                               Collection<Id> sources, Collection<Id> targets,
                               Directions dir,
                               List<Id> labels,
                               long degree,
                               long skipDegree,
                               long capacity,
                               boolean shortest,
                               boolean withEdge) {
        super(graph);
        this.record = new PathsRecords(true, sources, targets);
        this.direction = dir;
        this.labels = labels;
        this.degree = degree;
        this.skipDegree = skipDegree;
        this.capacity = capacity;
        this.shortest = shortest;
        this.withEdge = withEdge;
    }

    public PathCommonTraverser(HugeGraph graph,
                               Collection<Id> sources, Collection<Id> targets,
                               EdgeStep step,
                               long capacity,
                               boolean shortest,
                               boolean withEdge) {
        super(graph);
        this.record = new PathsRecords(true, sources, targets);
        this.direction = step.direction();
        this.labels = new ArrayList<>();
        for (Id id : step.edgeLabels()) {
            labels.add(id);
        }
        this.step = step;
        this.degree = step.degree();
        this.skipDegree = step.skipDegree();
        this.capacity = capacity;
        this.shortest = shortest;
        this.withEdge = withEdge;
    }

    public HugeTraverser.PathSet nextStep(boolean all, boolean forward) {
        // TODO 调整消费者模型,不通过带锁的set汇总结果,最好取得各线程结果后再统一汇总
        HugeTraverser.PathSet results = new HugeTraverser.PathSet(true);
        long degree = this.skipDegree > 0L ? this.skipDegree : this.degree;

        Directions dir = forward ? this.direction : this.direction.opposite();
        Function<Id, Boolean> filter = t -> !isSuperNode(t, dir, skipDegree);
        Consumer<EdgeId> consumer = edgeId -> {
            Id sourceId = edgeId.ownerVertexId();
            Id targetId = edgeId.otherVertexId();
            if (withEdge) {
                record.addEdgeId(sourceId, targetId, edgeId);
            }
            HugeTraverser.PathSet paths = this.record.findPath(sourceId, targetId,
                filter, all, false);
            if (paths.isEmpty()) {
                return;
            }
            if (shortest) {
                for (Path path : paths) {
                    int len = path.vertices().size();
                    if (len < shortestPathLen) {
                        synchronized (this) {
                            if (len < shortestPathLen) {
                                shortestPathLen = len;
                            }
                        }
                    }
                    if (len <= shortestPathLen) {
                        results.add(path);
                    }
                }
            } else {
                results.addAll(paths);

            }
            if (!all) {
                throw new Consumers.StopExecution("path found");
            }
        };

        this.record.startOneLayer(forward);
        List<Id> vertices = this.record.ids(Query.NO_LIMIT);
        if (step != null) {
            if (dir != step.direction() && Directions.BOTH != step.direction()) {
                step.swithDirection();
            }
            bfsQuery(vertices.iterator(), step, capacity, consumer, false,
                Query.OrderType.ORDER_NONE);
        } else {
            bfsQuery(vertices.iterator(),
                dir,
                labels != null && !labels.isEmpty() ? labels : null,
                degree,
                skipDegree,
                capacity,
                consumer,
                Query.OrderType.ORDER_NONE);
        }
        this.vertexIterCounter.getAndAdd(vertices.size());
        this.record.finishOneLayer();

        if (shortest) {
            results.parallelStream().filter(path -> path.vertices().size() > shortestPathLen)
                .forEach(results::remove);
        }
        if (!all) {
            subset(results, 1);
        }
        return results;
    }

    public PathResult nextStepMore(boolean all, boolean forward) {
        PathResult result = new PathResult();
        result.paths = nextStep(all, forward);
        if (withEdge) {
            result.edgeIds = this.record.getEdgeIds(result.paths);
        }
        return result;
    }

    public Set<Id> getEdges(HugeTraverser.PathSet paths) {
        return this.record.getEdgeIds(paths);
    }

    public boolean forward() {
        return this.record.lessSources();
    }

    public long accessed() {
        return this.record.accessed();
    }

    public static class PathResult {
        protected Set<Id> edgeIds = Collections.emptySet();
        protected Set<Path> paths = Collections.emptySet();

        public Set<Id> getEdgeIds() {
            return edgeIds;
        }

        public void setEdgeIds(Set<Id> edgeIds) {
            this.edgeIds = edgeIds;
        }

        public Set<Path> getPaths() {
            return paths;
        }

        public void setPaths(PathSet paths) {
            this.paths = paths;
        }

        void add(PathResult result) {
            if (edgeIds.isEmpty()) {
                edgeIds = result.getEdgeIds();
            } else {
                edgeIds.addAll(result.getEdgeIds());
            }
            if (paths.isEmpty()) {
                paths = result.getPaths();
            } else {
                paths.addAll(result.getPaths());
            }
        }
    }
}
