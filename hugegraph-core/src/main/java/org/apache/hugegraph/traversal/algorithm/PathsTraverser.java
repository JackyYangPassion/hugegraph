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

import java.util.List;
import java.util.function.Consumer;

import org.apache.hugegraph.HugeGraph;
import org.apache.hugegraph.backend.id.EdgeId;
import org.apache.hugegraph.backend.id.Id;
import org.apache.hugegraph.backend.query.Query;
import org.apache.hugegraph.iterator.CIter;
import org.apache.hugegraph.perf.PerfUtil.Watched;
import org.apache.hugegraph.traversal.algorithm.records.PathsRecords;
import org.apache.hugegraph.type.define.Directions;
import org.apache.hugegraph.util.E;

public class PathsTraverser extends OltpTraverser {

    public PathsTraverser(HugeGraph graph) {
        super(graph);
    }

    @Watched
    public PathSet paths(Id sourceV, Directions sourceDir,
                         Id targetV, Directions targetDir, String label,
                         int depth, long degree, long capacity, long limit) {
        E.checkNotNull(sourceV, "source vertex id");
        E.checkNotNull(targetV, "target vertex id");
        this.checkVertexExist(sourceV, "source");
        this.checkVertexExist(targetV, "target");
        E.checkNotNull(sourceDir, "source direction");
        E.checkNotNull(targetDir, "target direction");
        E.checkArgument(sourceDir == targetDir ||
                sourceDir == targetDir.opposite(),
            "Source direction must equal to target direction" +
                " or opposite to target direction");
        E.checkArgument(depth > 0 && depth <= 5000,
            "The depth must be in (0, 5000], but got: %s", depth);
        checkDegree(degree);
        checkCapacity(capacity);
        checkLimit(limit);

        PathSet paths = new PathSet();
        if (sourceV.equals(targetV)) {
            return paths;
        }

        Id labelId = this.getEdgeLabelId(label);
        Traverser traverser = new Traverser(sourceV, targetV, labelId,
            degree, capacity, limit);
        // We should stop early if find cycle or reach limit
        while (true) {
            if (--depth < 0 || traverser.reachLimit()) {
                break;
            }
            traverser.forward(targetV, sourceDir);

            if (--depth < 0 || traverser.reachLimit()) {
                break;
            }

            traverser.backward(sourceV, targetDir);
        }
        paths.addAll(traverser.paths());
        vertexIterCounter.addAndGet(traverser.accessed());
        return paths;
    }

    private class AdjacentVerticesBatchConsumerPath implements Consumer<CIter<EdgeId>> {

        private final Traverser traverser;
        private final String name;

        public AdjacentVerticesBatchConsumerPath(Traverser traverser, String name) {
            this.traverser = traverser;
            this.name = name;
        }

        @Override
        public void accept(CIter<EdgeId> edges) {
            if (this.traverser.reachLimit()) {
                return;
            }

            long count = 0;
            while (!this.traverser.reachLimit() && edges.hasNext()) {
                count++;
                EdgeId edgeId = edges.next();

                Id owner = edgeId.ownerVertexId();
                Id target = edgeId.otherVertexId();

                LOG.debug("Go {}, vid {}, edge {}, targetId {}", name, owner, edgeId, target);
                PathSet results = this.traverser.record.findPath(owner, target, null, true, false);
                for (Path path : results) {
                    this.traverser.paths.add(path);
                    if (this.traverser.reachLimit()) {
                        return;
                    }
                }
            }
            edgeIterCounter.addAndGet(count);
        }
    }

    private class Traverser {

        private final PathsRecords record;

        private final Id label;
        private final long degree;
        private final long capacity;
        private final long limit;

        private final PathSet paths;

        public Traverser(Id sourceV, Id targetV, Id label,
                         long degree, long capacity, long limit) {
            this.record = new PathsRecords(true, sourceV, targetV);
            this.label = label;
            this.degree = degree;
            this.capacity = capacity;
            this.limit = limit;

            this.paths = new PathSet(true);
        }

        /**
         * Search forward from source
         */
        @Watched
        public void forward(Id targetV, Directions direction) {
            this.record.startOneLayer(true);
            List<Id> vids = newList();
            while (this.record.hasNextKey()) {
                Id vid = this.record.nextKey();
                if (vid.equals(targetV)) {
                    LOG.debug("out of index, cur {}. targetV {}", vid, targetV);
                    continue;
                }
                vids.add(vid);
            }
            this.record.resetOneLayer();
            EdgeIdsIterator edgeIts = edgeIdsOfVertices(vids.iterator(), direction,
                this.label, degree, DEF_SKIP_DEGREE,
                Query.OrderType.ORDER_NONE);
            AdjacentVerticesBatchConsumerPath consumer =
                new AdjacentVerticesBatchConsumerPath(this, "forward");
            traverseBatch(edgeIts, consumer, "traverse-ite-edge", 1);
            this.record.finishOneLayer();
        }

        /**
         * Search backward from target
         */
        @Watched
        public void backward(Id sourceV, Directions direction) {
            this.record.startOneLayer(false);
            List<Id> vids = newList();
            while (this.record.hasNextKey()) {
                Id vid = this.record.nextKey();
                if (vid.equals(sourceV)) {
                    LOG.debug("out of index, cur {}. source {}", vid, sourceV);
                    continue;
                }
                vids.add(vid);
            }
            this.record.resetOneLayer();
            EdgeIdsIterator edgeIts = edgeIdsOfVertices(vids.iterator(), direction,
                this.label, this.degree, DEF_SKIP_DEGREE,
                Query.OrderType.ORDER_NONE);
            AdjacentVerticesBatchConsumerPath consumer =
                new AdjacentVerticesBatchConsumerPath(this, "backward");
            traverseBatch(edgeIts, consumer, "traverse-ite-edge", 1);
            this.record.finishOneLayer();
        }

        public PathSet paths() {
            return this.paths;
        }

        private boolean reachLimit() {
            checkCapacity(this.capacity, this.record.accessed(), "paths");
            return this.limit != NO_LIMIT && this.paths.size() >= this.limit;
        }

        public long accessed() {
            return this.record.accessed();
        }
    }
}
