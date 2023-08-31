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

import java.util.Set;
import java.util.function.Consumer;

import org.apache.hugegraph.HugeGraph;
import org.apache.hugegraph.backend.id.EdgeId;
import org.apache.hugegraph.backend.id.Id;
import org.apache.hugegraph.backend.query.Query;
import org.apache.hugegraph.traversal.algorithm.records.KneighborRecords;
import org.apache.hugegraph.traversal.algorithm.records.record.RecordType;
import org.apache.hugegraph.traversal.algorithm.steps.Steps;
import org.apache.hugegraph.type.define.Directions;
import org.apache.hugegraph.util.E;

public class KneighborTraverser extends OltpTraverser {

    public KneighborTraverser(HugeGraph graph) {
        super(graph);
    }

    public Set<Id> kneighbor(Id sourceV, Directions dir,
                             String label, int depth,
                             long degree, long limit) {
        E.checkNotNull(sourceV, "source vertex id");
        this.checkVertexExist(sourceV, "source");
        E.checkNotNull(dir, "direction");
        checkPositive(depth, "k-neighbor max_depth");
        checkDegree(degree);
        checkLimit(limit);

        Id labelId = this.getEdgeLabelId(label);

        KneighborRecords records = new KneighborRecords(RecordType.INT,
            true, sourceV, true);
        Consumer<EdgeId> consumer = edgeId -> {
            if (this.reachLimit(limit, records.size())) {
                return;
            }
            records.addPath(edgeId.ownerVertexId(), edgeId.otherVertexId());
        };

        while (depth-- > 0) {
            records.startOneLayer(true);
            bfsQuery(records.keys(), dir, labelId, degree,
                NO_LIMIT, NO_LIMIT, consumer, Query.OrderType.ORDER_NONE);
            records.finishOneLayer();
            if (this.reachLimit(limit, records.size())) {
                break;
            }
        }

        return records.idSet(limit);
    }

    public KneighborRecords customizedKneighbor(Id source, Steps steps,
                                                int maxDepth, long limit,
                                                boolean withEdge) {
        Set<Id> sources = newSet();
        sources.add(source);
        return customizedKneighbor(sources, steps, maxDepth, limit, withEdge);
    }

    public KneighborRecords customizedKneighbor(Set<Id> sources, Steps steps,
                                                int maxDepth, long limit,
                                                boolean withEdge) {
        E.checkNotNull(sources, "source vertices");
        E.checkArgument(sources.size() > 0, "source vertices can't be empty");
        for (Id source : sources) {
            E.checkNotNull(source, "source vertex id");
            this.checkVertexExist(source, "source");
        }
        checkPositive(maxDepth, "egonet max_depth");
        checkLimit(limit);

        KneighborRecords records = new KneighborRecords(RecordType.INT,
            true,
            sources,
            true);

        Consumer<EdgeId> consumer = edgeId -> {
            if (this.reachLimit(limit, records.size())) {
                return;
            }
            records.addPath(edgeId.ownerVertexId(), edgeId.otherVertexId());
            if (withEdge) {
                // for breadth, we have to collect all edge during traversal,
                // to avoid over occupy for memory, we collect edgeId only.
                records.addEdgeId(edgeId);
            }
        };

        while (maxDepth-- > 0) {
            records.startOneLayer(true);
            bfsQuery(records.keys(), steps, NO_LIMIT, consumer,
                Query.OrderType.ORDER_NONE);
            records.finishOneLayer();
            if (this.reachLimit(limit, records.size())) {
                break;
            }
        }

        return records;
    }

    private boolean reachLimit(long limit, int size) {
        return limit != NO_LIMIT && size >= limit;
    }
}
