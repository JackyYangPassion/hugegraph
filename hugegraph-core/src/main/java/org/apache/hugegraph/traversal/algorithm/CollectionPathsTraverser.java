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

import java.util.Iterator;
import java.util.List;

import org.apache.hugegraph.HugeGraph;
import org.apache.hugegraph.backend.id.Id;
import org.apache.hugegraph.structure.HugeVertex;
import org.apache.hugegraph.traversal.algorithm.steps.EdgeStep;
import org.apache.hugegraph.util.E;
import org.apache.tinkerpop.gremlin.structure.Vertex;

public class CollectionPathsTraverser extends HugeTraverser {

    public CollectionPathsTraverser(HugeGraph graph) {
        super(graph);
    }

    public PathCommonTraverser.PathResult paths(Iterator<Vertex> sources,
                                                Iterator<Vertex> targets,
                                                EdgeStep step, int depth, boolean nearest,
                                                long capacity, long limit, boolean withEdge) {
        checkCapacity(capacity);
        checkLimit(limit);

        List<Id> sourceList = newList();
        while (sources.hasNext()) {
            sourceList.add(((HugeVertex) sources.next()).id());
        }
        int sourceSize = sourceList.size();
        E.checkState(sourceSize >= 1 && sourceSize <= MAX_VERTICES,
            "The number of source vertices must in [1, %s], " +
                "but got: %s", MAX_VERTICES, sourceList.size());
        List<Id> targetList = newList();
        while (targets.hasNext()) {
            targetList.add(((HugeVertex) targets.next()).id());
        }
        int targetSize = targetList.size();
        E.checkState(targetSize >= 1 && targetSize <= MAX_VERTICES,
            "The number of target vertices must in [1, %s], " +
                "but got: %s", MAX_VERTICES, sourceList.size());
        checkPositive(depth, "max depth");
        PathCommonTraverser traverser = new PathCommonTraverser(graph(),
            sourceList, targetList,
            step, capacity, nearest,
            withEdge);
        PathCommonTraverser.PathResult result =
            new PathCommonTraverser.PathResult();
        this.vertexIterCounter = traverser.vertexIterCounter;
        this.edgeIterCounter = traverser.edgeIterCounter;
        while (true) {
            PathCommonTraverser.PathResult tmp =
                traverser.nextStepMore(true, traverser.forward());
            if (!tmp.paths.isEmpty()) {
                result.add(tmp);
            }
            // Found, reach max depth or reach capacity, stop searching
            if (--depth <= 0) {
                break;
            }
            checkCapacity(capacity, traverser.accessed(),
                "shortest path");
        }
        return result;
    }
}
