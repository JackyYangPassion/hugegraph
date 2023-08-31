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

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;

import org.apache.hugegraph.HugeGraph;
import org.apache.hugegraph.backend.id.Id;
import org.apache.hugegraph.perf.PerfUtil.Watched;
import org.apache.hugegraph.traversal.algorithm.steps.EdgeStep;
import org.apache.hugegraph.type.define.Directions;
import org.apache.hugegraph.util.E;

import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableSet;

public class ShortestPathTraverser extends HugeTraverser {

    public ShortestPathTraverser(HugeGraph graph) {
        super(graph);
    }

    @Watched
    public Path shortestPath(Id sourceV, Id targetV, Directions dir,
                             List<String> labels, int depth, long degree,
                             long skipDegree, long capacity) {
        E.checkNotNull(sourceV, "source vertex id");
        E.checkNotNull(targetV, "target vertex id");
        this.checkVertexExist(sourceV, "source");
        this.checkVertexExist(targetV, "target");
        E.checkNotNull(dir, "direction");
        checkPositive(depth, "max depth");
        checkDegree(degree);
        checkCapacity(capacity);
        checkSkipDegree(skipDegree, degree, capacity);

        if (sourceV.equals(targetV)) {
            return new Path(ImmutableList.of(sourceV));
        }

        List<Id> labelIds = new ArrayList<>(labels.size());
        for (String label : labels) {
            labelIds.add(this.getEdgeLabelId(label));
        }

        PathCommonTraverser traverser = new PathCommonTraverser(graph(),
            Collections.singletonList(sourceV),
            Collections.singletonList(targetV),
            dir, labelIds, degree,
            skipDegree, capacity, true, false);
        PathSet paths;
        while (true) {
            paths = traverser.nextStep(false, traverser.forward());
            // Found, reach max depth or reach capacity, stop searching
            if (!paths.isEmpty() || --depth <= 0) {
                break;
            }
            checkCapacity(capacity, traverser.accessed(),
                "shortest path");
        }
        this.edgeIterCounter.addAndGet(traverser.accessed());
        this.vertexIterCounter.addAndGet(traverser.vertexIterCounter.get());
        return paths.isEmpty() ? Path.EMPTY_PATH : paths.iterator().next();
    }

    public Path shortestPath(Id sourceV, Id targetV, EdgeStep step,
                             int depth, long capacity) {
        return this.shortestPath(sourceV, targetV, step.direction(),
            newList(step.labels().values()),
            depth, step.degree(), step.skipDegree(),
            capacity);
    }

    public PathCommonTraverser.PathResult allShortestPaths(Id sourceV, Id targetV, Directions dir,
                                                           List<String> labels, int depth,
                                                           long degree,
                                                           long skipDegree, long capacity,
                                                           boolean withEdge) {
        E.checkNotNull(sourceV, "source vertex id");
        E.checkNotNull(targetV, "target vertex id");
        this.checkVertexExist(sourceV, "source");
        this.checkVertexExist(targetV, "target");
        E.checkNotNull(dir, "direction");
        checkPositive(depth, "max depth");
        checkDegree(degree);
        checkCapacity(capacity);
        checkSkipDegree(skipDegree, degree, capacity);

        PathCommonTraverser.PathResult result;
        if (sourceV.equals(targetV)) {
            result = new PathCommonTraverser.PathResult();
            result.paths = ImmutableSet.of(new Path(ImmutableList.of(sourceV)));
            return result;
        }

        List<Id> labelIds = new ArrayList<>(labels.size());
        for (String label : labels) {
            labelIds.add(this.getEdgeLabelId(label));
        }

        PathCommonTraverser traverser = new PathCommonTraverser(graph(),
            Collections.singletonList(sourceV),
            Collections.singletonList(targetV),
            dir, labelIds, degree,
            skipDegree, capacity,
            true, withEdge);
        while (true) {
            result = traverser.nextStepMore(true, traverser.forward());
            // Found, reach max depth or reach capacity, stop searching
            if (!result.paths.isEmpty() || --depth <= 0) {
                break;
            }
            checkCapacity(capacity, traverser.accessed(),
                "shortest path");
        }
        this.edgeIterCounter.addAndGet(traverser.accessed());
        this.vertexIterCounter.addAndGet(traverser.vertexIterCounter.get());
        return result;
    }
}
