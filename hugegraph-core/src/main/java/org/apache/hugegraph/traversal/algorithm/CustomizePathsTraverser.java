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

import java.util.Collections;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.function.Consumer;

import org.apache.hugegraph.HugeGraph;
import org.apache.hugegraph.backend.id.Id;
import org.apache.hugegraph.backend.query.Query;
import org.apache.hugegraph.structure.HugeEdge;
import org.apache.hugegraph.structure.HugeVertex;
import org.apache.hugegraph.traversal.algorithm.steps.EdgeStep;
import org.apache.hugegraph.traversal.algorithm.steps.WeightedEdgeStep;
import org.apache.hugegraph.util.CollectionUtil;
import org.apache.hugegraph.util.E;
import org.apache.tinkerpop.gremlin.structure.Edge;
import org.apache.tinkerpop.gremlin.structure.Vertex;

import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;

import jakarta.ws.rs.core.MultivaluedMap;

public class CustomizePathsTraverser extends OltpTraverser {

    private long pathCount;
    private long limit;
    private long capacity;

    public CustomizePathsTraverser(HugeGraph graph) {
        super(graph);
        this.pathCount = 0;
        this.limit = 0;
        this.capacity = 0;
    }

    public static List<Path> topNPath(List<Path> paths,
                                      boolean incr, long limit) {
        paths.sort((p1, p2) -> {
            WeightPath wp1 = (WeightPath) p1;
            WeightPath wp2 = (WeightPath) p2;
            int result = Double.compare(wp1.totalWeight(), wp2.totalWeight());
            return incr ? result : -result;
        });

        if (limit == HugeTraverser.NO_LIMIT || paths.size() <= limit) {
            return paths;
        }
        return paths.subList(0, (int) limit);
    }

    public List<Path> customizedPaths(Iterator<Vertex> vertices,
                                      List<WeightedEdgeStep> steps, boolean sorted,
                                      long capacity, long limit) {
        E.checkArgument(vertices.hasNext(),
            "The source vertices can't be empty");
        E.checkArgument(!steps.isEmpty(), "The steps can't be empty");
        checkCapacity(capacity);
        checkLimit(limit);

        this.limit = limit;
        this.capacity = capacity;

        MultivaluedMap<Id, Node> sources = newMultivalueMap();
        while (vertices.hasNext()) {
            HugeVertex vertex = (HugeVertex) vertices.next();
            Node node = sorted ?
                new WeightNode(vertex.id(), null, 0) :
                new Node(vertex.id(), null);
            sources.add(vertex.id(), node);
        }
        int stepNum = steps.size();
        long access = 0;
        MultivaluedMap<Id, Node> newVertices = null;
        for (WeightedEdgeStep step : steps) {
            stepNum--;
            newVertices = newMultivalueMap();
            sources = traverseNodes(newVertices, sources, step, stepNum,
                sorted, access);

            if (this.pathCount > this.limit) {
                break;
            }
        }
        if (stepNum != 0) {
            return ImmutableList.of();
        }
        List<Path> paths = newList();
        for (List<Node> nodes : newVertices.values()) {
            for (Node n : nodes) {
                if (sorted) {
                    WeightNode wn = (WeightNode) n;
                    paths.add(new WeightPath(wn.path(), wn.weights()));
                } else {
                    paths.add(new Path(n.path()));
                }
            }
        }
        return paths;
    }

    protected MultivaluedMap<Id, Node> traverseNodes(
        MultivaluedMap<Id, Node> newVertices, MultivaluedMap<Id, Node> sources,
        WeightedEdgeStep step, int stepNum, boolean sorted, long access) {

        boolean withProperties = sorted && step.weightBy() != null;

        EdgeStep edgeStep = step.step();

        Map<Id, List<Node>> adjacencies = new HashMap<>();
        for (Map.Entry<Id, List<Node>> entry : sources.entrySet()) {
            adjacencies.put(entry.getKey(), newList(true));
        }

        // Traversal vertices of previous level
        Consumer<Edge> consumer = edgeItem -> {
            HugeEdge edge = (HugeEdge) edgeItem;
            Id target = edge.id().otherVertexId();
            Id owner = edge.id().ownerVertexId();
            List<Node> adjacency = adjacencies.get(owner);
            List<Node> nodes = sources.get(owner);

            for (Node n : nodes) {
                // If have loop, skip target
                if (n.contains(target)) {
                    continue;
                }
                Node newNode;
                if (sorted) {
                    double w = step.weightBy() != null ?
                        edge.value(step.weightBy().name()) :
                        step.defaultWeight();
                    newNode = new WeightNode(target, n, w);
                } else {
                    newNode = new Node(target, n);
                }
                adjacency.add(newNode);
            }
        };

        bfsQuery(sources.keySet().iterator(), edgeStep, capacity, consumer,
            true, Query.OrderType.ORDER_NONE, withProperties);

        if (step.sample() > 0) {
            for (Map.Entry<Id, List<Node>> entry : adjacencies.entrySet()) {
                // Sample current node's adjacent nodes
                adjacencies.put(entry.getKey(), sample(entry.getValue(), step.sample()));
            }
        }

        for (Map.Entry<Id, List<Node>> entry : adjacencies.entrySet()) {
            // Add current node's adjacent nodes
            for (Node node : entry.getValue()) {
                newVertices.add(node.id(), node);
                // Avoid exceeding limit
                if (stepNum == 0) {
                    if (this.limit != NO_LIMIT && !sorted &&
                        ++this.pathCount >= this.limit) {
                        break;
                    }
                }
            }
        }
        // Re-init sources
        return newVertices;
    }

    private List<Node> sample(List<Node> nodes, long sample) {
        if (nodes.size() <= sample) {
            return nodes;
        }
        List<Node> result = newList((int) sample);
        int size = nodes.size();
        for (int random : CollectionUtil.randomSet(0, size, (int) sample)) {
            result.add(nodes.get(random));
        }
        return result;
    }

    public static class WeightNode extends Node {

        private final double weight;

        public WeightNode(Id id, Node parent, double weight) {
            super(id, parent);
            this.weight = weight;
        }

        public List<Double> weights() {
            List<Double> weights = newList();
            WeightNode current = this;
            while (current.parent() != null) {
                weights.add(current.weight);
                current = (WeightNode) current.parent();
            }
            Collections.reverse(weights);
            return weights;
        }
    }

    public static class WeightPath extends Path {

        private final List<Double> weights;
        private double totalWeight;

        public WeightPath(List<Id> vertices,
                          List<Double> weights) {
            super(vertices);
            this.weights = weights;
            this.calcTotalWeight();
        }

        public List<Double> weights() {
            return this.weights;
        }

        public double totalWeight() {
            return this.totalWeight;
        }

        @Override
        public void reverse() {
            super.reverse();
            Collections.reverse(this.weights);
        }

        @Override
        public Map<String, Object> toMap(boolean withCrossPoint) {
            if (withCrossPoint) {
                return ImmutableMap.of("crosspoint", this.crosspoint(),
                    "objects", this.vertices(),
                    "weights", this.weights());
            } else {
                return ImmutableMap.of("objects", this.vertices(),
                    "weights", this.weights());
            }
        }

        private void calcTotalWeight() {
            double sum = 0;
            for (double w : this.weights()) {
                sum += w;
            }
            this.totalWeight = sum;
        }
    }
}
