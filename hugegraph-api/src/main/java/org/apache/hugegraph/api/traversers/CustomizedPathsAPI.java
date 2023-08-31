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

package org.apache.hugegraph.api.traversers;

import static org.apache.hugegraph.traversal.algorithm.HugeTraverser.DEFAULT_CAPACITY;
import static org.apache.hugegraph.traversal.algorithm.HugeTraverser.DEFAULT_MAX_DEGREE;
import static org.apache.hugegraph.traversal.algorithm.HugeTraverser.DEFAULT_PATHS_LIMIT;
import static org.apache.hugegraph.traversal.algorithm.HugeTraverser.DEFAULT_SAMPLE;
import static org.apache.hugegraph.traversal.algorithm.HugeTraverser.DEFAULT_WEIGHT;

import java.util.ArrayList;
import java.util.HashSet;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Set;

import org.apache.hugegraph.HugeGraph;
import org.apache.hugegraph.api.API;
import org.apache.hugegraph.backend.id.Id;
import org.apache.hugegraph.backend.query.QueryResults;
import org.apache.hugegraph.core.GraphManager;
import org.apache.hugegraph.server.RestServer;
import org.apache.hugegraph.traversal.algorithm.CustomizePathsTraverser;
import org.apache.hugegraph.traversal.algorithm.HugeTraverser;
import org.apache.hugegraph.traversal.algorithm.steps.WeightedEdgeStep;
import org.apache.hugegraph.type.define.Directions;
import org.apache.hugegraph.util.E;
import org.apache.hugegraph.util.Log;
import org.apache.tinkerpop.gremlin.structure.Vertex;
import org.apache.tinkerpop.gremlin.structure.util.CloseableIterator;
import org.slf4j.Logger;

import com.codahale.metrics.annotation.Timed;
import com.fasterxml.jackson.annotation.JsonAlias;
import com.fasterxml.jackson.annotation.JsonProperty;

import jakarta.inject.Singleton;
import jakarta.ws.rs.Consumes;
import jakarta.ws.rs.POST;
import jakarta.ws.rs.Path;
import jakarta.ws.rs.PathParam;
import jakarta.ws.rs.Produces;
import jakarta.ws.rs.core.Context;

@Path("graphs/{graph}/traversers/customizedpaths")
@Singleton
public class CustomizedPathsAPI extends API {

    private static final Logger LOG = Log.logger(RestServer.class);

    private static List<WeightedEdgeStep> step(HugeGraph graph,
                                               PathRequest req) {
        int stepSize = req.steps.size();
        List<WeightedEdgeStep> steps = new ArrayList<>(stepSize);
        for (Step step : req.steps) {
            steps.add(step.jsonToStep(graph));
        }
        return steps;
    }

    @POST
    @Timed
    @Consumes(APPLICATION_JSON)
    @Produces(APPLICATION_JSON_WITH_CHARSET)
    public String post(@Context GraphManager manager,
                       @PathParam("graph") String graph,
                       PathRequest request) {
        E.checkArgumentNotNull(request, "The path request body can't be null");
        E.checkArgumentNotNull(request.sources,
            "The sources of path request can't be null");
        E.checkArgument(request.steps != null && !request.steps.isEmpty(),
            "The steps of path request can't be empty");
        if (request.sortBy == null) {
            request.sortBy = SortBy.NONE;
        }

        LOG.debug("Graph [{}] get customized paths from source vertex '{}', " +
                "with steps '{}', sort by '{}', capacity '{}', limit '{}' " +
                "and with_vertex '{}'", graph, request.sources, request.steps,
            request.sortBy, request.capacity, request.limit,
            request.withVertex);

        ApiMeasurer measure = new ApiMeasurer();
        HugeGraph g = graph(manager, graph);
        Iterator<Vertex> sources = request.sources.vertices(g);
        List<WeightedEdgeStep> steps = step(g, request);
        boolean sorted = request.sortBy != SortBy.NONE;

        List<HugeTraverser.Path> paths;
        try (CustomizePathsTraverser traverser = new CustomizePathsTraverser(g)) {
            paths = traverser.customizedPaths(sources, steps, sorted,
                request.capacity, request.limit);
            measure.addIterCount(traverser.vertexIterCounter.get(),
                traverser.edgeIterCounter.get());
        } finally {
            CloseableIterator.closeIterator(sources);
        }

        if (sorted) {
            boolean incr = request.sortBy == SortBy.INCR;
            paths = CustomizePathsTraverser.topNPath(paths, incr,
                request.limit);
        }

        if (!request.withVertex) {
            return manager.serializer().writePaths("paths", paths, false);
        }

        Set<Id> ids = new HashSet<>();
        for (HugeTraverser.Path p : paths) {
            ids.addAll(p.vertices());
        }
        Iterator<Vertex> iter = QueryResults.emptyIterator();
        if (!ids.isEmpty()) {
            iter = g.vertices(ids.toArray());
            measure.addIterCount(ids.size(), 0);
        }
        try {
            return manager.serializer(g, measure.measures())
                .writePaths("paths", paths, false, iter, null);
        } finally {
            CloseableIterator.closeIterator(iter);
        }
    }

    private enum SortBy {
        INCR,
        DECR,
        NONE
    }

    private static class PathRequest {

        @JsonProperty("sources")
        public Vertices sources;
        @JsonProperty("steps")
        public List<Step> steps;
        @JsonProperty("sort_by")
        public SortBy sortBy;
        @JsonProperty("capacity")
        public long capacity = Long.parseLong(DEFAULT_CAPACITY);
        @JsonProperty("limit")
        public long limit = Long.parseLong(DEFAULT_PATHS_LIMIT);
        @JsonProperty("with_vertex")
        public boolean withVertex = false;

        @Override
        public String toString() {
            return String.format("PathRequest{sourceVertex=%s,steps=%s," +
                    "sortBy=%s,capacity=%s,limit=%s," +
                    "withVertex=%s}", this.sources, this.steps,
                this.sortBy, this.capacity, this.limit,
                this.withVertex);
        }
    }

    private static class Step {

        @JsonProperty("direction")
        public Directions direction;
        @JsonProperty("labels")
        public List<String> labels;
        @JsonProperty("properties")
        public Map<String, Object> properties;
        @JsonAlias("degree")
        @JsonProperty("max_degree")
        public long maxDegree = Long.parseLong(DEFAULT_MAX_DEGREE);
        @JsonProperty("skip_degree")
        public long skipDegree = 0L;
        @JsonProperty("weight_by")
        public String weightBy;
        @JsonProperty("default_weight")
        public double defaultWeight = Double.parseDouble(DEFAULT_WEIGHT);
        @JsonProperty("sample")
        public long sample = Long.parseLong(DEFAULT_SAMPLE);

        @Override
        public String toString() {
            return String.format("Step{direction=%s,labels=%s,properties=%s," +
                    "maxDegree=%s,skipDegree=%s," +
                    "weightBy=%s,defaultWeight=%s,sample=%s}",
                this.direction, this.labels, this.properties,
                this.maxDegree, this.skipDegree,
                this.weightBy, this.defaultWeight,
                this.sample);
        }

        private WeightedEdgeStep jsonToStep(HugeGraph g) {
            return new WeightedEdgeStep(g, this.direction, this.labels,
                this.properties, this.maxDegree,
                this.skipDegree, this.weightBy,
                this.defaultWeight, this.sample);
        }
    }
}
