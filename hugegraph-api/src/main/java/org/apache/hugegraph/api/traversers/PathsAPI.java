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

import java.util.Collections;
import java.util.HashSet;
import java.util.Iterator;
import java.util.Set;

import org.apache.hugegraph.HugeGraph;
import org.apache.hugegraph.api.graph.EdgeAPI;
import org.apache.hugegraph.api.graph.VertexAPI;
import org.apache.hugegraph.backend.id.Id;
import org.apache.hugegraph.core.GraphManager;
import org.apache.hugegraph.exception.ErrorCode;
import org.apache.hugegraph.server.RestServer;
import org.apache.hugegraph.traversal.algorithm.CollectionPathsTraverser;
import org.apache.hugegraph.traversal.algorithm.HugeTraverser;
import org.apache.hugegraph.traversal.algorithm.PathCommonTraverser;
import org.apache.hugegraph.traversal.algorithm.PathsTraverser;
import org.apache.hugegraph.traversal.algorithm.steps.EdgeStep;
import org.apache.hugegraph.type.define.Directions;
import org.apache.hugegraph.util.E;
import org.apache.hugegraph.util.Log;
import org.apache.tinkerpop.gremlin.structure.Edge;
import org.apache.tinkerpop.gremlin.structure.Vertex;
import org.apache.tinkerpop.gremlin.structure.util.CloseableIterator;
import org.slf4j.Logger;

import com.codahale.metrics.annotation.Timed;
import com.fasterxml.jackson.annotation.JsonProperty;

import jakarta.inject.Singleton;
import jakarta.ws.rs.Consumes;
import jakarta.ws.rs.DefaultValue;
import jakarta.ws.rs.GET;
import jakarta.ws.rs.POST;
import jakarta.ws.rs.Path;
import jakarta.ws.rs.PathParam;
import jakarta.ws.rs.Produces;
import jakarta.ws.rs.QueryParam;
import jakarta.ws.rs.core.Context;

@Path("graphs/{graph}/traversers/paths")
@Singleton
public class PathsAPI extends TraverserAPI {

    private static final Logger LOG = Log.logger(RestServer.class);

    @GET
    @Timed
    @Produces(APPLICATION_JSON_WITH_CHARSET)
    public String get(@Context GraphManager manager,
                      @PathParam("graph") String graph,
                      @QueryParam("source") String source,
                      @QueryParam("target") String target,
                      @QueryParam("direction") String direction,
                      @QueryParam("label") String edgeLabel,
                      @QueryParam("max_depth") int depth,
                      @QueryParam("max_degree")
                      @DefaultValue(DEFAULT_MAX_DEGREE) long maxDegree,
                      @QueryParam("capacity")
                      @DefaultValue(DEFAULT_CAPACITY) long capacity,
                      @QueryParam("limit")
                      @DefaultValue(DEFAULT_PATHS_LIMIT) long limit) {
        LOG.debug("Graph [{}] get paths from '{}', to '{}' with " +
                "direction {}, edge label {}, max depth '{}', " +
                "max degree '{}', capacity '{}' and limit '{}'",
            graph, source, target, direction, edgeLabel, depth,
            maxDegree, capacity, limit);

        Id sourceId = VertexAPI.checkAndParseVertexId(source);
        Id targetId = VertexAPI.checkAndParseVertexId(target);
        Directions dir = Directions.convert(EdgeAPI.parseDirection(direction));

        ApiMeasurer measure = new ApiMeasurer();
        HugeGraph g = graph(manager, graph);
        HugeTraverser.PathSet paths;
        try (PathsTraverser traverser = new PathsTraverser(g)) {
            paths = traverser.paths(sourceId, dir, targetId,
                dir.opposite(), edgeLabel,
                depth, maxDegree, capacity,
                limit);
            measure.addIterCount(traverser.vertexIterCounter.get(),
                traverser.edgeIterCounter.get());
        }
        return manager.serializer(g, measure.measures()).writePaths("paths", paths, false);
    }

    @POST
    @Timed
    @Consumes(APPLICATION_JSON)
    @Produces(APPLICATION_JSON_WITH_CHARSET)
    public String post(@Context GraphManager manager,
                       @PathParam("graph") String graph,
                       Request request) {
        E.checkArgumentNotNull(request, "The request body can't be null");
        E.checkArgumentNotNull(request.sources,
            "The sources of request can't be null");
        E.checkArgumentNotNull(request.targets,
            "The targets of request can't be null");
        E.checkArgumentNotNull(request.step,
            "The step of request can't be null");
        E.checkArgument(request.depth > 0 && request.depth <= 5000,
            ErrorCode.DEPTH_OUT_RANGE.format(request.depth));

        LOG.debug("Graph [{}] get paths from source vertices '{}', target " +
                "vertices '{}', with step '{}', max depth '{}', " +
                "capacity '{}', limit '{}' and with_vertex '{}'",
            graph, request.sources, request.targets, request.step,
            request.depth, request.capacity, request.limit,
            request.withVertex);

        ApiMeasurer measure = new ApiMeasurer();
        HugeGraph g = graph(manager, graph);
        Iterator<Vertex> sources = request.sources.vertices(g);
        Iterator<Vertex> targets = request.targets.vertices(g);
        EdgeStep edgeStep = step(g, request.step);

        CollectionPathsTraverser traverser = new CollectionPathsTraverser(g);
        PathCommonTraverser.PathResult result;
        try {
            result = traverser.paths(sources, targets, edgeStep, request.depth,
                request.nearest, request.capacity,
                request.limit, request.withEdge);
        } finally {
            CloseableIterator.closeIterator(sources);
            CloseableIterator.closeIterator(targets);
        }

        measure.addIterCount(traverser.vertexIterCounter.get(),
            traverser.edgeIterCounter.get());

        Iterator<Vertex> vertexs = Collections.emptyIterator();
        if (request.withVertex) {
            Set<Id> ids = new HashSet<>();
            for (HugeTraverser.Path p : result.getPaths()) {
                ids.addAll(p.vertices());
            }
            if (!ids.isEmpty()) {
                vertexs = g.vertices(ids.toArray());
                measure.addIterCount(ids.size(), 0);
            } else {
                vertexs = Collections.emptyIterator();
            }
        }

        Iterator<Edge> edges = Collections.emptyIterator();
        if (request.withEdge) {
            if (result.getEdgeIds().isEmpty()) {
                edges = Collections.emptyIterator();
            } else {
                Set<Id> edgeIds = result.getEdgeIds();
                edges = g.edges(edgeIds.toArray());
                measure.addIterCount(0, edgeIds.size());
            }
        }
        try {
            return manager.serializer(g, measure.measures())
                .writePaths("paths", result.getPaths(), false,
                    vertexs, edges);
        } finally {
            CloseableIterator.closeIterator(vertexs);
            CloseableIterator.closeIterator(edges);
        }
    }

    private static class Request {

        @JsonProperty("sources")
        public Vertices sources;
        @JsonProperty("targets")
        public Vertices targets;
        @JsonProperty("step")
        public TraverserAPI.Step step;
        @JsonProperty("max_depth")
        public int depth;
        @JsonProperty("nearest")
        public boolean nearest = false;
        @JsonProperty("capacity")
        public long capacity = Long.parseLong(DEFAULT_CAPACITY);
        @JsonProperty("limit")
        public long limit = Long.parseLong(DEFAULT_PATHS_LIMIT);
        @JsonProperty("with_vertex")
        public boolean withVertex = false;
        @JsonProperty("with_edge")
        public boolean withEdge = false;

        @Override
        public String toString() {
            return String.format("PathRequest{sources=%s,targets=%s,step=%s," +
                    "maxDepth=%s,nearest=%s,capacity=%s," +
                    "limit=%s,withVertex=%s,withEdge=%s}", this.sources,
                this.targets, this.step, this.depth,
                this.nearest, this.capacity,
                this.limit, this.withVertex,
                this.withEdge);
        }
    }
}
