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
import static org.apache.hugegraph.traversal.algorithm.HugeTraverser.DEFAULT_ELEMENTS_LIMIT;
import static org.apache.hugegraph.traversal.algorithm.HugeTraverser.DEFAULT_MAX_DEGREE;
import static org.apache.hugegraph.traversal.algorithm.HugeTraverser.closeSet;

import java.util.ArrayList;
import java.util.Collection;
import java.util.HashSet;
import java.util.Iterator;
import java.util.List;
import java.util.Set;

import org.apache.hugegraph.HugeGraph;
import org.apache.hugegraph.api.graph.EdgeAPI;
import org.apache.hugegraph.api.graph.VertexAPI;
import org.apache.hugegraph.backend.id.Id;
import org.apache.hugegraph.backend.query.QueryResults;
import org.apache.hugegraph.core.GraphManager;
import org.apache.hugegraph.server.RestServer;
import org.apache.hugegraph.structure.HugeVertex;
import org.apache.hugegraph.traversal.algorithm.HugeTraverser;
import org.apache.hugegraph.traversal.algorithm.KoutTraverser;
import org.apache.hugegraph.traversal.algorithm.records.KoutRecords;
import org.apache.hugegraph.traversal.algorithm.steps.Steps;
import org.apache.hugegraph.type.define.Directions;
import org.apache.hugegraph.util.E;
import org.apache.hugegraph.util.Log;
import org.apache.tinkerpop.gremlin.structure.Edge;
import org.apache.tinkerpop.gremlin.structure.Vertex;
import org.apache.tinkerpop.gremlin.structure.util.CloseableIterator;
import org.slf4j.Logger;

import com.codahale.metrics.annotation.Timed;
import com.fasterxml.jackson.annotation.JsonProperty;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;

import io.swagger.v3.oas.annotations.tags.Tag;
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

@Path("graphs/{graph}/traversers/kout")
@Singleton
@Tag(name = "KoutAPI")
public class KoutAPI extends TraverserAPI {
//宏观线程模型RestFull------------->KoutAPI------------------->KoutTraverser------------------->HugeGraph
    private static final Logger LOG = Log.logger(RestServer.class);

    @GET
    @Timed
    @Produces(APPLICATION_JSON_WITH_CHARSET)
    public String get(@Context GraphManager manager,
                      @PathParam("graph") String graph,
                      @QueryParam("source") String source,
                      @QueryParam("direction") String direction,
                      @QueryParam("label") String edgeLabel,
                      @QueryParam("max_depth") int depth,
                      @QueryParam("nearest")
                      @DefaultValue("true") boolean nearest,
                      @QueryParam("count_only")
                      @DefaultValue("false") boolean count_only,
                      @QueryParam("concurrent")
                      @DefaultValue("false") boolean concurrent,
                      @QueryParam("max_degree")
                      @DefaultValue(DEFAULT_MAX_DEGREE) long maxDegree,
                      @QueryParam("capacity")
                      @DefaultValue(DEFAULT_CAPACITY) long capacity,
                      @QueryParam("algorithm")
                      @DefaultValue(HugeTraverser.ALGORITHMS_BREADTH_FIRST)
                      String algorithm,
                      @QueryParam("limit")
                      @DefaultValue(DEFAULT_ELEMENTS_LIMIT) long limit) {
        LOG.debug("Graph [{}] get k-out from '{}' with " +
                "direction '{}', edge label '{}', max depth '{}', nearest " +
                "'{}', max degree '{}', capacity '{}', limit '{}' and " +
                "algorithm '{}'", graph, source, direction, edgeLabel, depth,
            nearest, maxDegree, capacity, limit, algorithm);

        HugeTraverser.checkAlgorithm(algorithm);
        ApiMeasurer measure = new ApiMeasurer();

        Id sourceId = VertexAPI.checkAndParseVertexId(source);
        Directions dir = Directions.convert(EdgeAPI.parseDirection(direction));

        HugeGraph g = graph(manager, graph);

        Collection<Id> ids;
        try (KoutTraverser traverser = new KoutTraverser(g)) {
            if (HugeTraverser.isDeepFirstAlgorithm(algorithm)) {
                Steps steps = steps(g, dir, edgeLabel, maxDegree);
                KoutRecords results = traverser.deepFirstKout(
                    sourceId, steps, depth,
                    nearest, capacity, limit, false);
                ids = results.ids(limit);
            } else {
                ids = traverser.kout(sourceId, dir, edgeLabel, depth,
                    nearest, concurrent, maxDegree, capacity, limit);
            }
            measure.addIterCount(traverser.vertexIterCounter.get(),
                traverser.edgeIterCounter.get());
        }

        String result = "";
        if (count_only) {
            result = manager.serializer(g, measure.measures()).writeMap(
                ImmutableMap.of("vertices_size", ids.size()));
        } else {
            result = manager.serializer(g, measure.measures())
                .writeList("vertices", ids);
        }

        closeSet(ids);
        return result;
    }

    @POST
    @Timed
    @Consumes(APPLICATION_JSON)
    @Produces(APPLICATION_JSON_WITH_CHARSET)
    public String post(@Context GraphManager manager,
                       @PathParam("graph") String graph,
                       Request request) {
        ApiMeasurer measure = new ApiMeasurer();

        E.checkArgumentNotNull(request, "The request body can't be null");
        E.checkArgumentNotNull(request.source,
            "The source of request can't be null");
        E.checkArgument(request.steps != null,
            "The steps of request can't be null");
        if (request.countOnly) {
            E.checkArgument(!request.withVertex && !request.withPath,
                "Can't return vertex or path when count only");
        }
        HugeTraverser.checkAlgorithm(request.algorithm);

        LOG.debug("Graph [{}] get customized kout from source vertex '{}', " +
                "with steps '{}', max_depth '{}', nearest '{}', " +
                "count_only '{}', capacity '{}', limit '{}', " +
                "with_vertex '{}', with_path '{}', with_edge '{}' " +
                "and algorithm '{}'", graph, request.source, request.steps,
            request.maxDepth, request.nearest, request.countOnly,
            request.capacity, request.limit, request.withVertex,
            request.withPath, request.withEdge, request.algorithm);

        HugeGraph g = graph(manager, graph);
        Id sourceId = HugeVertex.getIdValue(request.source);
        Steps steps = steps(g, request.steps);

        KoutRecords results;
        List<Integer> countResults = null;
        try (KoutTraverser traverser = new KoutTraverser(g)) {
            if (HugeTraverser.isDeepFirstAlgorithm(request.algorithm)) {
                results = traverser.deepFirstKout(sourceId, steps,
                    request.maxDepth,
                    request.nearest,
                    request.capacity,
                    request.limit,
                    request.withEdge);
            } else {
                results = traverser.customizedKout(sourceId, steps,
                    request.maxDepth,
                    request.nearest,
                    request.capacity,
                    request.limit,
                    request.withEdge);
            }
            measure.addIterCount(traverser.vertexIterCounter.get(),
                traverser.edgeIterCounter.get());
        }

        assert results != null : "Result can't be null, please check params";
        long size = results.size();
        if (size > request.limit) {
            size = request.limit;
        }
        List<Id> neighbors = request.countOnly ?
            ImmutableList.of() : results.ids(request.limit);

        if (request.countOnly) {
            countResults = new ArrayList<>();
            for (int i = 1; i < results.records().size(); ++i) {
                countResults.add(results.records().get(i).size());
            }
        }

        HugeTraverser.PathSet paths = new HugeTraverser.PathSet();
        if (request.withPath && !request.countOnly) {
            paths.addAll(results.paths(request.limit));
        }

        Iterator<Vertex> iterVertex = QueryResults.emptyIterator();
        Iterator<Edge> iterEdge = QueryResults.emptyIterator();
        if (request.withVertex && !request.countOnly) {
            Set<Id> ids = new HashSet<>(neighbors);
            if (request.withPath) {
                for (HugeTraverser.Path p : paths) {
                    ids.addAll(p.vertices());
                }
            }
            if (!ids.isEmpty()) {
                iterVertex = g.vertices(ids.toArray());
                measure.addIterCount(ids.size(), 0);
            }
        }

        if (request.withEdge && !request.countOnly) {
            Iterator<Edge> iter = results.getEdges();
            if (iter == null) {
                Set<Id> ids = results.getEdgeIds();
                if (!ids.isEmpty()) {
                    iter = g.edges(ids.toArray());
                    measure.addIterCount(0, ids.size());
                }
            }
            if (iter != null) {
                iterEdge = iter;
            }
        }

        if (request.countOnly) {
            assert countResults != null;
            LOG.info(String.format("kout-count for %s: %s", request.source, countResults));
        }
        try {
            return manager.serializer(g, measure.measures())
                .writeNodesWithPath("kout", neighbors, size,
                    paths, iterVertex, iterEdge,
                    countResults);
        } finally {
            CloseableIterator.closeIterator(iterVertex);
            CloseableIterator.closeIterator(iterEdge);
        }
    }

    private static class Request {

        @JsonProperty("source")
        public Object source;
        @JsonProperty("steps")
        public VESteps steps;
        @JsonProperty("max_depth")
        public int maxDepth;
        @JsonProperty("nearest")
        public boolean nearest = true;
        @JsonProperty("count_only")
        public boolean countOnly = false;
        @JsonProperty("capacity")
        public long capacity = Long.parseLong(DEFAULT_CAPACITY);
        @JsonProperty("limit")
        public long limit = Long.parseLong(DEFAULT_ELEMENTS_LIMIT);
        @JsonProperty("with_vertex")
        public boolean withVertex = false;
        @JsonProperty("with_path")
        public boolean withPath = false;
        @JsonProperty("with_edge")
        public boolean withEdge = false;
        @JsonProperty("algorithm")
        public String algorithm = HugeTraverser.ALGORITHMS_BREADTH_FIRST;

        @Override
        public String toString() {
            return String.format("KoutRequest{source=%s,steps=%s," +
                    "maxDepth=%s,nearest=%s,countOnly=%s," +
                    "capacity=%s,limit=%s,withVertex=%s," +
                    "withPath=%s,withEdge=%s,algorithm=%s}",
                this.source, this.steps, this.maxDepth,
                this.nearest, this.countOnly, this.capacity,
                this.limit, this.withVertex, this.withPath,
                this.withEdge, this.algorithm);
        }
    }
}
