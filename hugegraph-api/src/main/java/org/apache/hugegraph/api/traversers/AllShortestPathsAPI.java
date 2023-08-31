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

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashSet;
import java.util.Iterator;
import java.util.List;
import java.util.Set;

import org.apache.hugegraph.HugeGraph;
import org.apache.hugegraph.api.API;
import org.apache.hugegraph.api.graph.EdgeAPI;
import org.apache.hugegraph.api.graph.VertexAPI;
import org.apache.hugegraph.backend.id.Id;
import org.apache.hugegraph.backend.query.QueryResults;
import org.apache.hugegraph.core.GraphManager;
import org.apache.hugegraph.server.RestServer;
import org.apache.hugegraph.traversal.algorithm.HugeTraverser;
import org.apache.hugegraph.traversal.algorithm.PathCommonTraverser;
import org.apache.hugegraph.traversal.algorithm.ShortestPathTraverser;
import org.apache.hugegraph.type.define.Directions;
import org.apache.hugegraph.util.Log;
import org.apache.tinkerpop.gremlin.structure.Edge;
import org.apache.tinkerpop.gremlin.structure.Vertex;
import org.apache.tinkerpop.gremlin.structure.util.CloseableIterator;
import org.slf4j.Logger;

import com.codahale.metrics.annotation.Timed;
import com.google.common.collect.ImmutableList;

import io.swagger.v3.oas.annotations.tags.Tag;
import jakarta.inject.Singleton;
import jakarta.ws.rs.DefaultValue;
import jakarta.ws.rs.GET;
import jakarta.ws.rs.Path;
import jakarta.ws.rs.PathParam;
import jakarta.ws.rs.Produces;
import jakarta.ws.rs.QueryParam;
import jakarta.ws.rs.core.Context;

@Path("graphs/{graph}/traversers/allshortestpaths")
@Singleton
@Tag(name = "AllShortestPathsAPI")
public class AllShortestPathsAPI extends API {

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
                      @QueryParam("with_vertex")
                      @DefaultValue("false") boolean withVertex,
                      @QueryParam("with_edge")
                      @DefaultValue("false") boolean withEdge,
                      @QueryParam("max_degree")
                      @DefaultValue(DEFAULT_MAX_DEGREE) long maxDegree,
                      @QueryParam("skip_degree")
                      @DefaultValue("0") long skipDegree,
                      @QueryParam("capacity")
                      @DefaultValue(DEFAULT_CAPACITY) long capacity) {
        LOG.debug("Graph [{}] get shortest path from '{}', to '{}' with " +
                "direction {}, edge label {}, max depth '{}', " +
                "max degree '{}', skipped degree '{}' and capacity '{}'",
            graph, source, target, direction, edgeLabel, depth,
            maxDegree, skipDegree, capacity);

        Id sourceId = VertexAPI.checkAndParseVertexId(source);
        Id targetId = VertexAPI.checkAndParseVertexId(target);
        Directions dir = Directions.convert(EdgeAPI.parseDirection(direction));

        ApiMeasurer measure = new ApiMeasurer();
        HugeGraph g = graph(manager,graph);

        ShortestPathTraverser traverser = new ShortestPathTraverser(g);
        List<String> edgeLabels;
        if (edgeLabel == null) {
            edgeLabels = ImmutableList.of();
        } else {
            edgeLabels = new ArrayList<>(Arrays.asList(edgeLabel.split(",")));
        }

        PathCommonTraverser.PathResult result = traverser.allShortestPaths(
            sourceId, targetId, dir,
            edgeLabels, depth, maxDegree,
            skipDegree, capacity, withEdge);
        Iterator<Edge> edges = QueryResults.emptyIterator();
        if (withEdge) {
            if (result.getEdgeIds().isEmpty()) {
                edges = Collections.emptyIterator();
            } else {
                edges = g.edges(result.getEdgeIds().toArray());
            }
        }
        Iterator<Vertex> vertexs = QueryResults.emptyIterator();
        if (withVertex) {
            Set<Id> ids = new HashSet<>();
            for (HugeTraverser.Path p : result.getPaths()) {
                ids.addAll(p.vertices());
            }
            if (!ids.isEmpty()) {
                vertexs = g.vertices(ids.toArray());
            }
        }
        measure.addIterCount(traverser.vertexIterCounter.get(),
            traverser.edgeIterCounter.get());
        try {
            return manager.serializer(g, measure.measures())
                .writePaths("paths", result.getPaths(),
                    false, vertexs, edges);
        } finally {
            CloseableIterator.closeIterator(vertexs);
            CloseableIterator.closeIterator(edges);
        }
    }
}
