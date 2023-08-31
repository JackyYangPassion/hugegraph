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

import static org.apache.hugegraph.traversal.algorithm.HugeTraverser.DEFAULT_LIMIT;
import static org.apache.hugegraph.traversal.algorithm.HugeTraverser.DEFAULT_MAX_DEGREE;
import static org.apache.hugegraph.traversal.algorithm.HugeTraverser.DEFAULT_MAX_DEPTH;
import static org.apache.hugegraph.traversal.algorithm.HugeTraverser.NO_LIMIT;

import java.util.List;
import java.util.Map;

import org.apache.commons.compress.utils.Lists;
import org.apache.hugegraph.HugeGraph;
import org.apache.hugegraph.api.API;
import org.apache.hugegraph.backend.id.Id;
import org.apache.hugegraph.core.GraphManager;
import org.apache.hugegraph.server.RestServer;
import org.apache.hugegraph.structure.HugeVertex;
import org.apache.hugegraph.traversal.algorithm.HugeTraverser;
import org.apache.hugegraph.traversal.algorithm.PersonalRankTraverser;
import org.apache.hugegraph.util.E;
import org.apache.hugegraph.util.Log;
import org.slf4j.Logger;

import com.codahale.metrics.annotation.Timed;
import com.fasterxml.jackson.annotation.JsonProperty;
import com.google.common.collect.ImmutableMap;

import jakarta.inject.Singleton;
import jakarta.ws.rs.POST;
import jakarta.ws.rs.Path;
import jakarta.ws.rs.PathParam;
import jakarta.ws.rs.Produces;
import jakarta.ws.rs.core.Context;

@Path("graphs/{graph}/traversers/personalrank")
@Singleton
public class PersonalRankAPI extends API {

    private static final Logger LOG = Log.logger(RestServer.class);

    private static final double DEFAULT_DIFF = 0.0001;
    private static final double DEFAULT_SAMPLE = 0.85;
    private static final int DEFAULT_DEPTH = 5;

    @POST
    @Timed
    @Produces(APPLICATION_JSON_WITH_CHARSET)
    public String personalRank(@Context GraphManager manager,
                               @PathParam("graph") String graph,
                               RankRequest request) {

        E.checkArgument(request.label != null,
            "The edge label of rank request can't be null");
        this.validParams(request);

        LOG.debug("Graph [{}] get personal rank from '{}' with " +
                "edge label '{}', alpha '{}', maxDegree '{}', " +
                "max depth '{}' and sorted '{}'",
            graph, request.source, request.label, request.alpha,
            request.maxDegree, request.maxDepth, request.sorted);

        ApiMeasurer measure = new ApiMeasurer();
        Id sourceId = HugeVertex.getIdValue(request.source);
        HugeGraph g = graph(manager, graph);

        PersonalRankTraverser traverser;
        traverser = new PersonalRankTraverser(g, request.alpha, request.maxDegree,
            request.maxDepth);
        Map<Id, Double> ranks = traverser.personalRank(sourceId, request.label,
            request.withLabel);
        measure.addIterCount(traverser.vertexIterCounter.get(),
            traverser.edgeIterCounter.get());
        ranks = HugeTraverser.topN(ranks, request.sorted, request.limit);
        return manager.serializer(g, measure.measures())
            .writeMap(ImmutableMap.of("personal_rank", ranks));
    }

    /**
     * 采用批量方式计算personal rank
     *
     * @param manager
     * @param graph
     * @param request
     * @return
     */
    @Path("batch")
    @POST
    @Timed
    @Produces(APPLICATION_JSON_WITH_CHARSET)
    public String personalRankForBatch(@Context GraphManager manager,
                                       @PathParam("graph") String graph,
                                       RankRequest request) {
        if (!request.useLabel) {
            request.labels = Lists.newArrayList();
        }
        E.checkArgument(request.labels != null,
            "The edge labels of rank request can't be null");
        this.validParams(request);

        LOG.debug("Graph [{}] get personal rank from '{}' with " +
                "edge label '{}', alpha '{}', maxDegree '{}', " +
                "max depth '{}' and sorted '{}'",
            graph, request.source, request.label, request.alpha,
            request.maxDegree, request.maxDepth, request.sorted);

        ApiMeasurer measure = new ApiMeasurer();
        Id sourceId = HugeVertex.getIdValue(request.source);
        HugeGraph g = graph(manager, graph);

        PersonalRankTraverser traverser;
        traverser = new PersonalRankTraverser(g, request.alpha, request.maxDegree,
            request.maxDepth);
        Map<Id, Double> ranks = traverser.personalRankForBatch(sourceId,
            request.labels,
            request.withLabel,
            request.direction);
        measure.addIterCount(traverser.vertexIterCounter.get(),
            traverser.edgeIterCounter.get());
        ranks = HugeTraverser.topNByPriorityQueue(ranks, request.sorted, request.limit);
        return manager.serializer(g, measure.measures())
            .writeMap(ImmutableMap.of("personal_rank", ranks));
    }

    /**
     * 对请求输入的参数 进行校验
     *
     * @param request
     */
    public void validParams(RankRequest request) {
        E.checkArgumentNotNull(request, "The rank request body can't be null");
        E.checkArgument(request.source != null,
            "The source vertex id of rank request can't be null");
        E.checkArgument(request.alpha > 0 && request.alpha <= 1.0,
            "The alpha of rank request must be in range (0, 1], " +
                "but got '%s'", request.alpha);
        E.checkArgument(request.maxDiff > 0 && request.maxDiff <= 1.0,
            "The max diff of rank request must be in range " +
                "(0, 1], but got '%s'", request.maxDiff);
        E.checkArgument(request.maxDegree > 0L || request.maxDegree == NO_LIMIT,
            "The max degree of rank request must be > 0 " +
                "or == -1, but got: %s", request.maxDegree);
        E.checkArgument(request.limit > 0L || request.limit == NO_LIMIT,
            "The limit of rank request must be > 0 or == -1, " +
                "but got: %s", request.limit);
        E.checkArgument(request.maxDepth > 1L &&
                request.maxDepth <= Long.parseLong(DEFAULT_MAX_DEPTH),
            "The max depth of rank request must be " +
                "in range (1, %s], but got '%s'",
            DEFAULT_MAX_DEPTH, request.maxDepth);
    }

    private static class RankRequest {

        @JsonProperty("useLabel")
        public Boolean useLabel = true;
        @JsonProperty("direction")
        public String direction;
        @JsonProperty("source")
        private Object source;
        @JsonProperty("label")
        private String label;
        @JsonProperty("labels")
        private List<String> labels;
        @JsonProperty("alpha")
        private double alpha = DEFAULT_SAMPLE;
        @JsonProperty("max_diff")
        private double maxDiff = DEFAULT_DIFF;
        @JsonProperty("max_degree")
        private long maxDegree = Long.parseLong(DEFAULT_MAX_DEGREE);
        @JsonProperty("limit")
        private long limit = Long.parseLong(DEFAULT_LIMIT);
        @JsonProperty("max_depth")
        private int maxDepth = DEFAULT_DEPTH;
        @JsonProperty("with_label")
        private PersonalRankTraverser.WithLabel withLabel =
            PersonalRankTraverser.WithLabel.BOTH_LABEL;
        @JsonProperty("sorted")
        private boolean sorted = true;

        @Override
        public String toString() {
            return String.format("RankRequest{source=%s,label=%s,alpha=%s," +
                    "maxDiff=%s,maxDegree=%s,limit=%s," +
                    "maxDepth=%s,withLabel=%s,sorted=%s}",
                this.source, this.label, this.alpha,
                this.maxDiff, this.maxDegree, this.limit,
                this.maxDepth, this.withLabel, this.sorted);
        }
    }
}
