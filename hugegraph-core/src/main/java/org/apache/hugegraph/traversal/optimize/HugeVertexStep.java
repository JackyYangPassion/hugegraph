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

package org.apache.hugegraph.traversal.optimize;

import java.util.*;
import java.util.stream.Collectors;

import com.google.common.collect.Iterators;
import com.google.common.collect.Lists;
import org.apache.hugegraph.HugeGraph;
import org.apache.hugegraph.backend.id.Id;
import org.apache.hugegraph.backend.query.ConditionQuery;
import org.apache.hugegraph.backend.query.EdgesQueryIterator;
import org.apache.hugegraph.backend.query.Query;
import org.apache.hugegraph.backend.query.QueryResults;
import org.apache.hugegraph.backend.serializer.EdgeToVertexIterator;
import org.apache.hugegraph.backend.serializer.IteratorCiterIterator;
import org.apache.hugegraph.backend.tx.GraphTransaction;
import org.apache.hugegraph.iterator.BatchMapperIterator;
import org.apache.hugegraph.iterator.CIter;
import org.apache.hugegraph.iterator.MapperIterator;
import org.apache.hugegraph.schema.EdgeLabel;
import org.apache.hugegraph.structure.HugeEdge;
import org.apache.hugegraph.structure.HugeVertex;
import org.apache.hugegraph.type.define.Directions;
import org.apache.tinkerpop.gremlin.process.traversal.Traverser;
import org.apache.tinkerpop.gremlin.process.traversal.step.map.VertexStep;
import org.apache.tinkerpop.gremlin.process.traversal.step.util.HasContainer;
import org.apache.tinkerpop.gremlin.structure.Edge;
import org.apache.tinkerpop.gremlin.structure.Element;
import org.apache.tinkerpop.gremlin.structure.Vertex;
import org.apache.tinkerpop.gremlin.structure.util.StringFactory;
import org.slf4j.Logger;

import org.apache.hugegraph.util.Log;

public class HugeVertexStep<E extends Element>
       extends VertexStep<E> implements QueryHolder {

    private static final long serialVersionUID = -7850636388424382454L;

    private static final Logger LOG = Log.logger(HugeVertexStep.class);

    private final List<HasContainer> hasContainers = new ArrayList<>();
    private Iterator<E> lastTimeResults = QueryResults.emptyIterator();

    private int batchSize = 1000;

    private Traverser.Admin<Vertex> head = null;

    private boolean rangeGlobalFlag = false;

    // 最后一步是否VertexStep
    private boolean endStepFlag;

    private boolean batchPropertyPrefetching = false;

    private boolean isContainEdgeOtherVertexStep;

    private long limit;

    // Store limit/order-by
    private final Query queryInfo = new Query(null);

    private Iterator<E> iterator = QueryResults.emptyIterator();

    public HugeVertexStep(final VertexStep<E> originVertexStep,final boolean flag, boolean endStepFlag,
                          boolean isContainEdgeOtherVertexStep, long limit) {
        super(originVertexStep.getTraversal(),
              originVertexStep.getReturnClass(),
              originVertexStep.getDirection(),
              originVertexStep.getEdgeLabels());
        this.rangeGlobalFlag = flag;
        this.endStepFlag = endStepFlag;
        this.limit = limit;
        originVertexStep.getLabels().forEach(this::addLabel);
    }

    protected Iterator<E> flatMapBatch(final List<Id> traverserIdList) {
        Iterator<E> results;
        boolean queryVertex = this.returnsVertex();
        boolean queryEdge = this.returnsEdge();
        assert queryVertex || queryEdge;
        if (queryVertex) {
            results = (Iterator<E>) this.vertices(traverserIdList);
        } else {
            assert queryEdge;
            results = (Iterator<E>) this.edges(traverserIdList);
        }
        this.lastTimeResults = results;
        return results;
    }

    private Iterator<Vertex> vertices(List<Id> traverserIdList) {
        HugeGraph graph = TraversalUtil.getGraph(this);

        Iterator<Edge> edges = this.edges(traverserIdList);

        // 如果没有条件,直接返回点的信息
        if (!endStepFlag && this.hasContainers.isEmpty()) {
            return new EdgeToVertexIterator<>(edges);
        }

        Iterator<Vertex> vertices = graph.adjacentVertices(edges);

        if (LOG.isDebugEnabled()) {
            LOG.debug("HugeVertexStep.vertices(): is there adjacent " +
                    "vertices of {}: {}, has={}",
                traverserIdList.toArray(), vertices.hasNext(),
                this.hasContainers);
        }

        if (this.hasContainers.isEmpty()) {
            return vertices;
        }

        // TODO: query by vertex index to optimize
        return TraversalUtil.filterResult(this.hasContainers, vertices);
    }

    private Iterator<Edge> edges(List<Id> traverserIdList) {
        HugeGraph graph = TraversalUtil.getGraph(this);
        EdgeLabel[] els = graph.mapElName2El(this.getEdgeLabels());
        //Id[] edgeLabels = graph.mapElName2Id(this.getEdgeLabels());
        // Id vertex = (Id) traverser.get().id();

        Iterator<Edge> edges;
        // Separate the upper layer of the content of Edge Label[] to do query
        if (els.length == 1) {
            edges = edgesBySingleEl(graph, traverserIdList, els[0]);
        } else if (els.length == 0) {
            edges = edgesBySingleEl(graph, traverserIdList, els);
        } else {
            edges = edgesBySingleEl(graph, traverserIdList, els[0]);
            for (int i = 1; i < els.length; i++) {
                edges = Iterators.concat(edges, edgesBySingleEl(graph, traverserIdList, els[i]));
            }
        }

        if (this.batchPropertyPrefetching) {
            edges = prefetchEdges(edges);
        }

        return edges;
    }



    /**
     * prefetch other vertex of edge
     *
     * @param edges
     * @return
     */
    private Iterator<Edge> prefetchEdges(Iterator<Edge> edges) {
        HugeGraph graph = TraversalUtil.getGraph(this);

        return new BatchMapperIterator<>(batchSize, edges,
            batchEdges -> {
                List<Id> vertexIds = new ArrayList<>();
                for (Edge edge : batchEdges) {
                    vertexIds.add(
                        ((HugeEdge) edge).otherVertex().id());
                }
                assert vertexIds.size() > 0;

                List<Vertex> vertices =
                    Lists.newArrayList(
                        graph.vertices(vertexIds.toArray()));
                Map<Object, Vertex> mapVertices =
                    vertices.stream()
                        .collect(Collectors.toMap(v -> v.id(),
                            v -> v,
                            (v1, v2) -> v1));

                ListIterator<Edge> edgeItes =
                    batchEdges.listIterator();

                // Some vertex maybe not exists
                return new MapperIterator<>(edgeItes, (e) -> {
                    HugeEdge edge = (HugeEdge) e;
                    HugeVertex v =
                        (HugeVertex) mapVertices.get(
                            edge.otherVertex().id());
                    if (v != null) {
                        edge.resetOtherVertex(v);
                    }
                    return e;
                });
            });
    }

    private Iterator<Edge> edgesBySingleEl(HugeGraph graph,
                                           List<Id> traverserIdList,
                                           EdgeLabel... edgeLabels) {
        // Build the query, and then go to the back-end query
        List<HasContainer> conditions = this.hasContainers;

        // Query for edge with conditions(else conditions for vertex)
        boolean withEdgeCond = this.returnsEdge() && !conditions.isEmpty();
        boolean withVertexCond = this.returnsVertex() && !conditions.isEmpty();
        Directions direction = Directions.convert(this.getDirection());
        LOG.debug("HugeVertexStep.edges(): vertex={}, direction={}, " +
                "edgeLabels={}, has={}",
            traverserIdList.toArray(), direction, edgeLabels, this.hasContainers);

//        ConditionQuery query = GraphTransaction.constructEdgesQuery(
//                vertex, direction, edgeLabels);

        List<Id> edgeLabelIdList = Lists.newArrayList();
        for (EdgeLabel edgeLabel : edgeLabels) {
            edgeLabelIdList.add(edgeLabel.id());
        }

        ConditionQuery query = GraphTransaction.constructEdgesQuery2(
            null, direction, edgeLabelIdList, graph);

        EdgesQueryIterator queryIterator =
            new EdgesQueryIterator(
                traverserIdList.iterator(), direction,
                edgeLabelIdList, -1L, -1L,
                true, Query.OrderType.ORDER_NONE,
                conditions, graph, withEdgeCond);

        // Query by sort-keys
        if (withEdgeCond) {
            TraversalUtil.fillConditionQuery(query, conditions, graph);
            if (GraphTransaction.matchFullEdgeSortKeys(query, graph)) {
                // if all sysprop conditions are in sort-keys,
                // there is no need to filter the query results
                withEdgeCond = false;
            }
        }

        // Do query
        Iterator<CIter<Edge>> edgesBatch = graph.edges(queryIterator);
        // 双层迭代器转换
        Iterator<Edge> edges = new IteratorCiterIterator<>(edgesBatch);

        // TODO: remove after hstore supports filter props
        if (withEdgeCond) {
            return TraversalUtil.filterResult(conditions, edges);
        }
        return edges;
    }

    protected Iterator<E> flatMapBatchBySingle(final Traverser.Admin<Vertex> traverser) {
        Iterator<E> results;
        boolean queryVertex = this.returnsVertex();
        boolean queryEdge = this.returnsEdge();
        assert queryVertex || queryEdge;
        if (queryVertex) {
            results = (Iterator<E>) this.verticesBySingle(traverser);
        } else {
            assert queryEdge;
            results = (Iterator<E>) this.edgesBySingle(traverser);
        }
        this.lastTimeResults = results;
        return results;
    }

    private Iterator<Vertex> verticesBySingle(Traverser.Admin<Vertex> traverser) {
        HugeGraph graph = TraversalUtil.getGraph(this);
        Vertex vertex = traverser.get();

        Iterator<Edge> edges = this.edgesBySingle(traverser);
        Iterator<Vertex> vertices = graph.adjacentVertices(edges);

        if (LOG.isDebugEnabled()) {
            LOG.debug("HugeVertexStep.vertices(): is there adjacent " +
                    "vertices of {}: {}, has={}",
                vertex.id(), vertices.hasNext(), this.hasContainers);
        }

        if (this.hasContainers.isEmpty()) {
            return vertices;
        }

        // TODO: query by vertex index to optimize
        return TraversalUtil.filterResult(this.hasContainers, vertices);
    }

    private Iterator<Edge> edgesBySingle(Traverser.Admin<Vertex> traverser) {
        HugeGraph graph = TraversalUtil.getGraph(this);
        EdgeLabel[] els = graph.mapElName2El(this.getEdgeLabels());
        Id vertex = (Id) traverser.get().id();

        Iterator<Edge> edges;
        // Separate the upper layer of the content of Edge Label[] to do query
        if (els.length == 1) {
            edges = edgesBySingle(graph, vertex, els[0]);
        } else if (els.length == 0) {
            edges = edgesBySingle(graph, vertex, els);
        } else {
            edges = edgesBySingle(graph, vertex, els[0]);
            for (int i = 1; i < els.length; i++) {
                edges = Iterators.concat(edges, edgesBySingle(graph, vertex, els[i]));
            }
        }

        if (this.batchPropertyPrefetching) {
            edges = prefetchEdges(edges);
        }

        return edges;
    }

    private Iterator<Edge> edgesBySingle(HugeGraph graph,
                                         Id vertex,
                                         EdgeLabel... edgeLabels) {
        // Build the query, and then go to the back-end query
        List<HasContainer> conditions = this.hasContainers;

        // Query for edge with conditions(else conditions for vertex)
        boolean withEdgeCond = this.returnsEdge() && !conditions.isEmpty();
        boolean withVertexCond = this.returnsVertex() && !conditions.isEmpty();
        Directions direction = Directions.convert(this.getDirection());
        LOG.debug("HugeVertexStep.edges(): vertex={}, direction={}, " +
                "edgeLabels={}, has={}",
            vertex, direction, edgeLabels, this.hasContainers);

//        ConditionQuery query = GraphTransaction.constructEdgesQuery(
//                vertex, direction, edgeLabels);

        List<Id> edgeLabelIdList = Lists.newArrayList();
        for (EdgeLabel edgeLabel : edgeLabels) {
            edgeLabelIdList.add(edgeLabel.id());
        }
        ConditionQuery query = GraphTransaction.constructEdgesQuery2(
            vertex, direction, edgeLabelIdList, graph);

        // Query by sort-keys
        if (withEdgeCond) {
            TraversalUtil.fillConditionQuery(query, conditions, graph);
            if (GraphTransaction.matchFullEdgeSortKeys(query, graph)) {
                // if all sysprop conditions are in sort-keys,
                // there is no need to filter the query results
                withEdgeCond = false;
            }
        }

        // Query by has(id)
        if (!query.ids().isEmpty()) {
            // Ignore conditions if query by edge id in has-containers
            // FIXME: should check that the edge id matches the `vertex`
            query.resetConditions();
            LOG.warn("It's not recommended to query by has(id)");
        }

        /*
         * Unset limit when needed to filter property after store query
         * like query: outE().has(k,v).limit(n)
         * NOTE: outE().limit(m).has(k,v).limit(n) will also be unset limit,
         * Can't unset limit if query by paging due to page position will be
         * exceeded when reaching the limit in tinkerpop layer
         */
        if (withEdgeCond || withVertexCond) {
            org.apache.hugegraph.util.E.checkArgument(!this.queryInfo().paging(),
                "Can't query by paging " +
                    "and filtering");
            this.queryInfo().limit(Query.NO_LIMIT);
        }

        query = this.injectQueryInfo(query);

        // Do query
        Iterator<Edge> edges = graph.edges(query);

        // TODO: remove after hstore supports filter props
        if (withEdgeCond) {
            return TraversalUtil.filterResult(conditions, edges);
        }
        return edges;
    }


    protected boolean withVertexCondition() {
        return this.returnsVertex() && !this.hasContainers.isEmpty();
    }

    protected boolean withEdgeCondition() {
        return this.returnsEdge() && !this.hasContainers.isEmpty();
    }

    @Override
    public String toString() {
        if (this.hasContainers.isEmpty()) {
            return super.toString();
        }

        return StringFactory.stepString(
               this,
               getDirection(),
               Arrays.asList(getEdgeLabels()),
               getReturnClass().getSimpleName(),
               this.hasContainers);
    }

    @Override
    public List<HasContainer> getHasContainers() {
        return Collections.unmodifiableList(this.hasContainers);
    }

    @Override
    public void addHasContainer(final HasContainer has) {
        if (SYSPROP_PAGE.equals(has.getKey())) {
            this.setPage((String) has.getValue());
            return;
        }
        this.hasContainers.add(has);
    }

    @Override
    public Query queryInfo() {
        return this.queryInfo;
    }

    @Override
    public Iterator<?> lastTimeResults() {
        return this.iterator;
    }

    public boolean equals(Object obj) {
        if (!(obj instanceof HugeVertexStep)) {
            return false;
        }

        if (!super.equals(obj)) {
            return false;
        }

        HugeVertexStep other = (HugeVertexStep) obj;
        return this.hasContainers.equals(other.hasContainers) &&
               this.queryInfo.equals(other.queryInfo) &&
               this.iterator.equals(other.iterator);
    }

    @Override
    public int hashCode() {
        return super.hashCode() ^
               this.queryInfo.hashCode() ^
               this.hasContainers.hashCode();
    }


    @Override
    protected Traverser.Admin<E> processNextStart() {
        while (true) {
            if (this.iterator.hasNext()) {
                return this.head.split(this.iterator.next(), this);
            } else {
                closeIterator();

                this.head = this.starts.next();

                if (this.queryInfo.paging()) {
                    this.iterator =
                        this.flatMapBatchBySingle(this.head);
                } else {
                    List<Id> allTraverser = Lists.newLinkedList();
                    allTraverser.add((Id) this.head.get().id());

                    // 不包含limit 则收集所有的start进行批量查询
                    //TODO: 如何精准控制批量查询的数量，防止有超级节点 hang 住
                    //if (!rangeGlobalFlag) {
                        // collect all start
                        long count = 0;
                        while (this.starts.hasNext()) {
                            Traverser.Admin<Vertex> start = this.starts.next();
                            allTraverser.add((Id) start.get().id());
                            count++;
                            if(count >= limit){
                                break;
                            }
                        }
                    //}
                    this.iterator = this.flatMapBatch(allTraverser);
                }
            }
        }
    }



    //    private Iterator<Vertex> vertices(Traverser.Admin<Vertex> traverser) {
//        Iterator<Edge> edges = this.edges(traverser);
//        Iterator<Vertex> vertices = this.queryAdjacentVertices(edges);
//
//        if (LOG.isDebugEnabled()) {
//            Vertex vertex = traverser.get();
//            LOG.debug("HugeVertexStep.vertices(): is there adjacent " +
//                      "vertices of {}: {}, has={}",
//                      vertex.id(), vertices.hasNext(), this.hasContainers);
//        }
//
//        return vertices;
//    }
//
//    private Iterator<Edge> edges(Traverser.Admin<Vertex> traverser) {
//        Query query = this.constructEdgesQuery(traverser);
//        return this.queryEdges(query);
//    }
    //use by HugeVertexStepByBatch
    protected Iterator<Vertex> queryAdjacentVertices(Iterator<Edge> edges) {
        HugeGraph graph = TraversalUtil.getGraph(this);
        Iterator<Vertex> vertices = graph.adjacentVertices(edges);

        if (!this.withVertexCondition()) {
            return vertices;
        }

        // TODO: query by vertex index to optimize
        return TraversalUtil.filterResult(this.hasContainers, vertices);
    }

    protected Iterator<Edge> queryEdges(Query query) {
        HugeGraph graph = TraversalUtil.getGraph(this);

        // Do query
        Iterator<Edge> edges = graph.edges(query);

        if (!this.withEdgeCondition()) {
            return edges;
        }

        // Do filter by edge conditions
        return TraversalUtil.filterResult(this.hasContainers, edges);
    }

    protected ConditionQuery constructEdgesQuery(
                             Traverser.Admin<Vertex> traverser) {
        HugeGraph graph = TraversalUtil.getGraph(this);

        // Query for edge with conditions(else conditions for vertex)
        boolean withEdgeCond = this.withEdgeCondition();
        boolean withVertexCond = this.withVertexCondition();

        Id vertex = (Id) traverser.get().id();
        Directions direction = Directions.convert(this.getDirection());
        Id[] edgeLabels = graph.mapElName2Id(this.getEdgeLabels());

        LOG.debug("HugeVertexStep.edges(): vertex={}, direction={}, " +
                  "edgeLabels={}, has={}",
                  vertex, direction, edgeLabels, this.hasContainers);

        ConditionQuery query = GraphTransaction.constructEdgesQuery(
                               vertex, direction, edgeLabels);
        // Query by sort-keys
        if (withEdgeCond && edgeLabels.length == 1) {
            TraversalUtil.fillConditionQuery(query, this.hasContainers, graph);
            if (!GraphTransaction.matchPartialEdgeSortKeys(query, graph)) {
                // Can't query by sysprop and by index (HugeGraph-749)
                query.resetUserpropConditions();
            } else if (GraphTransaction.matchFullEdgeSortKeys(query, graph)) {
                // All sysprop conditions are in sort-keys
                withEdgeCond = false;
            } else {
                // Partial sysprop conditions are in sort-keys
                assert query.userpropKeys().size() > 0;
            }
        }

        // Query by has(id)
        if (query.idsSize() > 0) {
            // Ignore conditions if query by edge id in has-containers
            // FIXME: should check that the edge id matches the `vertex`
            query.resetConditions();
            LOG.warn("It's not recommended to query by has(id)");
        }

        /*
         * Unset limit when needed to filter property after store query
         * like query: outE().has(k,v).limit(n)
         * NOTE: outE().limit(m).has(k,v).limit(n) will also be unset limit,
         * Can't unset limit if query by paging due to page position will be
         * exceeded when reaching the limit in tinkerpop layer
         */
        if (withEdgeCond || withVertexCond) {
            org.apache.hugegraph.util.E.checkArgument(!this.queryInfo().paging(),
                                                     "Can't query by paging " +
                                                     "and filtering");
            this.queryInfo().limit(Query.NO_LIMIT);
        }

        query = this.injectQueryInfo(query);

        return query;
    }
}
