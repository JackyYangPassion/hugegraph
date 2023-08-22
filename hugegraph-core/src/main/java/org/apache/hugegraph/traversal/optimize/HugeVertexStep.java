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

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.Iterator;
import java.util.List;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;

import com.google.common.collect.Iterators;
import org.apache.hugegraph.HugeGraph;
import org.apache.hugegraph.backend.id.Id;
import org.apache.hugegraph.backend.query.ConditionQuery;
import org.apache.hugegraph.backend.query.Query;
import org.apache.hugegraph.backend.query.QueryResults;
import org.apache.hugegraph.backend.tx.GraphTransaction;
import org.apache.hugegraph.type.define.Directions;
import org.apache.tinkerpop.gremlin.process.traversal.Traverser;
import org.apache.tinkerpop.gremlin.process.traversal.step.map.VertexStep;
import org.apache.tinkerpop.gremlin.process.traversal.step.util.HasContainer;
import org.apache.tinkerpop.gremlin.structure.Edge;
import org.apache.tinkerpop.gremlin.structure.Element;
import org.apache.tinkerpop.gremlin.structure.Vertex;
import org.apache.tinkerpop.gremlin.structure.util.CloseableIterator;
import org.apache.tinkerpop.gremlin.structure.util.StringFactory;
import org.slf4j.Logger;

import org.apache.hugegraph.util.Log;

public class HugeVertexStep<E extends Element>
       extends VertexStep<E> implements QueryHolder {

    private static final long serialVersionUID = -7850636388424382454L;

    private Traverser.Admin<Vertex> head = null;

    private static final Logger LOG = Log.logger(HugeVertexStep.class);

    private final List<HasContainer> hasContainers = new ArrayList<>();

    // Store limit/order-by
    private final Query queryInfo = new Query(null);

    private Iterator<E> iterator = QueryResults.emptyIterator();
    //TODO：通过配置项进行配置线程池大小
    private ExecutorService executorService;

    public HugeVertexStep(final VertexStep<E> originVertexStep,ExecutorService executorService) {
        super(originVertexStep.getTraversal(),
              originVertexStep.getReturnClass(),
              originVertexStep.getDirection(),
              originVertexStep.getEdgeLabels());
        this.executorService = executorService;
        originVertexStep.getLabels().forEach(this::addLabel);
    }

    @SuppressWarnings("unchecked")
    @Override
    protected Iterator<E> flatMap(final Traverser.Admin<Vertex> traverser) {
        boolean queryVertex = this.returnsVertex();
        boolean queryEdge = this.returnsEdge();
        assert queryVertex || queryEdge;
        if (queryVertex) {
            this.iterator = (Iterator<E>) this.vertices(traverser);
        } else {
            assert queryEdge;
            this.iterator = (Iterator<E>) this.edges(traverser);
        }
        return this.iterator;
    }

    private Iterator<Vertex> vertices(Traverser.Admin<Vertex> traverser) {
        Iterator<Edge> edges = this.edges(traverser);
        Iterator<Vertex> vertices = this.queryAdjacentVertices(edges);

        if (LOG.isDebugEnabled()) {
            Vertex vertex = traverser.get();
            LOG.debug("HugeVertexStep.vertices(): is there adjacent " +
                      "vertices of {}: {}, has={}",
                      vertex.id(), vertices.hasNext(), this.hasContainers);
        }

        return vertices;
    }

    private Iterator<Edge> edges(Traverser.Admin<Vertex> traverser) {
        Query query = this.constructEdgesQuery(traverser);
        return this.queryEdges(query);
    }

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

    /**
     * 优化思路：
     * 1. 并发查询数据: 当前实现
     * 2. 批量查询数据
     * @return
     */
    @Override
    protected Traverser.Admin<E> processNextStart(){
        if (!this.iterator.hasNext()) {
            this.closeIterator();
            this.head = this.starts.next();
            this.iterator = this.flatMap(this.head);

            List<Future<Iterator<E>>> futures = new ArrayList<>();
            while (this.starts.hasNext()) {//并发查询 xhop >=2 生效
                Traverser.Admin<Vertex> start = this.starts.next();
                Future<Iterator<E>> its = executorService.submit(() -> {
                    //TODO： 实现批量查询数据
                    Iterator<E> end = this.flatMap(start);
                    return end;
                });
                futures.add(its);
            }

            try{
                LOG.info("HugeVertexStep.processNextStart(): " +
                        "vertices of {}", futures.size());

                for (Future<Iterator<E>> its : futures) {
                    this.iterator = Iterators.concat(this.iterator, its.get());
                }
            } catch (InterruptedException e) {
                throw new RuntimeException(e);
            } catch (ExecutionException e) {
                throw new RuntimeException(e.getCause());
            }
        }

        return this.head.split(this.iterator.next(), this);
    }

}
