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

import java.io.Closeable;
import java.io.IOException;
import java.util.*;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.atomic.AtomicLong;
import java.util.stream.Collectors;

import org.apache.commons.collections.CollectionUtils;
import org.apache.commons.collections.list.SynchronizedList;
import org.apache.hugegraph.HugeException;
import org.apache.hugegraph.HugeGraph;
import org.apache.hugegraph.backend.id.EdgeId;
import org.apache.hugegraph.backend.id.Id;
import org.apache.hugegraph.backend.query.Aggregate;
import org.apache.hugegraph.backend.query.Condition;
import org.apache.hugegraph.backend.query.ConditionQuery;
import org.apache.hugegraph.backend.query.EdgesQueryIterator;
import org.apache.hugegraph.backend.query.Query;
import org.apache.hugegraph.backend.tx.GraphTransaction;
import org.apache.hugegraph.config.CoreOptions;
import org.apache.hugegraph.exception.ErrorCode;
import org.apache.hugegraph.exception.NotFoundException;
import org.apache.hugegraph.iterator.CIter;
import org.apache.hugegraph.iterator.FilterIterator;
import org.apache.hugegraph.iterator.MapperIterator;
import org.apache.hugegraph.iterator.WrappedIterator;
import org.apache.hugegraph.perf.PerfUtil.Watched;
import org.apache.hugegraph.schema.SchemaLabel;
import org.apache.hugegraph.structure.HugeEdge;
import org.apache.hugegraph.structure.HugeVertex;
import org.apache.hugegraph.traversal.algorithm.steps.EdgeStep;
import org.apache.hugegraph.traversal.algorithm.steps.Steps;
import org.apache.hugegraph.traversal.optimize.TraversalUtil;
import org.apache.hugegraph.type.HugeType;
import org.apache.hugegraph.type.define.CollectionType;
import org.apache.hugegraph.type.define.Directions;
import org.apache.hugegraph.type.define.HugeKeys;
import org.apache.hugegraph.util.CollectionUtil;
import org.apache.hugegraph.util.E;
import org.apache.hugegraph.util.InsertionOrderUtil;
import org.apache.hugegraph.util.Log;
import org.apache.hugegraph.util.collection.CollectionFactory;
import org.apache.hugegraph.util.collection.JniIdSet;
import org.apache.hugegraph.util.collection.ObjectIntMapping;
import org.apache.hugegraph.util.collection.ObjectIntMappingFactory;
import org.apache.tinkerpop.gremlin.structure.Edge;
import org.apache.tinkerpop.gremlin.structure.util.CloseableIterator;
import org.slf4j.Logger;

import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;

import jakarta.ws.rs.core.MultivaluedHashMap;
import jakarta.ws.rs.core.MultivaluedMap;

public class HugeTraverser {

    public static final Logger LOG = Log.logger(HugeTraverser.class);
    public static final String DEFAULT_CAPACITY = "10000000";
    public static final String DEFAULT_ELEMENTS_LIMIT = "10000000";
    public static final String DEFAULT_PATHS_LIMIT = "10";
    public static final String DEFAULT_LIMIT = "100";
    public static final String DEFAULT_MAX_DEGREE = "10000";
    public static final String DEFAULT_SKIP_DEGREE = "100000";
    public static final String DEFAULT_SAMPLE = "100";
    public static final String DEFAULT_MAX_DEPTH = "50";
    public static final String DEFAULT_WEIGHT = "0";
    public static final long DEF_CAPACITY = Long.parseLong(DEFAULT_CAPACITY);
    public static final long DEF_SKIP_DEGREE = Long.parseLong(DEFAULT_SKIP_DEGREE);
    public static final long DEF_MAX_DEGREE = Long.parseLong(DEFAULT_MAX_DEGREE);
    // Empirical value of scan limit, with which results can be returned in 3s
    public static final String DEFAULT_PAGE_LIMIT = "100000";
    public static final long NO_LIMIT = -1L;
    public static final long SKIP_DEGREE_NO_LIMIT = 0L;
    public static final String ALGORITHMS_BREADTH_FIRST = "breadth_first";
    public static final String ALGORITHMS_DEEP_FIRST = "deep_first";
    protected static final int MAX_VERTICES = 10;
    private static CollectionFactory collectionFactory;
    private final HugeGraph graph;
    // for debugMeasure
    public AtomicLong edgeIterCounter = new AtomicLong(0);
    public AtomicLong vertexIterCounter = new AtomicLong(0);

    public HugeTraverser(HugeGraph graph) {
        this.graph = graph;
        if (collectionFactory == null) {
            synchronized (HugeTraverser.class) {
                if (collectionFactory == null) {
                    collectionFactory = new CollectionFactory(this.collectionType());
                }
            }
        }
    }

    public static void checkDegree(long degree) {
        checkPositiveOrNoLimit(degree, "max degree");
    }

    public static void checkCapacity(long capacity) {
        checkPositiveOrNoLimit(capacity, "capacity");
    }

    public static void checkLimit(long limit) {
        checkPositiveOrNoLimit(limit, "limit");
    }

    public static void checkPositive(long value, String name) {
        E.checkArgument(value > 0,
            "The %s parameter must be > 0, but got %s",
            name, value);
    }

    public static void checkPositiveOrNoLimit(long value, String name) {
        E.checkArgument(value > 0L || value == NO_LIMIT,
            "The %s parameter must be > 0 or == %s, but got: %s",
            name, NO_LIMIT, value);
    }

    public static void checkNonNegative(long value, String name) {
        E.checkArgument(value >= 0L,
            "The %s parameter must be >= 0, but got: %s",
            name, value);
    }

    public static void checkNonNegativeOrNoLimit(long value, String name) {
        E.checkArgument(value >= 0L || value == NO_LIMIT,
            "The %s parameter must be >= 0 or == %s, but got: %s",
            name, NO_LIMIT, value);
    }

    public static void checkCapacity(long capacity, long access,
                                     String traverse) {
        if (capacity != NO_LIMIT && access > capacity) {
            throw new HugeException(ErrorCode.EXCEED_CAPACITY_WHILE_FINDING,
                capacity, traverse);
        }
    }

    public static void checkSkipDegree(long skipDegree, long degree,
                                       long capacity) {
        E.checkArgument(skipDegree >= 0L &&
                skipDegree <= DEF_CAPACITY,
            "The skipped degree must be in [0, %s], but got '%s'",
            DEF_CAPACITY, skipDegree);
        if (capacity != NO_LIMIT) {
            E.checkArgument(degree != NO_LIMIT && degree < capacity,
                ErrorCode.PARAM_SMALLER.format("max degree",
                    "capacity"));
            E.checkArgument(skipDegree < capacity,
                ErrorCode.PARAM_SMALLER.format("skipped degree",
                    "capacity"));
        }
        if (skipDegree > 0L) {
            String message = ErrorCode.PARAM_GREATER.format(
                "skipped degree", "max degree", skipDegree, degree);
            E.checkArgument(degree != NO_LIMIT && skipDegree >= degree,
                message);
        }
    }

    public static void checkAlgorithm(String algorithm) {
        E.checkArgument(algorithm.compareToIgnoreCase(ALGORITHMS_BREADTH_FIRST) == 0 ||
                algorithm.compareToIgnoreCase(ALGORITHMS_DEEP_FIRST) == 0,
            "The algorithm must be one of '%s' or '%s', but got '%s'",
            ALGORITHMS_BREADTH_FIRST, ALGORITHMS_DEEP_FIRST, algorithm);
    }

    public static boolean isDeepFirstAlgorithm(String algorithm) {
        return algorithm.compareToIgnoreCase(ALGORITHMS_DEEP_FIRST) == 0;
    }

    public static <K, V extends Comparable<? super V>> Map<K, V> topN(
        Map<K, V> map,
        boolean sorted,
        long limit) {
        if (sorted) {
            map = CollectionUtil.sortByValue(map, false);
        }
        if (limit == NO_LIMIT || map.size() <= limit) {
            return map;
        }
        Map<K, V> results = InsertionOrderUtil.newMap();
        long count = 0L;
        for (Map.Entry<K, V> entry : map.entrySet()) {
            results.put(entry.getKey(), entry.getValue());
            if (++count >= limit) {
                break;
            }
        }
        return results;
    }

    public static <K, V extends Comparable<? super V>> Map<K, V> sortMap(
        Map<K, V> map,
        boolean sorted) {
        if (sorted) {
            map = CollectionUtil.sortByValue(map, false);
        }
        return map;
    }

    /**
     * 利用小顶堆取topN
     * 使用优先级队列维护limit个元素, 和输入流输入优先队列
     *
     * @param map
     * @param sorted
     * @param limit
     * @return
     */
    public static <K, V extends Comparable<? super V>> Map<K, V> topNByPriorityQueue(
        Map<K, V> map, boolean sorted, long limit) {
        if (sorted) {
            // 优先级队列
            PriorityQueue<Map.Entry<K, V>> entryPriorityQueue =
                new PriorityQueue<>((int) limit,
            new Comparator<Map.Entry<K, V>>() {
                        @Override
                        public int compare(
                            Map.Entry<K, V> o1,
                            Map.Entry<K, V> o2) {
                            return o1.getValue().compareTo(o2.getValue());
                        }
                    });

            int limitCount = 0;
            Set<Map.Entry<K, V>> entries = map.entrySet();
            for (Map.Entry<K, V> entry : entries) {
                if (limitCount < limit) {
                    entryPriorityQueue.add(entry);
                    limitCount++;
                    continue;
                }
                if (entry.getValue().compareTo(entryPriorityQueue.peek().getValue()) >= 0) {
                    entryPriorityQueue.add(entry);
                    entryPriorityQueue.poll();
                }
            }

            List<Map.Entry<K, V>> tmpListMap = new ArrayList<>((int) limit);
            while (!entryPriorityQueue.isEmpty()) {
                tmpListMap.add(entryPriorityQueue.poll());
            }

            Map<K, V> mapTmp = InsertionOrderUtil.newMap();
            for (int j = tmpListMap.size() - 1; j >= 0; j--) {
                mapTmp.put(tmpListMap.get(j).getKey(), tmpListMap.get(j).getValue());
            }
            map = mapTmp;
        }

        if (limit == NO_LIMIT || map.size() <= limit) {
            return map;
        }

        return map;
    }

    protected static Set<Id> newIdSet() {
        return collectionFactory.newIdSet();
    }

    protected static Set<Id> newJniIdSet() {
        return new JniIdSet();
    }

    protected static Set<Id> newJniSet(Collection<Id> collection) {
        Set<Id> idSet = new JniIdSet();
        idSet.addAll(collection);
        return idSet;
    }

    public static void closeSet(Collection<?> set) {
        if (set instanceof Closeable) {
            try {
                ((Closeable) set).close();
            } catch (IOException e) {
                LOG.error("exception ", e);
            }
        }
    }

    protected static <V> Set<V> newSet() {
        return newSet(false);
    }

    protected static <V> Set<V> newSet(boolean concurrent) {
        if (concurrent) {
            return ConcurrentHashMap.newKeySet();
        } else {
            return collectionFactory.newSet();
        }
    }

    protected static <V> Set<V> newSet(int initialCapacity) {
        return collectionFactory.newSet(initialCapacity);
    }

    protected static <V> Set<V> newSet(Collection<V> collection) {
        return collectionFactory.newSet(collection);
    }

    protected static <V> List<V> newList() {
        return collectionFactory.newList();
    }

    protected static <V> List<V> newList(boolean concurrent) {
        if (concurrent) {
            return SynchronizedList.decorate(newList());
        } else {
            return newList();
        }
    }

    protected static <V> List<V> newList(int initialCapacity) {
        return collectionFactory.newList(initialCapacity);
    }

    protected static <V> List<V> newList(Collection<V> collection) {
        return collectionFactory.newList(collection);
    }

    protected static <K, V> Map<K, V> newMap() {
        return collectionFactory.newMap();
    }

    protected static <K, V> Map<K, V> newMap(int initialCapacity) {
        return collectionFactory.newMap(initialCapacity);
    }

    protected static <K, V> MultivaluedMap<K, V> newMultivalueMap() {
        return new MultivaluedHashMap<>();
    }

    protected static List<Id> joinPath(Node prev, Node back, boolean ring) {
        // Get self path
        List<Id> path = prev.path();

        // Get reversed other path
        List<Id> backPath = back.path();
        Collections.reverse(backPath);

        if (!ring) {
            // Avoid loop in path
            if (CollectionUtils.containsAny(path, backPath)) {
                return ImmutableList.of();
            }
        }

        // Append other path behind self path
        path.addAll(backPath);
        return path;
    }

    public static List<HugeEdge> getPathEdges(Iterator<Edge> it,
                                              HugeEdge edge) {
        ArrayList<HugeEdge> edges = new ArrayList<>();
        if (it instanceof NestedIterator) {
            ((NestedIterator) it).getPathEdges(edges);
        }
        edges.add(edge);
        return edges;
    }

    public HugeGraph graph() {
        return this.graph;
    }

    protected int concurrentDepth() {
        return this.graph.option(CoreOptions.OLTP_CONCURRENT_DEPTH);
    }

    private CollectionType collectionType() {
        return this.graph.option(CoreOptions.OLTP_COLLECTION_TYPE);
    }

    protected Iterator<Id> adjacentVertices(Id source, Directions dir,
                                            Id label, long limit) {
        Iterator<EdgeId> edges = this.edgeIdsOfVertex(source, dir, label, limit, NO_LIMIT);
        return new MapperIterator<>(edges, EdgeId::otherVertexId);
    }

    protected Iterator<Id> adjacentVertices(Id source, Directions dir,
                                            List<Id> labels, long limit) {
        Iterator<EdgeId> edges = this.edgeIdsOfVertex(source, dir, labels, limit, NO_LIMIT);
        return new MapperIterator<>(edges, EdgeId::otherVertexId);
    }

    protected Set<Id> adjacentVertices(Id source, EdgeStep step) {
        Set<Id> neighbors = newSet();
        Iterator<EdgeId> edges = this.edgeIdsOfVertex(source, step);
        try {
            while (edges.hasNext()) {
                neighbors.add(edges.next().otherVertexId());
            }
        } finally {
            CloseableIterator.closeIterator(edges);
        }
        return neighbors;
    }

    protected int vertexDegree(Id source, Directions dir, Id label) {
        Query query = GraphTransaction.constructEdgesQuery(source, dir, label);
        query.aggregate(Aggregate.AggregateFunc.COUNT, null);
        long degree = graph().queryNumber(query).longValue();
        this.edgeIterCounter.addAndGet(degree);
        this.vertexIterCounter.incrementAndGet();
        return (int) degree;
//        return Iterators.size(this.edgeIdsOfVertex(source, dir, label, NO_LIMIT, NO_LIMIT));
    }

    protected boolean isSuperNode(Id source, Directions dir, long skipDegree) {
        if (skipDegree <= 0) {
            return false;
        }
        Query query = GraphTransaction.constructEdgesQuery(source, dir);
        query.aggregate(Aggregate.AggregateFunc.COUNT, null);
        long degree = graph().queryNumber(query).longValue();
        return degree > skipDegree;
    }

    @Watched
    protected CIter<EdgeId> edgeIdsOfVertex(Id source, Directions dir,
                                            Id label, long limit, long skipDegree) {
        Id[] labels = label != null ? new Id[]{label} : null;
        return edgeIdsOfVertex(source, dir, labels, limit, skipDegree);
    }

    @Watched
    protected CIter<EdgeId> edgeIdsOfVertex(Id source, Directions dir,
                                            List<Id> label, long limit,
                                            long skipDegree) {
        Id[] labels = label == null || label.size() == 0 ? null :
            label.toArray(new Id[0]);
        return edgeIdsOfVertex(source, dir, labels, limit, skipDegree);
    }

    @Watched
    protected CIter<EdgeId> edgeIdsOfVertex(Id source, Directions dir,
                                            Id[] labels, long degree, long skipDegree) {
        Query query = GraphTransaction.constructEdgesQuery(source, dir, labels);
        if (degree != NO_LIMIT) {
            query.limit(degree);
        }
        if (skipDegree != SKIP_DEGREE_NO_LIMIT) {
            query.skipDegree(skipDegree);
        }
        return this.graph.edgeIds(query);
    }

    @Watched
    protected Iterator<Edge> edgesOfVertex(Id source, Directions dir,
                                           Id label, long limit) {
        Id[] labels = {};
        if (label != null) {
            labels = new Id[]{label};
        }

        Query query = GraphTransaction.constructEdgesQuery(source, dir, labels);
        if (limit != NO_LIMIT) {
            query.limit(limit);
        }
        return this.graph.edges(query);
    }

    protected Iterator<EdgeId> edgeIdsOfVertex(Id source, EdgeStep edgeStep) {
        if (edgeStep.properties() == null || edgeStep.properties().isEmpty()) {
            return this.edgeIdsOfVertex(source,
                edgeStep.direction(),
                edgeStep.labels().keySet().toArray(new Id[0]),
                edgeStep.limit(),
                edgeStep.skipDegree());
        }

        Id[] edgeLabels = edgeStep.edgeLabels();
        Query query = GraphTransaction.constructEdgesQuery(source,
            edgeStep.direction(),
            edgeLabels);
        ConditionQuery filter = (ConditionQuery) query.copy();
        // TODO ConditionQuery operator sinking
        this.fillFilterByProperties(filter, edgeStep.properties());
        query.capacity(Query.NO_CAPACITY);
        if (edgeStep.limit() != NO_LIMIT) {
            query.limit(edgeStep.limit());
        }
        if (edgeStep.skipDegree() != NO_LIMIT) {
            query.skipDegree(edgeStep.skipDegree());
        }
        Iterator<Edge> edges = this.graph().edges(query);
        edges = new FilterIterator<>(edges, (e) -> filter.test((HugeEdge) e));
        return new MapperIterator<>(edges, (e) -> ((HugeEdge) e).id());
    }

    @Watched
    protected Iterator<Edge> edgesOfVertex(Id source, Directions dir,
                                           Id label, long limit, long skipDegree,
                                           boolean withEdgeProperties) {
        Id[] labels = label != null ? new Id[]{label} : null;
        return edgesOfVertex(source, dir, labels, limit, skipDegree, withEdgeProperties);
    }

    protected Iterator<Edge> edgesOfVertex(Id source, Directions dir,
                                           Id[] labels, long limit, long skipDegree,
                                           boolean withEdgeProperties) {
        Query query = GraphTransaction.constructEdgesQuery(source, dir, labels);
        if (limit != NO_LIMIT) {
            query.limit(limit);
        }
        if (skipDegree != SKIP_DEGREE_NO_LIMIT) {
            query.skipDegree(skipDegree);
        }
        query.withProperties(withEdgeProperties);
        return this.graph.edges(query);
    }

    @Watched
    protected EdgeIdsIterator edgeIdsOfVertices(Iterator<Id> sources,
                                                Directions dir,
                                                Id label,
                                                long degree,
                                                long skipDegree,
                                                Query.OrderType orderType) {
        return new EdgeIdsIterator(new EdgesQueryIterator(sources, dir, label,
            degree, skipDegree, false, orderType));
    }

    @Watched
    protected EdgeIdsIterator edgeIdsOfVertices(Iterator<Id> sources,
                                                Directions dir,
                                                List<Id> labelIds,
                                                long degree,
                                                long skipDegree,
                                                Query.OrderType orderType) {
        return new EdgeIdsIterator(new EdgesQueryIterator(sources, dir, labelIds,
            degree, skipDegree, false, orderType));
    }

    protected Iterator<Edge> edgesOfVertexStep(Iterator<Edge> edges,
                                               Steps steps) {
        if (checkParamsNull(edges, steps)) {
            return edges;
        }

        Iterator<Edge> result = edges;
        if (!steps.isVertexEmpty()) {
            // Edge & Vertex Step are not empty
            Map<Id, ConditionQuery> vConditions = getElementFilterQuery(steps.vertexSteps(), HugeType.VERTEX);

            result = new FilterIterator<>(result,
                edge -> validateVertex(vConditions, (HugeEdge) edge));
        }

        return result;
    }

    private Boolean validateVertex(Map<Id, ConditionQuery> vConditions,
                                   HugeEdge edge) {
        HugeVertex sVertex = edge.sourceVertex();
        HugeVertex tVertex = edge.targetVertex();
        if (!vConditions.containsKey(sVertex.schemaLabel().id()) ||
            !vConditions.containsKey(tVertex.schemaLabel().id())) {
            return false;
        }

        ConditionQuery cq = vConditions.get(sVertex.schemaLabel().id());
        if (cq != null) {
            sVertex = (HugeVertex) this.graph.vertex(sVertex.id());
            if (!cq.test(sVertex)) {
                return false;
            }
        }

        cq = vConditions.get(tVertex.schemaLabel().id());
        if (cq != null) {
            tVertex = (HugeVertex) this.graph.vertex(tVertex.id());
            return cq.test(tVertex);
        }
        return true;
    }

    private Boolean validateEdge(Map<Id, ConditionQuery> eConditions,
                                 HugeEdge edge) {
        if (!eConditions.containsKey(edge.schemaLabel().id())) {
            return false;
        }

        ConditionQuery cq = eConditions.get(edge.schemaLabel().id());
        if (cq != null) {
            return cq.test(edge);
        }
        return true;
    }

    private Map<Id, ConditionQuery> getElementFilterQuery(
        Map<Id, Steps.StepEntity> idStepEntityMap, HugeType vertex) {
        Map<Id, ConditionQuery> vertexConditions = new HashMap<>();
        for (Map.Entry<Id, Steps.StepEntity> entry :
            idStepEntityMap.entrySet()) {
            Steps.StepEntity stepEntity = entry.getValue();
            if (stepEntity.getProperties() != null &&
                !stepEntity.getProperties().isEmpty()) {
                ConditionQuery cq = new ConditionQuery(vertex);
                Map<Id, Object> props = stepEntity.getProperties();
                TraversalUtil.fillConditionQuery(cq, props, this.graph);
                vertexConditions.put(entry.getKey(), cq);
            } else {
                vertexConditions.put(entry.getKey(), null);
            }
        }
        return vertexConditions;
    }

    protected Condition getConditionWithStepsForQuery(
        Map<Id, Steps.StepEntity> edgeStepEntityMap,
        Map<Id, Steps.StepEntity> vertexStepEntityMap) {
        Condition unionCondition = null;
        if (edgeStepEntityMap != null && !edgeStepEntityMap.isEmpty()) {
            for (Map.Entry<Id, Steps.StepEntity> entry :
                edgeStepEntityMap.entrySet()) {
                Steps.StepEntity stepEntity = entry.getValue();
                // Step forms a condition containing label and properties
                Condition edgeCondition = Condition.eq(HugeKeys.LABEL,
                    entry.getKey());
                if (stepEntity.getProperties() != null &&
                    !stepEntity.getProperties().isEmpty()) {
                    Map<Id, Object> props = stepEntity.getProperties();
                    edgeCondition = TraversalUtil.fillIdPropCondition(edgeCondition,
                        props,
                        this.graph);
                }

                if (unionCondition == null) {
                    unionCondition = edgeCondition;
                } else {
                    unionCondition = unionCondition.or(edgeCondition);
                }
            }
        }

        return unionCondition;
    }

    private boolean checkParamsNull(Iterator<Edge> edges, Steps s) {

        //edges.hasNext() 比较耗时 直接从底层查询了数据
        if (edges == null || s == null || (!edges.hasNext())) {
            return true;
        }
        // Both are empty
        return (s.edgeSteps() == null || s.isEdgeStepPropertiesEmpty()) &&
            (s.vertexSteps() == null || s.vertexSteps().isEmpty());
    }

    protected Iterator<Edge> edgesOfVertexAF(Id source, Steps steps,
                                             boolean withEdgeProperties) {

        List<Id> edgeLabels = steps.edgeLabels();
        ConditionQuery cq = GraphTransaction.constructEdgesQuery(source, steps.direction(), edgeLabels);
        cq.capacity(Query.NO_CAPACITY);
        if (steps.limit() != NO_LIMIT) {
            cq.limit(steps.limit());
        }

        // check if we need edgeProperties.
        cq.withProperties(withEdgeProperties || !steps.isEdgeStepPropertiesEmpty());

        if (!steps.isEdgeStepPropertiesEmpty() || !steps.isVertexEmpty()) {
            Condition eCondition =
                getConditionWithStepsForQuery(steps.edgeSteps(), steps.vertexSteps());
            cq.query(eCondition);
        }
        Iterator<Edge> edges = this.graph().edges(cq);
        return edgesOfVertexStep(edges, steps);
    }

    private void fillFilterBySortKeys(Query query, Id[] edgeLabels,
                                      Map<Id, Object> properties) {
        if (properties == null || properties.isEmpty()) {
            return;
        }

        E.checkArgument(edgeLabels.length == 1,
            "The properties filter condition can be set " +
                "only if just set one edge label");

        this.fillFilterByProperties(query, properties);

        ConditionQuery condQuery = (ConditionQuery) query;
        if (!GraphTransaction.matchFullEdgeSortKeys(condQuery, this.graph())) {
            Id label = condQuery.condition(HugeKeys.LABEL);
            E.checkArgument(false, "The properties %s does not match " +
                    "sort keys of edge label '%s'",
                this.graph().mapPkId2Name(properties.keySet()),
                this.graph().edgeLabel(label).name());
        }
    }

    private void fillFilterByProperties(Query query,
                                        Map<Id, Object> properties) {
        if (properties == null || properties.isEmpty()) {
            return;
        }

        ConditionQuery condQuery = (ConditionQuery) query;
        TraversalUtil.fillConditionQuery(condQuery, properties, this.graph);
    }

    protected long edgesCount(Id source, EdgeStep edgeStep) {
        Id[] edgeLabels = edgeStep.edgeLabels();
        Query query = GraphTransaction.constructEdgesQuery(source,
            edgeStep.direction(),
            edgeLabels);
        this.fillFilterBySortKeys(query, edgeLabels, edgeStep.properties());
        query.aggregate(Aggregate.AggregateFunc.COUNT, null);
        query.capacity(Query.NO_CAPACITY);
        query.limit(Query.NO_LIMIT);
        long count = graph().queryNumber(query).longValue();
        if (edgeStep.degree() == NO_LIMIT || count < edgeStep.degree()) {
            return count;
        } else if (edgeStep.skipDegree() != 0L &&
            count >= edgeStep.skipDegree()) {
            return 0L;
        } else {
            return edgeStep.degree();
        }
    }

    protected Object getVertexLabelId(Object label) {
        if (label == null) {
            return null;
        }
        return SchemaLabel.getLabelId(this.graph, HugeType.VERTEX, label);
    }

    protected Id getEdgeLabelId(Object label) {
        if (label == null) {
            return null;
        }
        return SchemaLabel.getLabelId(this.graph, HugeType.EDGE, label);
    }

    protected void checkVertexExist(Id vertexId, String name) {
        try {
            this.graph.vertex(vertexId);
        } catch (NotFoundException e) {
            throw new IllegalArgumentException(
                ErrorCode.VERTEX_ID_NOT_EXIST.format(name, vertexId), e);
        }
    }

    public Iterator<Edge> createNestedIterator(Id sourceV, Steps steps, int depth,
                                               Set<Id> visited,
                                               boolean withEdgeProperties) {
        E.checkArgument(depth > 0,
            "The depth should large than 0 for nested iterator");

        visited.add(sourceV);

        // build a chained iterator path with length of depth
        Iterator<Edge> it = this.edgesOfVertexAF(sourceV, steps, withEdgeProperties);
        for (int i = 1; i < depth; i++) {
            it = new NestedIterator(this, it, steps, visited, withEdgeProperties);
        }
        return it;
    }

    public static class Node {

        private final Id id;
        private final Node parent;

        public Node(Id id) {
            this(id, null);
        }

        public Node(Id id, Node parent) {
            E.checkArgumentNotNull(id, "Id of Node can't be null");
            this.id = id;
            this.parent = parent;
        }

        public Id id() {
            return this.id;
        }

        public Node parent() {
            return this.parent;
        }

        public List<Id> path() {
            List<Id> ids = newList();
            Node current = this;
            do {
                ids.add(current.id);
                current = current.parent;
            } while (current != null);
            Collections.reverse(ids);
            return ids;
        }

        public List<Id> joinPath(Node back) {
            return HugeTraverser.joinPath(this, back, false);
        }

        public boolean contains(Id id) {
            Node node = this;
            do {
                if (node.id.equals(id)) {
                    return true;
                }
                node = node.parent;
            } while (node != null);
            return false;
        }

        @Override
        public int hashCode() {
            return this.id.hashCode();
        }

        @Override
        public boolean equals(Object object) {
            if (!(object instanceof Node)) {
                return false;
            }
            Node other = (Node) object;
            return Objects.equals(this.id, other.id) &&
                Objects.equals(this.parent, other.parent);
        }

        @Override
        public String toString() {
            return this.id.toString();
        }
    }

    public static class Path {

        public static final Path EMPTY_PATH = new Path(ImmutableList.of());

        private final Id crosspoint;
        private final List<Id> vertices;

        public Path(List<Id> vertices) {
            this(null, vertices);
        }

        public Path(Id crosspoint, List<Id> vertices) {
            this.crosspoint = crosspoint;
            this.vertices = vertices;
        }

        public Id crosspoint() {
            return this.crosspoint;
        }

        public void addToLast(Id id) {
            this.vertices.add(id);
        }

        public List<Id> vertices() {
            return this.vertices;
        }

        public void reverse() {
            Collections.reverse(this.vertices);
        }

        public Map<String, Object> toMap(boolean withCrossPoint) {
            if (withCrossPoint) {
                return ImmutableMap.of("crosspoint", this.crosspoint,
                    "objects", this.vertices);
            } else {
                return ImmutableMap.of("objects", this.vertices);
            }
        }

        public boolean ownedBy(Id source) {
            E.checkNotNull(source, "source");
            Id min = null;
            for (Id id : this.vertices) {
                if (min == null || id.compareTo(min) < 0) {
                    min = id;
                }
            }
            return source.equals(min);
        }

        @Override
        public int hashCode() {
            return this.vertices.hashCode();
        }

        /**
         * Compares the specified object with this path for equality.
         * Returns <tt>true</tt> if and only if both have same vertices list
         * without regard of crosspoint.
         *
         * @param other the object to be compared for equality with this path
         * @return <tt>true</tt> if the specified object is equal to this path
         */
        @Override
        public boolean equals(Object other) {
            if (!(other instanceof Path)) {
                return false;
            }
            return this.vertices.equals(((Path) other).vertices);
        }
    }

    public static class PathSet implements Set<Path> {

        private static final long serialVersionUID = -8237531948776524872L;

        private final Set<Path> paths;

        public PathSet() {
            this(false);
        }

        public PathSet(boolean concurrent) {
            paths = newSet(concurrent);
        }

        @Override
        public boolean add(Path path) {
            return this.paths.add(path);
        }

        @Override
        public boolean remove(Object o) {
            return this.paths.remove(o);
        }

        @Override
        public boolean containsAll(Collection<?> c) {
            return this.paths.containsAll(c);
        }

        @Override
        public boolean addAll(Collection<? extends Path> c) {
            return this.paths.addAll(c);
        }

        @Override
        public boolean retainAll(Collection<?> c) {
            return this.paths.retainAll(c);
        }

        @Override
        public boolean removeAll(Collection<?> c) {
            return this.paths.removeAll(c);
        }

        @Override
        public void clear() {
            this.paths.clear();
        }

        @Override
        public boolean isEmpty() {
            return this.paths.isEmpty();
        }

        @Override
        public boolean contains(Object o) {
            return this.paths.contains(o);
        }

        @Override
        public int size() {
            return this.paths.size();
        }

        @Override
        public Iterator<Path> iterator() {
            return this.paths.iterator();
        }

        @Override
        public Object[] toArray() {
            return this.paths.toArray();
        }

        @Override
        public <T> T[] toArray(T[] a) {
            return this.paths.toArray(a);
        }

        public boolean addAll(PathSet paths) {
            return this.paths.addAll(paths.paths);
        }

        public Set<Id> vertices() {
            Set<Id> vertices = newIdSet();
            for (Path path : this.paths) {
                vertices.addAll(path.vertices());
            }
            return vertices;
        }

        public void append(Id current) {
            for (Iterator<Path> iter = this.paths.iterator(); iter.hasNext(); ) {
                Path path = iter.next();
                if (path.vertices().contains(current)) {
                    iter.remove();
                    continue;
                }
                path.addToLast(current);
            }
        }
    }

    public static class NestedIterator extends WrappedIterator<Edge> {

        private static final int MAX_CACHED_COUNT = 1000;

        // skip de-dup for vertices those exceed this limit
        private static final int MAX_VISITED_COUNT = 1000000;

        private final Iterator<Edge> parentIterator;
        private final HugeTraverser traverser;
        private final Steps steps;

        // visited vertex-ids of all parent-tree,
        // used to exclude visited vertex or other purpose
        private final Set<Id> visited;

        // cache for edges, initial capacity to avoid mem-fragment
        private final ArrayList<HugeEdge> cache = new ArrayList<>(MAX_CACHED_COUNT);
        private final boolean withEdgeProperties;
        private int cachePointer = 0;
        // of parent
        private HugeEdge currentEdge = null;
        private Iterator<Edge> currentIterator = null;

        // todo: may add edge-filter and/or vertex-filter ...

        public NestedIterator(HugeTraverser traverser, Iterator<Edge> parent,
                              Steps steps, Set<Id> visited,
                              boolean withEdgeProperties) {
            this.traverser = traverser;
            this.parentIterator = parent;
            this.steps = steps;
            this.visited = visited;
            this.withEdgeProperties = withEdgeProperties;
        }

        @Override
        public boolean hasNext() {
            if (currentIterator == null || !currentIterator.hasNext()) {
                return fetch();
            }
            return true;
        }

        @Override
        public Edge next() {
            return currentIterator.next();
        }

        @Override
        protected Iterator<?> originIterator() {
            return parentIterator;
        }

        @Override
        public void close() throws Exception {
            if (currentIterator != null) {
                CloseableIterator.closeIterator(currentIterator);
            }
            super.close();
        }

        @Override
        protected boolean fetch() {
            while (this.currentIterator == null ||
                !this.currentIterator.hasNext()) {
                if (currentIterator != null) {
                    CloseableIterator.closeIterator(currentIterator);
                    currentIterator = null;
                }
                if (this.cache.size() == this.cachePointer &&
                    !this.fillCache()) {
                    return false;
                }
                this.currentEdge = this.cache.get(this.cachePointer);
                this.cachePointer++;
                this.currentIterator = traverser.edgesOfVertexAF(
                    this.currentEdge.id().otherVertexId(),
                    steps, withEdgeProperties);
            }
            return true;
        }

        protected boolean fillCache() {
            this.cache.clear();
            this.cachePointer = 0;
            // fill cache from parent
            while (this.parentIterator.hasNext() &&
                this.cache.size() < MAX_CACHED_COUNT) {
                HugeEdge e = (HugeEdge) parentIterator.next();
                traverser.edgeIterCounter.incrementAndGet();
                Id vid = e.id().otherVertexId();
                if (!visited.contains(vid)) {
                    this.cache.add(e);
                    if (visited.size() < MAX_VISITED_COUNT) {
                        // skip over limit visited vertices.
                        this.visited.add(vid);
                    }
                }
            }
            return this.cache.size() > 0;
        }

        public List<HugeEdge> getPathEdges(List<HugeEdge> edges) {
            if (parentIterator instanceof NestedIterator) {
                NestedIterator parent = (NestedIterator) this.parentIterator;
                parent.getPathEdges(edges);
            }
            edges.add(currentEdge);
            return edges;
        }
    }


    public static class EdgeRecord {
        private final Map<Long, Edge> edgeMap;
        private final ObjectIntMapping<Id> idMapping;

        public EdgeRecord(boolean concurrent) {
            this.edgeMap = new HashMap<>();
            this.idMapping = ObjectIntMappingFactory.newObjectIntMapping(concurrent);
        }

        private static Long makeVertexPairIndex(int source, int target) {
            return ((long) source & 0xFFFFFFFFL) |
                (((long) target << 32) & 0xFFFFFFFF00000000L);
        }

        public static Set<Id> getEdgeIds(Set<Edge> edges) {
            return edges.stream().map(edge -> ((HugeEdge) edge).id()).collect(Collectors.toSet());
        }

        private int code(Id id) {
            if (id.number()) {
                long l = id.asLong();
                if (0 <= l && l <= Integer.MAX_VALUE) {
                    return (int) l;
                }
            }
            int code = this.idMapping.object2Code(id);
            assert code > 0;
            return -code;
        }

        public void addEdge(Id source, Id target, Edge edge) {
            Long index = makeVertexPairIndex(this.code(source), this.code(target));
            this.edgeMap.put(index, edge);
        }

        private Edge getEdge(Id source, Id target) {
            Long index = makeVertexPairIndex(this.code(source), this.code(target));
            return this.edgeMap.get(index);
        }

        public Set<Edge> getEdges(HugeTraverser.Path path) {
            if (path == null || path.vertices().isEmpty()) {
                return new HashSet<>();
            }
            Iterator<Id> vertexIter = path.vertices().iterator();
            return getEdges(vertexIter);
        }

        public Set<Edge> getEdges(Collection<HugeTraverser.Path> paths) {
            Set<Edge> edgeIds = new HashSet<>();
            for (HugeTraverser.Path path : paths) {
                edgeIds.addAll(getEdges(path));
            }
            return edgeIds;
        }

        public Set<Edge> getEdges(Iterator<Id> vertexIter) {
            Set<Edge> edges = new HashSet<>();
            Id first = vertexIter.next();
            Id second;
            while (vertexIter.hasNext()) {
                second = vertexIter.next();
                Edge edge = getEdge(first, second);
                if (edge == null) {
                    edge = getEdge(second, first);
                }
                if (edge != null) {
                    edges.add(edge);
                }
                first = second;
            }
            return edges;
        }

    }




    public class EdgeIdsIterator implements Iterator<CIter<EdgeId>>, Closeable {
        private final Iterator<CIter<EdgeId>> currentIt;

        public EdgeIdsIterator(EdgesQueryIterator queryIterator) {
            this.currentIt = graph().edgeIds(queryIterator);
        }

        @Override
        public boolean hasNext() {
            return this.currentIt.hasNext();
        }

        @Override
        public CIter<EdgeId> next() {
            return this.currentIt.next();
        }

        @Override
        public void close() throws IOException {
            CloseableIterator.closeIterator(currentIt);
        }
    }

    public class EdgesIterator implements Iterator<CIter<Edge>>, Closeable {
        private final Iterator<CIter<Edge>> currentIt;

        public EdgesIterator(EdgesQueryIterator queryIterator) {
            this.currentIt = graph().edges(queryIterator);//
        }

        @Override
        public boolean hasNext() {
            return this.currentIt.hasNext();
        }

        @Override
        public CIter<Edge> next() {
            return this.currentIt.next();
        }

        @Override
        public void close() throws IOException {
            CloseableIterator.closeIterator(currentIt);
        }
    }
}
