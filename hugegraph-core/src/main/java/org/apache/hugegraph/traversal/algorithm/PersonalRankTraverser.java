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

import java.util.Collection;
import java.util.Iterator;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Future;
import java.util.concurrent.atomic.AtomicLong;
import java.util.function.BiFunction;
import java.util.function.Consumer;

import org.apache.commons.compress.utils.Lists;
import org.apache.commons.lang3.tuple.Pair;
import org.apache.hugegraph.HugeException;
import org.apache.hugegraph.HugeGraph;
import org.apache.hugegraph.backend.id.EdgeId;
import org.apache.hugegraph.backend.id.Id;
import org.apache.hugegraph.backend.query.EdgesQueryIterator;
import org.apache.hugegraph.backend.query.Query;
import org.apache.hugegraph.backend.serializer.IteratorCiterIterator;
import org.apache.hugegraph.config.CoreOptions;
import org.apache.hugegraph.iterator.CIter;
import org.apache.hugegraph.schema.EdgeLabel;
import org.apache.hugegraph.schema.VertexLabel;
import org.apache.hugegraph.structure.HugeVertex;
import org.apache.hugegraph.type.define.Directions;
import org.apache.hugegraph.util.E;
import org.apache.hugegraph.util.ExecutorUtil;
import org.apache.tinkerpop.gremlin.structure.Direction;
import org.apache.tinkerpop.gremlin.util.iterator.IteratorUtils;

import com.google.common.collect.Sets;

public class PersonalRankTraverser extends HugeTraverser {

    private static final ExecutorService EXECUTOR_SERVICE =
        ExecutorUtil.newFixedThreadPool(
            2 * CoreOptions.CPUS, "calculatePersonRank");
    private final double alpha;
    private final long degree;
    private final int maxDepth;

    public PersonalRankTraverser(HugeGraph graph, double alpha,
                                 long degree, int maxDepth) {
        super(graph);
        this.alpha = alpha;
        this.degree = degree;
        this.maxDepth = maxDepth;
    }

    private static void removeAll(Map<Id, Double> map, Set<Id> keys) {
        for (Id key : keys) {
            map.remove(key);
        }
    }

    private static void removeAll(Map<Id, Double> map,
                                  Collection<Set<Id>> keys) {
        for (Set<Id> keysTmp : keys) {
            for (Id key : keysTmp) {
                map.remove(key);
            }
        }
    }

    public Map<Id, Double> personalRank(Id source, String label,
                                        WithLabel withLabel) {
        E.checkNotNull(source, "source vertex id");
        this.checkVertexExist(source, "source");
        E.checkArgumentNotNull(label, "The edge label can't be null");

        Map<Id, Double> ranks = newMap();
        ranks.put(source, 1.0);

        Id labelId = this.graph().edgeLabel(label).id();
        Directions dir = this.getStartDirection(source, label);

        Set<Id> outSeeds = newIdSet();
        Set<Id> inSeeds = newIdSet();
        if (dir == Directions.OUT) {
            outSeeds.add(source);
        } else {
            inSeeds.add(source);
        }

        Set<Id> rootAdjacencies = newIdSet();
        for (long i = 0; i < this.maxDepth; i++) {
            Map<Id, Double> newRanks = this.calcNewRanks(outSeeds, inSeeds,
                labelId, ranks);
            ranks = this.compensateRoot(source, newRanks);
            if (i == 0) {
                rootAdjacencies.addAll(ranks.keySet());
            }
        }
        // Remove directly connected neighbors
        removeAll(ranks, rootAdjacencies);
        // Remove unnecessary label
        if (withLabel == WithLabel.SAME_LABEL) {
            removeAll(ranks, dir == Directions.OUT ? inSeeds : outSeeds);
        } else if (withLabel == WithLabel.OTHER_LABEL) {
            removeAll(ranks, dir == Directions.OUT ? outSeeds : inSeeds);
        }
        return ranks;
    }

    /**
     * 根据输入的id 计算该id 二部图的personal rank
     *
     * @param source
     * @param labels
     * @param withLabel
     * @return
     */
    public Map<Id, Double> personalRankForBatch(Id source, List<String> labels,
                                                WithLabel withLabel, String direction) {
        E.checkNotNull(source, "source vertex id");
        this.checkVertexExist(source, "source");
        E.checkArgumentNotNull(labels, "The edge label can't be null");

        Map<Id, Double> ranks = newMap();
        ranks.put(source, 1.0);

        List<Id> edgeLabels = Lists.newArrayList();
        Directions dir;
        if (!labels.isEmpty()) {
            Set<Directions> directionsSet = newSet();
            for (String label : labels) {
                Id edgeLabel = this.graph().edgeLabel(label).id();
                edgeLabels.add(edgeLabel);
                Directions dirTmp = this.getStartDirection(source, label);
                directionsSet.add(dirTmp);
            }

            E.checkState(directionsSet.size() == 1,
                "Only one direction can be " +
                    "used");
            dir = directionsSet.iterator().next();
        } else {
            dir = Directions.convert(parseDirection(direction));
        }

        Set<Id> outSeeds = newJniIdSet();
        Set<Id> inSeeds = newJniIdSet();
        if (dir == Directions.OUT) {
            outSeeds.add(source);
        } else {
            inSeeds.add(source);
        }

        Map<Id, Set<Id>> idAndNeighborsForOut = new ConcurrentHashMap<>();
        Map<Id, Set<Id>> idAndNeighborsForIn = new ConcurrentHashMap<>();
        Set<Id> rootAdjacencies = newJniIdSet();
        Map<Id, Double> newRanks;
        for (long i = 0; i < this.maxDepth; i++) {
            CalcNewRanksResult calcNewRanksResult =
                this.calcNewRanksNewForBatch(outSeeds, inSeeds,
                    edgeLabels
                    , ranks, idAndNeighborsForOut, idAndNeighborsForIn);
            newRanks = calcNewRanksResult.getNewRanks();
            closeSet(outSeeds);
            closeSet(inSeeds);
            outSeeds = calcNewRanksResult.getOutSeeds();
            inSeeds = calcNewRanksResult.getInSeeds();
            ranks = this.compensateRoot(source, newRanks);
            if (i == 0) {
                rootAdjacencies.addAll(ranks.keySet());
            }
        }
        closeSet(outSeeds);
        closeSet(inSeeds);
        LOG.info("Personal rank source:{} ranks size: {}",
            source.asString(), ranks.size());
        // Remove directly connected neighbors
        long startTime = System.currentTimeMillis();
        removeAll(ranks, rootAdjacencies);
        closeSet(rootAdjacencies);
        // Remove unnecessary label
        if (withLabel == WithLabel.SAME_LABEL) {
            removeAll(ranks, dir == Directions.OUT ?
                idAndNeighborsForOut.values() :
                idAndNeighborsForIn.values());
        } else if (withLabel == WithLabel.OTHER_LABEL) {
            removeAll(ranks, dir == Directions.OUT ?
                idAndNeighborsForIn.values() :
                idAndNeighborsForOut.values());
        }
        LOG.info("Personal rank source:{} ranks size: {} remove all cost: {}",
            source.asString(), ranks.size(),
            System.currentTimeMillis() - startTime);
        return ranks;
    }

    public Direction parseDirection(String direction) {
        if (direction == null || direction.isEmpty()) {
            return Direction.OUT;
        }
        try {
            return Direction.valueOf(direction);
        } catch (Exception e) {
            throw new IllegalArgumentException(String.format(
                "Direction value must be in [OUT, IN, BOTH], " +
                    "but got '%s'", direction));
        }
    }

    private Map<Id, Double> calcNewRanks(Set<Id> outSeeds, Set<Id> inSeeds,
                                         Id label, Map<Id, Double> ranks) {
        Map<Id, Double> newRanks = newMap();
        BiFunction<Set<Id>, Directions, Set<Id>> neighborIncrRanks;
        neighborIncrRanks = (seeds, dir) -> {
            Set<Id> tmpSeeds = newIdSet();
            for (Id seed : seeds) {
                Double oldRank = ranks.get(seed);
                E.checkState(oldRank != null, "Expect rank of seed exists");

                Iterator<Id> iter = this.adjacentVertices(seed, dir, label,
                    this.degree);
                List<Id> neighbors = IteratorUtils.list(iter);

                long degree = neighbors.size();
                if (degree == 0L) {
                    newRanks.put(seed, oldRank);
                    continue;
                }
                double incrRank = oldRank * this.alpha / degree;

                // Collect all neighbors increment
                for (Id neighbor : neighbors) {
                    tmpSeeds.add(neighbor);
                    // Assign an initial value when firstly update neighbor rank
                    double rank = newRanks.getOrDefault(neighbor, 0.0);
                    newRanks.put(neighbor, rank + incrRank);
                }
            }
            return tmpSeeds;
        };

        Set<Id> tmpInSeeds = neighborIncrRanks.apply(outSeeds, Directions.OUT);
        Set<Id> tmpOutSeeds = neighborIncrRanks.apply(inSeeds, Directions.IN);

        outSeeds.addAll(tmpOutSeeds);
        inSeeds.addAll(tmpInSeeds);
        return newRanks;
    }

    /**
     * 使用批量计算, 计算personal rank
     *
     * @param outSeeds
     * @param inSeeds
     * @param labelIds
     * @param ranks
     * @return
     */
    private CalcNewRanksResult calcNewRanksNewForBatch(Set<Id> outSeeds,
                                                       Set<Id> inSeeds,
                                                       List<Id> labelIds,
                                                       Map<Id, Double> ranks,
                                                       Map<Id, Set<Id>> idAndNeighborsForOut,
                                                       Map<Id, Set<Id>> idAndNeighborsForIn) {

        Set<Id> tmpInSeeds = this.queryVertexNeighborsByParallel(outSeeds,
            labelIds,
            Directions.OUT,
            idAndNeighborsForOut);
        Set<Id> tmpOutSeeds = this.queryVertexNeighborsByParallel(inSeeds,
            labelIds,
            Directions.IN,
            idAndNeighborsForIn);

        long startTime = System.currentTimeMillis();
        Map<Id, Double> newRanks = new ConcurrentHashMap<>();

        this.calculatePersonRank(ranks, newRanks, idAndNeighborsForOut);
        this.calculatePersonRank(ranks, newRanks, idAndNeighborsForIn);
        LOG.info("calculatePersonRank ranks size: {} cost: {}",
            newRanks.size(), System.currentTimeMillis() - startTime);

        startTime = System.currentTimeMillis();

        removeAll(ranks, newRanks.keySet());
        for (Map.Entry<Id, Double> entry : ranks.entrySet()) {
            newRanks.put(entry.getKey(), entry.getValue());
        }
        LOG.info("2 calculatePersonRank ranks size: {} cost: {}",
            newRanks.size(), System.currentTimeMillis() - startTime);
        ranks = null;
        return new CalcNewRanksResult(newRanks, tmpOutSeeds, tmpInSeeds);
    }

    /**
     * 计算personal rank的值
     *
     * @param ranks
     * @param newRanks
     * @param idAndNeighbors
     */
    public void calculatePersonRank(Map<Id, Double> ranks,
                                    Map<Id, Double> newRanks,
                                    Map<Id, Set<Id>> idAndNeighbors) {
        List<Future<?>> submitList = new LinkedList<>();
        for (Map.Entry<Id, Set<Id>> entry : idAndNeighbors.entrySet()) {
            Future<?> submit = EXECUTOR_SERVICE.submit(() -> {
                Double oldRank = ranks.get(entry.getKey());
                E.checkState(oldRank != null, "Expect rank of seed exists");
                long degree = entry.getValue().size();
                double incrRank = oldRank * this.alpha / degree;
                for (Id neighbor : entry.getValue()) {
                    double rank = newRanks.getOrDefault(neighbor, 0.0);
                    newRanks.put(neighbor, rank + incrRank);
                }
            });
            submitList.add(submit);
        }
        for (Future<?> submit : submitList) {
            try {
                submit.get();
            } catch (Exception e) {
                throw new RuntimeException(e);
            }
        }
    }

    /**
     * 批量+并行消费方式查询点的邻节点
     *
     * @param seeds
     * @param labelId
     * @param dir
     * @param idAndNeighbors 记录节点和邻节点的关系
     * @return
     */
    public Set<Id> queryVertexNeighborsByParallel(Set<Id> seeds,
                                                  List<Id> labelId,
                                                  Directions dir,
                                                  Map<Id, Set<Id>> idAndNeighbors) {
        Set<Id> tmpSeeds = newJniIdSet();
        if (seeds.isEmpty()) {
            return tmpSeeds;
        }
        long startTime = System.currentTimeMillis();
        AtomicLong count = new AtomicLong(0L);
        Consumer<EdgeId> consumer = (edgeId) -> {
            Id sourceVertexId = edgeId.ownerVertexId();
            Id targetVertexId = edgeId.otherVertexId();
            count.addAndGet(1L);
            tmpSeeds.add(targetVertexId);
            if (idAndNeighbors.containsKey(sourceVertexId)) {
                Set<Id> ids = idAndNeighbors.get(sourceVertexId);
                ids.add(targetVertexId);
            } else {
                Set<Id> ids = Sets.newConcurrentHashSet();
                ids.add(targetVertexId);
                idAndNeighbors.put(sourceVertexId, ids);
            }
        };
        KoutTraverser koutTraverser = new KoutTraverser(this.graph());
        koutTraverser.bfsQueryForPersonalRank(seeds.iterator(), dir,
            labelId,
            this.degree, -1, -1,
            consumer, Query.OrderType.ORDER_NONE);
        LOG.info("seeds size:{} Parallel query vertex neighbors size: {}, " +
                "count:{}, cost:{}",
            seeds.size(),
            tmpSeeds.size(), count, System.currentTimeMillis() - startTime);

        return tmpSeeds;
    }

    /**
     * 批量的方式查询点的邻节点
     *
     * @param seeds
     * @param labelIds
     * @param dir
     * @param idAndNeighbors 记录节点和邻节点的关系
     * @return
     */
    public Set<Id> queryVertexNeighborsByBatch(Set<Id> seeds,
                                               List<Id> labelIds,
                                               Directions dir,
                                               Map<Id, Set<Id>> idAndNeighbors) {
        Set<Id> tmpSeeds = newIdSet();
        if (seeds.isEmpty()) {
            return tmpSeeds;
        }

        // 批量的方式
        EdgesQueryIterator edgesQueryIterator =
            new EdgesQueryIterator(seeds.iterator(), dir,
                labelIds,
                degree, NO_LIMIT, false,
                Query.OrderType.ORDER_NONE);
        // Do query
        Iterator<CIter<EdgeId>> edgesBatch =
            this.graph().edgeIds(edgesQueryIterator);
        // 双层迭代器转换
        Iterator<EdgeId> edges = new IteratorCiterIterator<>(edgesBatch);

        AtomicLong count = new AtomicLong(0L);
        while (edges.hasNext()) {
            count.addAndGet(1L);
            EdgeId edgeId = (EdgeId) edges.next();
            Id sourceVertexId = edgeId.ownerVertexId();
            Id targetVertexId = edgeId.otherVertexId();
            if (!seeds.contains(sourceVertexId)) {
                continue;
            }
            tmpSeeds.add(targetVertexId);
            if (idAndNeighbors.containsKey(sourceVertexId)) {
                idAndNeighbors.get(sourceVertexId).add(targetVertexId);
            } else {
                Set<Id> ids = newIdSet();
                ids.add(targetVertexId);
                idAndNeighbors.put(sourceVertexId, ids);
            }
        }
        LOG.info("seeds size:{} Parallel query vertex neighbors size: {}, " +
                "count:{}",
            seeds.size(),
            tmpSeeds.size(), count);
        return tmpSeeds;
    }

    private Map<Id, Double> compensateRoot(Id root, Map<Id, Double> newRanks) {
        double rank = newRanks.getOrDefault(root, 0.0);
        rank += (1 - this.alpha);
        newRanks.put(root, rank);
        return newRanks;
    }

    private Directions getStartDirection(Id source, String label) {
        // NOTE: The outer layer needs to ensure that the vertex Id is valid
        HugeVertex vertex = (HugeVertex) graph().vertex(source);
        this.edgeIterCounter.addAndGet(1);
        VertexLabel vertexLabel = vertex.schemaLabel();
        EdgeLabel edgeLabel = this.graph().edgeLabel(label);
        for (Pair<Id, Id> link : edgeLabel.links()) {
            Id sourceLabel = link.getLeft();
            Id targetLabel = link.getRight();

            E.checkArgument(edgeLabel.linkWithLabel(vertexLabel.id()),
                "The vertex '%s' doesn't link with edge label '%s'",
                source, label);
            E.checkArgument(!sourceLabel.equals(targetLabel),
                "The edge label for personal rank must " +
                    "link different vertex labels");
            if (sourceLabel.equals(vertexLabel.id())) {
                return Directions.OUT;
            } else if (targetLabel.equals(vertexLabel.id())) {
                return Directions.IN;
            }
        }
        throw new HugeException("The edgelabel %s not link vertex label %s",
            edgeLabel.name(), vertexLabel.name());
    }

    public enum WithLabel {
        SAME_LABEL,
        OTHER_LABEL,
        BOTH_LABEL
    }

    public class CalcNewRanksResult {
        Map<Id, Double> newRanks;
        Set<Id> outSeeds;
        Set<Id> inSeeds;

        public CalcNewRanksResult(Map<Id, Double> newRanks, Set<Id> outSeeds,
                                  Set<Id> inSeeds) {
            this.newRanks = newRanks;
            this.outSeeds = outSeeds;
            this.inSeeds = inSeeds;
        }

        public Map<Id, Double> getNewRanks() {
            return newRanks;
        }

        public Set<Id> getOutSeeds() {
            return outSeeds;
        }

        public Set<Id> getInSeeds() {
            return inSeeds;
        }
    }
}
