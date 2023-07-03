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

package org.apache.hugegraph.backend.query;

import org.apache.hugegraph.HugeGraph;
import org.apache.hugegraph.backend.id.Id;
import org.apache.hugegraph.backend.tx.GraphTransaction;
import org.apache.hugegraph.traversal.optimize.TraversalUtil;
import org.apache.hugegraph.type.define.Directions;
import org.apache.tinkerpop.gremlin.process.traversal.step.util.HasContainer;

import java.util.Collections;
import java.util.Iterator;
import java.util.List;

import static org.apache.hugegraph.backend.query.Query.NO_LIMIT;

public class EdgesQueryIterator implements Iterator<Query> {

    private final List<Id> labels;
    private final Directions dir;
    // 对单个查询来说，就是 degree
    private final long limit;
    // 顶点的度数（按当前的查询的边类型统计，或BOTH）超出改值时，为超级节点；超级节点会自动过滤。 如果NO_LIMIT 表示不过滤
    private final long skipDegree;
    private final boolean withEdgeProperties;
    private final Query.OrderType orderType;
    private final Iterator<Id> sources;
    private final List<HasContainer> conditions;
    private final HugeGraph graph;
    private final boolean withEdgeCond;
    private final boolean lightWeightSerializer = false;
    private Condition condition;

    public EdgesQueryIterator(Iterator<Id> sources, Directions dir,
                              List<Id> labels, long limit,
                              long skipDegree,
                              boolean withEdgeProperties,
                              Query.OrderType orderType,
                              List<HasContainer> conditions,
                              HugeGraph graph,
                              boolean withEdgeCond) {
        this.sources = sources;
        this.labels = labels;
        this.dir = dir;
        // Traverse NO_LIMIT 与 Query.NO_LIMIT 不同
        this.limit = limit < 0 ? Query.NO_LIMIT : limit;
        this.skipDegree = skipDegree < 0 ? Query.NO_LIMIT : skipDegree;
        this.withEdgeProperties = withEdgeProperties;
        this.orderType = orderType;
        this.conditions = conditions;
        this.graph = graph;
        this.withEdgeCond = withEdgeCond;
    }

    public EdgesQueryIterator(Iterator<Id> sources, Directions dir,
                              Id label, long limit, long skipDegree,
                              boolean withEdgeProperties,
                              Query.OrderType orderType) {
        this(sources, dir, label == null ? null : Collections.singletonList(label), limit,
             skipDegree, withEdgeProperties, orderType);
    }

    public EdgesQueryIterator(Iterator<Id> sources, Directions dir,
                              List<Id> labelsId, long limit, long skipDegree,
                              boolean withEdgeProperties,
                              Query.OrderType orderType) {
        this(sources, dir, labelsId, limit,
             skipDegree, withEdgeProperties, orderType, null, null, false);
    }

    @Override
    public boolean hasNext() {
        return sources.hasNext();
    }

    public Condition getCondition() {
        return condition;
    }

    public void setCondition(Condition condition) {
        this.condition = condition;
    }

    @Override
    public Query next() {
        Id sourceId = this.sources.next();
        ConditionQuery query = GraphTransaction.constructEdgesQuery2(sourceId
                , this.dir, this.labels, graph);
        // Query by sort-keys
        if (withEdgeCond && graph != null) {
            TraversalUtil.fillConditionQuery(query, conditions, graph);
        }

        if (this.limit != NO_LIMIT) {
            query.limit(this.limit);
            query.capacity(this.limit);
        } else {
            query.capacity(Query.NO_CAPACITY);
        }
        if (condition != null) {
            query.query(condition);
        }

        query.skipDegree(this.skipDegree);
        query.orderType(this.orderType);
        query.withProperties(this.withEdgeProperties);
        return query;
    }

    public boolean isWithEdgeProperties() {
        return withEdgeProperties;
    }

    public boolean isLightWeightSerializer() {
        return lightWeightSerializer;
    }

    public Query sampleQuery() {
        ConditionQuery query = GraphTransaction.constructEdgesQuery2(null,
                                                                     this.dir
                , this.labels, null);
        if (this.limit != NO_LIMIT) {
            query.limit(this.limit);
            query.capacity(this.limit);
        } else {
            query.capacity(Query.NO_CAPACITY);
        }
        if (condition != null) {
            query.query(condition);
        }
        query.skipDegree(this.skipDegree);
        query.orderType(this.orderType);
        query.withProperties(this.withEdgeProperties);
        return query;
    }
}
