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

package org.apache.hugegraph.backend.serializer;

import org.apache.commons.compress.utils.Lists;
import org.apache.hugegraph.HugeGraph;
import org.apache.hugegraph.backend.id.Id;
import org.apache.hugegraph.backend.tx.GraphTransaction;
import org.apache.hugegraph.structure.HugeEdge;
import org.apache.hugegraph.util.ExecutorUtil;
import org.apache.tinkerpop.gremlin.structure.Edge;
import org.apache.tinkerpop.gremlin.structure.Vertex;

import java.util.Iterator;
import java.util.List;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Future;

/**
 * 存在线程和Iterator问题，
 * 当iterator不再使用时，线程没有及时关闭且底层iterator未执行关闭操作，
 * 不要使用
 */
@Deprecated
public class HugeVertexBatchQuery {

    private static ExecutorService executors = null;
    private static int perBatchNum = 0;
    private final Iterator<Edge> originIterator;
    private HugeGraph hugeGraph;
    private GraphTransaction graphTransaction;

    public HugeVertexBatchQuery(Iterator<Edge> origin, HugeGraph hugeGraph,
                                GraphTransaction graphTransaction) {
        assert hugeGraph != null;
        assert graphTransaction != null;

        this.originIterator = origin;
        this.hugeGraph = hugeGraph;
        this.graphTransaction = graphTransaction;

        if (executors != null) {
            return;
        }
        synchronized (HugeVertexBatchQuery.class) {
            if (executors != null) {
                return;
            }
            int workers = 10;
            if (workers > 0) {
                executors = ExecutorUtil.newFixedThreadPool(workers, "huge-batch-thread");
                perBatchNum = 10000;
            }
        }
    }

    public Iterator<Vertex> getAllResult() {

        List<Id> queryList = Lists.newArrayList();
        List<Iterator<Vertex>> resultList = Lists.newArrayList();
        List<Future<Iterator<Vertex>>> submitList = Lists.newArrayList();

        while (originIterator.hasNext()) {
            queryList.add(((HugeEdge) originIterator.next()).otherVertex().id());
            if (queryList.size() >= perBatchNum) {
                List<Id> finalQueryList = queryList;
                Future<Iterator<Vertex>> submit = executors.submit(() -> {
                    return this.graphTransaction.queryAdjacentVertices(
                            finalQueryList.toArray());
                });
                submitList.add(submit);
                queryList = Lists.newArrayList();
            }
        }

        if (queryList.size() > 0) {
            List<Id> finalQueryList = queryList;
            Future<Iterator<Vertex>> submit = executors.submit(() -> {
                return this.graphTransaction.queryAdjacentVertices(
                        finalQueryList.toArray());
            });
            submitList.add(submit);
        }

        for (Future<Iterator<Vertex>> future : submitList) {
            try {
                resultList.add(future.get());
            } catch (Exception e) {
                e.printStackTrace();
            }
        }
        return new ListIteratorIterator<>(resultList);
    }
}
