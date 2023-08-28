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

import java.util.Iterator;

import org.apache.hugegraph.iterator.WrappedIterator;
import org.apache.hugegraph.structure.HugeEdge;
import org.apache.hugegraph.structure.HugeVertex;
import org.apache.tinkerpop.gremlin.structure.Edge;
import org.apache.tinkerpop.gremlin.structure.Vertex;

/**
 * 构造edge转换 vertex的迭代器
 *
 * @param <T>
 * @param <R>
 */
public class EdgeToVertexIterator<T extends Edge, R> extends WrappedIterator<Vertex> {

    private Iterator<T> edgesBatch;

    public EdgeToVertexIterator(final Iterator<T> edgesBatch) {
        this.edgesBatch = edgesBatch;
    }

    @Override
    protected Iterator<?> originIterator() {
        return edgesBatch;
    }

    /**
     * 遍历的时候 进行根据边 转换成 点 信息
     * 仅仅转换出 id信息
     *
     * @return
     */
    @Override
    protected boolean fetch() {
        if (!edgesBatch.hasNext()) {
            return false;
        }
        assert this.current == none();
        HugeEdge hugeEdge = (HugeEdge) edgesBatch.next();
        HugeVertex vertex = hugeEdge.otherVertex();
        this.current = vertex;
        return true;
    }
}
