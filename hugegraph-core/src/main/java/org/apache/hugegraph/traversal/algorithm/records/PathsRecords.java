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

package org.apache.hugegraph.traversal.algorithm.records;

import java.util.Collection;
import java.util.HashMap;
import java.util.Iterator;
import java.util.Map;
import java.util.Set;

import org.apache.hugegraph.backend.id.Id;
import org.apache.hugegraph.traversal.algorithm.HugeTraverser;
import org.apache.hugegraph.traversal.algorithm.records.record.RecordType;
import org.apache.hugegraph.type.define.CollectionType;
import org.apache.hugegraph.util.collection.CollectionFactory;

public class PathsRecords extends DoubleWayMultiPathsRecords {
    private Map<Long, Id> edgeIds = new HashMap<>();

    public PathsRecords(boolean concurrent, Id sourceV, Id targetV) {
        super(RecordType.ARRAY, concurrent, sourceV, targetV);
    }

    public PathsRecords(boolean concurrent, Collection<Id> sources, Collection<Id> targets) {
        super(RecordType.ARRAY, concurrent, sources, targets);
    }

    public static Long makeCodePair(int source, int target) {
        return ((long) source & 0xFFFFFFFFL) |
            (((long) target << 32) & 0xFFFFFFFF00000000L);
    }

    public void addEdgeId(Id source, Id target, Id edgeId) {
        Long pair = makeCodePair(this.code(source),
            this.code(target));
        this.edgeIds.put(pair, edgeId);
    }

    protected Id getEdgeId(Id source, Id target) {
        Long pair = makeCodePair(this.code(source),
            this.code(target));
        return this.edgeIds.get(pair);
    }

    public Set<Id> getEdgeIds(HugeTraverser.Path path) {
        Set<Id> edgeIds = CollectionFactory.newSet(CollectionType.EC);
        if (path == null || path.vertices().isEmpty()) {
            return edgeIds;
        }
        Iterator<Id> verticeIter = path.vertices().iterator();
        Id before = verticeIter.next();
        Id after;
        while (verticeIter.hasNext()) {
            after = verticeIter.next();
            Id edgeId = getEdgeId(before, after);
            if (edgeId == null) {
                edgeId = getEdgeId(after, before);
            }
            if (edgeId != null) {
                edgeIds.add(edgeId);
            }
            before = after;
        }
        return edgeIds;
    }

    public Set<Id> getEdgeIds(Set<HugeTraverser.Path> paths) {
        Set<Id> edgeIds = CollectionFactory.newSet(CollectionType.EC);
        for (HugeTraverser.Path path : paths) {
            edgeIds.addAll(getEdgeIds(path));
        }
        return edgeIds;
    }
}
