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

import static org.apache.hugegraph.backend.query.Query.NO_LIMIT;

import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.List;
import java.util.Stack;
import java.util.concurrent.atomic.AtomicLong;
import java.util.function.Function;

import org.apache.commons.collections.CollectionUtils;
import org.apache.hugegraph.backend.id.Id;
import org.apache.hugegraph.perf.PerfUtil.Watched;
import org.apache.hugegraph.traversal.algorithm.HugeTraverser.Path;
import org.apache.hugegraph.traversal.algorithm.HugeTraverser.PathSet;
import org.apache.hugegraph.traversal.algorithm.records.record.IntIterator;
import org.apache.hugegraph.traversal.algorithm.records.record.Record;
import org.apache.hugegraph.traversal.algorithm.records.record.RecordType;
import org.apache.hugegraph.type.define.CollectionType;
import org.apache.hugegraph.util.collection.CollectionFactory;

import com.google.common.collect.Lists;

public abstract class DoubleWayMultiPathsRecords extends AbstractRecords {

    private final Stack<Record> sourceRecords;
    private final Stack<Record> targetRecords;

    private IntIterator parentRecordKeys;
    private int currentKey;
    private boolean forward;
    private final AtomicLong accessed;

    public DoubleWayMultiPathsRecords(RecordType type, boolean concurrent,
                                      Collection<Id> sources, Collection<Id> targets) {
        super(type, concurrent);
        Record firstSourceRecord = this.newRecord();
        Record firstTargetRecord = this.newRecord();
        for (Id sourceV : sources) {
            firstSourceRecord.addPath(this.code(sourceV), 0);
        }
        for (Id targetV : targets) {
            firstTargetRecord.addPath(this.code(targetV), 0);
        }
        this.sourceRecords = new Stack<>();
        this.targetRecords = new Stack<>();
        this.sourceRecords.push(firstSourceRecord);
        this.targetRecords.push(firstTargetRecord);

        this.accessed = new AtomicLong(sources.size() + targets.size());
    }

    public DoubleWayMultiPathsRecords(RecordType type, boolean concurrent,
                                      Id sourceV, Id targetV) {
        this(type, concurrent, Collections.singletonList(sourceV),
            Collections.singletonList(targetV));
    }

    @Override
    public void startOneLayer(boolean forward) {
        this.forward = forward;
        Record parentRecord = forward ? this.sourceRecords.peek() :
            this.targetRecords.peek();
        this.currentRecord(this.newRecord(), parentRecord);
        this.parentRecordKeys = parentRecord.keys();
    }

    /* remove this */
    public void resetOneLayer() {
        Record parentRecord = forward ? this.sourceRecords.peek() :
            this.targetRecords.peek();
        this.parentRecordKeys = parentRecord.keys();
    }

    @Override
    public void finishOneLayer() {
        Record record = this.currentRecord();
        if (this.forward) {
            this.sourceRecords.push(record);
        } else {
            this.targetRecords.push(record);
        }
        this.accessed.addAndGet(record.size());
    }

    @Watched
    @Override
    public boolean hasNextKey() {
        return this.parentRecordKeys.hasNext();
    }

    @Watched
    @Override
    public Id nextKey() {
        this.currentKey = this.parentRecordKeys.next();
        return this.id(this.currentKey);
    }

    public boolean parentsContain(int source, int target) {
        Record parentRecord = this.parentRecord();
        if (parentRecord == null) {
            return false;
        }

        IntIterator parents = parentRecord.get(source);
        while (parents.hasNext()) {
            int parent = parents.next();
            LOG.debug("parent = {}, curId = {}", this.id(parent), this.id(target));
            if (parent == target) {
                // find loop, stop
                return true;
            }
        }
        LOG.debug("parent = null, curId = {}", this.id(target));
        return false;
    }

    public List<Id> ids(long limit) {
        Record parentRecord = forward ? this.sourceRecords.peek() :
            this.targetRecords.peek();
        List<Id> ids = CollectionFactory.newList(CollectionType.EC);
        IntIterator iterator = parentRecord.keys();
        while ((limit == NO_LIMIT || limit-- > 0L) && iterator.hasNext()) {
            ids.add(this.id(iterator.next()));
        }
        return ids;
    }

    @Override
    @Watched
    public PathSet findPath(Id target, Function<Id, Boolean> filter,
                            boolean all, boolean ring) {
        return findPath(null, target, filter, all, ring);
    }

    public PathSet findPath(Id source, Id target, Function<Id, Boolean> filter,
                            boolean all, boolean ring) {
        PathSet results = new PathSet();
        int targetCode = this.code(target);
        int sourceCode = source == null ? this.current() : this.code(source);

        if (this.parentsContain(sourceCode, targetCode)) {
            return results;
        }
        // If cross point exists, path found, concat them
        if (this.forward && this.targetContains(targetCode)) {
            results = this.linkPath(sourceCode, targetCode, ring);
        }
        if (!this.forward && this.sourceContains(targetCode)) {
            results = this.linkPath(targetCode, sourceCode, ring);
        }
        this.addPath(targetCode, sourceCode);
        return results;
    }

    public boolean lessSources() {
        return this.sourceRecords.peek().size() <=
            this.targetRecords.peek().size();
    }

    @Override
    public long accessed() {
        return this.accessed.get();
    }

    protected boolean sourceContains(int node) {
        return this.sourceRecords.peek().containsKey(node);
    }

    protected boolean targetContains(int node) {
        return this.targetRecords.peek().containsKey(node);
    }

    @Watched
    private PathSet linkPath(int source, int target, boolean ring) {
        PathSet results = new PathSet();
        PathSet sources = this.linkSourcePath(source);
        PathSet targets = this.linkTargetPath(target);
        for (Path tpath : targets) {
            tpath.reverse();
            for (Path spath : sources) {
                if (!ring) {
                    // Avoid loop in path
                    if (CollectionUtils.containsAny(spath.vertices(),
                        tpath.vertices())) {
                        continue;
                    }
                }
                List<Id> ids = new ArrayList<>(spath.vertices());
                ids.addAll(tpath.vertices());
                Id crosspoint = this.id(this.forward ? target : source);
                results.add(new Path(crosspoint, ids));
            }
        }
        return results;
    }

    private PathSet linkSourcePath(int source) {
        return this.linkPath(this.sourceRecords, source,
            this.sourceRecords.size() - 1);
    }

    private PathSet linkTargetPath(int target) {
        return this.linkPath(this.targetRecords, target,
            this.targetRecords.size() - 1);
    }

    private PathSet linkPath(Stack<Record> all, int id, int layerIndex) {
        PathSet results = new PathSet();
        if (layerIndex == 0) {
            Id sid = this.id(id);
            results.add(new Path(Lists.newArrayList(sid)));
            return results;
        }

        Id current = this.id(id);
        Record layer = all.elementAt(layerIndex);
        IntIterator iterator = layer.get(id);
        while (iterator.hasNext()) {
            int parent = iterator.next();
            PathSet paths = this.linkPath(all, parent, layerIndex - 1);
            paths.append(current);
            results.addAll(paths);
        }
        return results;
    }

    @Watched
    protected void addPath(int current, int parent) {
        this.currentRecord().addPath(current, parent);
    }

    protected Stack<Record> sourceRecords() {
        return this.sourceRecords;
    }

    protected Stack<Record> targetRecords() {
        return this.targetRecords;
    }

    protected boolean forward() {
        return this.forward;
    }

    protected int current() {
        return this.currentKey;
    }
}
