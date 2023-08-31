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

package org.apache.hugegraph.util.collection;

import org.apache.hugegraph.backend.id.Id;
import org.apache.hugegraph.backend.id.IdGenerator;
import org.apache.hugegraph.backend.id.IdUtil;
import org.apache.hugegraph.iterator.ExtendableIterator;
import org.apache.hugegraph.iterator.WrappedIterator;
import org.apache.hugegraph.util.Log;
import org.apache.tinkerpop.gremlin.structure.util.CloseableIterator;
import org.slf4j.Logger;

import java.io.Closeable;
import java.io.IOException;
import java.util.AbstractSet;
import java.util.Collection;
import java.util.Iterator;
import java.util.concurrent.atomic.AtomicInteger;

public class JniIdSet extends AbstractSet<Id> implements Closeable {
    private static final Logger LOG = Log.logger(JniIdSet.class);
    public static AtomicInteger refCount = new AtomicInteger();

    static {
        JniSetLoader.loadLibrary();
    }

    private final int partitionBits = 5;
    private final int capacityBits = 10;
    private final JniLongSet numberIds;
    private final JniBytesSet nonNumberIds;
    private long limit = Long.MAX_VALUE;

    public JniIdSet() {
        this.numberIds = new JniLongSet(partitionBits, capacityBits);
        this.nonNumberIds = new JniBytesSet(partitionBits, capacityBits);
        refCount.incrementAndGet();
    }

    @Override
    public int size() {
        return (int) (this.numberIds.size() + this.nonNumberIds.size());
    }

    @Override
    public boolean isEmpty() {
        return size() == 0;
    }

    public void setLimit(long limit) {
        this.limit = limit;
    }

    @Override
    public boolean contains(Object object) {
        if (!(object instanceof Id)) {
            return false;
        }
        Id id = (Id) object;
        if (id.type() == Id.IdType.LONG) {
            long value = id.asLong();
            return this.numberIds.contains(value);
        } else {
            byte[] value = IdUtil.asBytes(id);
            return this.nonNumberIds.contains(value);
        }
    }


    <E extends Iterable> CloseableIterator<Id> getIterator(E jniObj) {
        boolean isLongType = jniObj instanceof JniLongSet;
        return new CloseableIterator<Id>() {
            Iterator iterator = jniObj.iterator();

            @Override
            public boolean hasNext() {
                boolean has = iterator != null && iterator.hasNext();
                if (!has && iterator != null) {
                    close();
                }
                return has;
            }


            @Override
            public Id next() {
                if (iterator != null) {
                    if (isLongType) {
                        return IdGenerator.of(iterator.next());
                    } else {
                        return IdUtil.fromBytes((byte[]) iterator.next());
                    }
                }
                return null;
            }

            @Override
            public void close() {
                if (iterator != null) {
                    try {
                        ((Closeable) iterator).close();
                    } catch (IOException e) {
                        LOG.error("exception", e);
                    }
                }
                iterator = null;
            }
        };
    }

    @Override
    public Iterator<Id> iterator() {
        return new LimitIterator(new ExtendableIterator<>(
                getIterator(this.numberIds),
                getIterator(this.nonNumberIds)), this.limit);
    }

    @Override
    public boolean add(Id id) {
        if (id.type() == Id.IdType.LONG) {
            long value = id.asLong();
            return this.numberIds.add(value);
        } else {
            byte[] value = IdUtil.asBytes(id);
            return this.nonNumberIds.add(value);
        }
    }


    @Override
    public boolean addAll(Collection<? extends Id> c) {
        if (c instanceof JniIdSet) {
            numberIds.addAll(((JniIdSet) c).numberIds);
            nonNumberIds.addAll(((JniIdSet) c).nonNumberIds);
            return true;
        } else {
            return super.addAll(c);
        }
    }

    public boolean addExclusive(Id id, JniIdSet other) {
        if (id.type() == Id.IdType.LONG) {
            long value = id.asLong();
            return this.numberIds.addExclusive(value, other.numberIds);
        } else {
            byte[] value = IdUtil.asBytes(id);
            return this.nonNumberIds.addExclusive(value, other.nonNumberIds);
        }
    }

    public boolean remove(Id id) {
        throw new RuntimeException("Method not implement.");
    }

    @Override
    public void clear() {
        this.numberIds.clear();
        this.nonNumberIds.clear();
    }

    @Override
    public void close() {
        refCount.decrementAndGet();
        LOG.info("JniIdSet counter {}", refCount.get());
        numberIds.close();
        nonNumberIds.close();
    }


    static class LimitIterator<T> extends WrappedIterator<T> {
        private final Iterator<T> iterator;
        private final long limit;
        private int counter;

        public LimitIterator(Iterator<T> origin, long limit) {
            this.iterator = origin;
            this.limit = limit;
            this.counter = 0;
        }

        @Override
        protected Iterator<T> originIterator() {
            return this.iterator;
        }

        @Override
        protected final boolean fetch() {
            if (this.iterator.hasNext()) {
                T next = this.iterator.next();
                if (counter++ < limit) {
                    this.current = next;
                    return true;
                } else {
                    this.closeOriginIterator();
                }
            }
            return false;
        }

        protected final void closeOriginIterator() {
            if (this.iterator != null) {
                close(this.iterator);
            }
        }
    }
}
