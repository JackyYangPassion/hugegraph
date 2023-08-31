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

public class JniLongSet extends NativeReference implements Iterable<Long> {
    long handle;

    public JniLongSet(int partitionBits, int capacityBits) {
        handle = init(true, partitionBits, capacityBits);
    }

    private native boolean add(long handle, long value);

    public boolean add(long value) {
        return add(handle, value);
    }

    private native long addAll(long handle, long other);

    public long addAll(JniLongSet other) {
        return addAll(handle, other.handle);
    }

    private native boolean addExclusive(long handle, long value, long other);

    public boolean addExclusive(long value, JniLongSet other) {
        return addExclusive(handle, value, other.handle);
    }

    private native boolean contains(long handle, long value);

    public boolean contains(long value) {
        return contains(handle, value);
    }

    private native long size(long handle);

    public long size() {
        return handle != 0 ? size(handle) : 0;
    }

    private native void clear(long handle);

    public void clear() {
        clear(handle);
    }

    @Override
    public void close() {
        if (handle != 0) {
            deleteNative(handle);
            handle = 0;
        }
    }

    private native long iterator(long handle);

    @Override
    public JniLongSetIterator iterator() {
        return new JniLongSetIterator(iterator(handle));
    }

    private native long init(boolean coCurrent, int partitionBits, int capacityBits);

    private native void deleteNative(long handle);

}
