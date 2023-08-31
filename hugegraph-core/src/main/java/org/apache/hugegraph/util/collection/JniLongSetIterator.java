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

import java.util.Iterator;

public class JniLongSetIterator extends NativeReference implements Iterator<Long> {
    long handle;

    public JniLongSetIterator(long handle) {
        this.handle = handle;
    }

    private native boolean hasNext(long handle);

    @Override
    public boolean hasNext() {
        return handle != 0 ? hasNext(handle) : false;
    }

    private native long next(long handle);

    @Override
    public Long next() {
        if (handle != 0) {
            return next(handle);
        }
        throw new NullPointerException();
    }

    @Override
    public void close() {
        if (handle != 0) {
            deleteNative(handle);
            handle = 0;
        }
    }


    private native void deleteNative(long handle);
}
