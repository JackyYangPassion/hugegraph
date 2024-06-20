/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.hugegraph.backend.cache;

import org.apache.hugegraph.backend.id.Id;

import java.util.Iterator;
import java.util.function.Consumer;

public class RedisCache  extends AbstractCache<Id, Object> {
    //TODO: 实现提升xhop 查询性能的一种选择
    // 需要明确这种缓存粒度
    // 缓存一致性问题：集中式缓存

    @Override
    protected Object access(Id id) {
        return null;
    }

    @Override
    protected boolean write(Id id, Object value, long timeOffset) {
        return false;
    }

    @Override
    protected void remove(Id id) {

    }

    @Override
    protected Iterator<CacheNode<Id, Object>> nodes() {
        return null;
    }

    @Override
    public boolean containsKey(Id id) {
        return false;
    }

    @Override
    public void traverse(Consumer<Object> consumer) {

    }

    @Override
    public void clear() {

    }

    @Override
    public long size() {
        return 0;
    }
}
