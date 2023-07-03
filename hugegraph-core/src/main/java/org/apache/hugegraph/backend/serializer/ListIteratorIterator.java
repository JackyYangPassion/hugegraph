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

import org.apache.hugegraph.iterator.WrappedIterator;

import java.util.Iterator;
import java.util.List;

/**
 * 实现对双层迭代器的遍历
 *
 * @param <R>
 */
public class ListIteratorIterator<R> extends WrappedIterator<R> {

    private Iterator<Iterator<R>> iteratorCiter;

    private Iterator<R> cuurentIterator;

    public ListIteratorIterator(List<Iterator<R>> iteratorCiter) {
        this.iteratorCiter = iteratorCiter.iterator();
        if (this.iteratorCiter.hasNext()) {
            cuurentIterator = this.iteratorCiter.next();
        }

    }

    @Override
    protected Iterator<?> originIterator() {
        return cuurentIterator;
    }

    @Override
    protected boolean fetch() {
        // 双层迭代器为空 为遍历结束
        if (!iteratorCiter.hasNext() && !cuurentIterator.hasNext()) {
            return false;
        }

        // 在双层迭代器中 寻找可以遍历的迭代器
        while (!cuurentIterator.hasNext() && iteratorCiter.hasNext()) {
            cuurentIterator = iteratorCiter.next();
        }

        // 再次判断寻找的迭代器是否可以遍历
        if (!cuurentIterator.hasNext()) {
            return false;
        }
        assert this.current == none();
        this.current = cuurentIterator.next();
        return true;
    }
}
