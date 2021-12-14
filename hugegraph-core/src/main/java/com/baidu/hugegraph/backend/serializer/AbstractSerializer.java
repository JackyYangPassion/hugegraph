/*
 * Copyright 2017 HugeGraph Authors
 *
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

package com.baidu.hugegraph.backend.serializer;

import com.baidu.hugegraph.backend.BackendException;
import com.baidu.hugegraph.backend.id.Id;
import com.baidu.hugegraph.backend.query.ConditionQuery;
import com.baidu.hugegraph.backend.query.IdQuery;
import com.baidu.hugegraph.backend.query.Query;
import com.baidu.hugegraph.backend.store.BackendEntry;
import com.baidu.hugegraph.config.HugeConfig;
import com.baidu.hugegraph.type.HugeType;

public abstract class AbstractSerializer
                implements GraphSerializer, SchemaSerializer {

    public AbstractSerializer() {
        // TODO: default constructor
    }

    public AbstractSerializer(HugeConfig config) {
        // TODO: use the config
    }

    protected BackendEntry convertEntry(BackendEntry entry) {
        return entry;
    }

    protected abstract BackendEntry newBackendEntry(HugeType type, Id id);

    protected abstract Id writeQueryId(HugeType type, Id id);

    protected abstract Query writeQueryEdgeCondition(Query query);

    protected abstract Query writeQueryCondition(Query query);

    @Override
    public Query writeQuery(Query query) {
        HugeType type = query.resultType();

        // Serialize edge condition query (TODO: add VEQ(for EOUT/EIN))
        if (type.isEdge() && query.conditionsSize() > 0) {
            if (query.idsSize() > 0) {
                throw new BackendException("Not supported query edge by id " +
                                           "and by condition at the same time");
            }

            Query result = this.writeQueryEdgeCondition(query);
            if (result != null) {
                return result;
            }
        }

        // Serialize id in query
        if (query.idsSize() == 1 && query instanceof IdQuery.OneIdQuery) {
            IdQuery.OneIdQuery result = (IdQuery.OneIdQuery) query.copy();
            result.resetId(this.writeQueryId(type, result.id()));
            query = result;
        } else if (query.idsSize() > 0 && query instanceof IdQuery) {
            IdQuery result = (IdQuery) query.copy();
            result.resetIds();
            for (Id id : query.ids()) {
                result.query(this.writeQueryId(type, id));
            }
            query = result;
        }

        // Serialize condition(key/value) in query
        if (query instanceof ConditionQuery && query.conditionsSize() > 0) {
            query = this.writeQueryCondition(query);
        }

        return query;
    }
}
