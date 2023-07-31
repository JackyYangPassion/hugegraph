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

package org.apache.hugegraph.backend.store.hbase;

import java.io.IOException;
import java.util.*;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.locks.Lock;
import java.util.concurrent.locks.ReadWriteLock;
import java.util.concurrent.locks.ReentrantReadWriteLock;
import java.util.function.BiFunction;
import java.util.function.Function;
import java.util.stream.Collectors;

import org.apache.commons.collections.CollectionUtils;
import org.apache.commons.compress.utils.Lists;
import org.apache.hadoop.hbase.NamespaceExistException;
import org.apache.hadoop.hbase.TableExistsException;
import org.apache.hadoop.hbase.TableNotFoundException;
import org.apache.hugegraph.HugeGraph;
import org.apache.hugegraph.backend.query.ConditionQuery;
import org.apache.hugegraph.backend.query.ConditionQueryFlatten;
import org.apache.hugegraph.backend.query.IdPrefixQuery;
import org.apache.hugegraph.backend.serializer.BinaryBackendEntry;
import org.apache.hugegraph.backend.serializer.BytesBuffer;
import org.apache.hugegraph.type.define.HugeKeys;
import org.slf4j.Logger;

import org.apache.hugegraph.backend.BackendException;
import org.apache.hugegraph.backend.id.Id;
import org.apache.hugegraph.backend.query.Query;
import org.apache.hugegraph.backend.store.AbstractBackendStore;
import org.apache.hugegraph.backend.store.BackendAction;
import org.apache.hugegraph.backend.store.BackendEntry;
import org.apache.hugegraph.backend.store.BackendFeatures;
import org.apache.hugegraph.backend.store.BackendMutation;
import org.apache.hugegraph.backend.store.BackendStoreProvider;
import org.apache.hugegraph.config.HugeConfig;
import org.apache.hugegraph.exception.ConnectionException;
import org.apache.hugegraph.type.HugeType;
import org.apache.hugegraph.util.E;
import org.apache.hugegraph.util.Log;

public abstract class HbaseStore extends AbstractBackendStore<HbaseSessions.Session> {

    private static final Logger LOG = Log.logger(HbaseStore.class);

    private final BackendFeatures features;

    private final String store;
    private final String namespace;

    private final BackendStoreProvider provider;
    private final Map<HugeType, HbaseTable> tables;
    private short vertexLogicPartitions;
    private short edgeLogicPartitions;

    private HbaseSessions sessions;

    private final ReadWriteLock storeLock;

    public HbaseStore(BackendStoreProvider provider,
                      String namespace, String store, boolean enablePartition) {
        this.tables = new HashMap<>();

        this.provider = provider;
        this.namespace = namespace;
        this.store = store;
        this.sessions = null;
        this.storeLock = new ReentrantReadWriteLock();
        this.features = new HbaseFeatures(enablePartition);

        this.registerMetaHandlers();
        LOG.debug("Store loaded: {}", store);
    }

    private void registerMetaHandlers() {
        this.registerMetaHandler("metrics", (session, meta, args) -> {
            HbaseMetrics metrics = new HbaseMetrics(this.sessions);
            return metrics.metrics();
        });

        this.registerMetaHandler("compact", (session, meta, args) -> {
            HbaseMetrics metrics = new HbaseMetrics(this.sessions);
            return metrics.compact(this.tableNames());
        });
    }

    protected void registerTableManager(HugeType type, HbaseTable table) {
        this.tables.put(type, table);
    }

    @Override
    protected final HbaseTable table(HugeType type) {
        assert type != null;
        HbaseTable table = this.tables.get(type);
        if (table == null) {
            throw new BackendException("Unsupported table type: %s", type);
        }
        return table;
    }

    @Override
    protected HbaseSessions.Session session(HugeType type) {
        this.checkOpened();
        return this.sessions.session();
    }

    protected List<String> tableNames() {
        return this.tables.values().stream().map(t -> t.table())
                                            .collect(Collectors.toList());
    }

    public String namespace() {
        return this.namespace;
    }

    @Override
    public String store() {
        return this.store;
    }

    @Override
    public String database() {
        return this.namespace;
    }

    @Override
    public BackendStoreProvider provider() {
        return this.provider;
    }

    @Override
    public BackendFeatures features() {
        return features;
    }

    @Override
    public synchronized void open(HugeConfig config) {
        E.checkNotNull(config, "config");
        this.vertexLogicPartitions = config.get(HbaseOptions.HBASE_VERTEX_PARTITION).shortValue();
        this.edgeLogicPartitions = config.get(HbaseOptions.HBASE_EDGE_PARTITION).shortValue();

        if (this.sessions == null) {
            this.sessions = new HbaseSessions(config, this.namespace, this.store);
        }

        assert this.sessions != null;
        if (!this.sessions.closed()) {
            LOG.debug("Store {} has been opened before", this.store);
            this.sessions.useSession();
            return;
        }

        try {
            // NOTE: won't throw error even if connection refused
            this.sessions.open();
        } catch (Throwable e) {
            LOG.error("Failed to open HBase '{}'", this.store, e);
            throw new ConnectionException("Failed to connect to HBase", e);
        }

        this.sessions.session().open();
        LOG.debug("Store opened: {}", this.store);
    }

    @Override
    public void close() {
        this.checkOpened();
        this.sessions.close();

        LOG.debug("Store closed: {}", this.store);
    }

    @Override
    public boolean opened() {
        this.checkConnectionOpened();
        return this.sessions.session().opened();
    }

    @Override
    public void mutate(BackendMutation mutation) {
        if (LOG.isDebugEnabled()) {
            LOG.debug("Store {} mutation: {}", this.store, mutation);
        }

        this.checkOpened();
        HbaseSessions.Session session = this.sessions.session();

        for (Iterator<BackendAction> it = mutation.mutation(); it.hasNext();) {
            this.mutate(session, it.next());
        }
    }

    private void mutate(HbaseSessions.Session session, BackendAction item) {
        BackendEntry entry = item.entry();
        HbaseTable table = this.table(entry.type());

        switch (item.action()) {
            case INSERT:
                table.insert(session, entry);
                break;
            case DELETE:
                table.delete(session, entry);
                break;
            case APPEND:
                table.append(session, entry);
                break;
            case ELIMINATE:
                table.eliminate(session, entry);
                break;
            case UPDATE_IF_PRESENT:
                table.updateIfPresent(session, entry);
                break;
            case UPDATE_IF_ABSENT:
                table.updateIfAbsent(session, entry);
                break;
            default:
                throw new AssertionError(String.format(
                          "Unsupported mutate action: %s", item.action()));
        }
    }

    @Override
    public Iterator<BackendEntry> query(Query query) {
        this.checkOpened();

        HbaseSessions.Session session = this.sessions.session();
        HbaseTable table = this.table(HbaseTable.tableType(query));
        return table.query(session, query);
    }

    /**
     * 1. 单独实现一种批量 Scan 逻辑
     * 2. 返回值为双层迭代器
     * @param queries
     * @param queryWriter
     * @param hugeGraph
     * @return
     */
    @Override
    public Iterator<Iterator<BackendEntry>> query(Iterator<Query> queries,
                                        Function<Query, Query> queryWriter,
                                        HugeGraph hugeGraph) {
        this.checkOpened();
        if (queries == null || !queries.hasNext()) {
            return new LinkedList<Iterator<BackendEntry>>().iterator();
        }
        //TODO: support batch query
        //queries 是从VertexStep 中传递过来的 Query 集合迭代器
        //TODO: transform Query to IdPrefixQuery
        //transform Query to IdPrefixQuery
        class QueryWrapper implements Iterator<IdPrefixQuery> {
            Query first;
            Iterator<Query> queries;
            Iterator<Id> subEls;
            Query preQuery;
            Iterator<IdPrefixQuery> queryListIterator;

            QueryWrapper(Iterator<Query> queries, Query first) {
                this.queries = queries;
                this.first = first;
            }

            @Override
            public boolean hasNext() {
                return first != null || (this.subEls != null && this.subEls.hasNext())
                    || (queryListIterator != null && queryListIterator.hasNext()) ||
                    queries.hasNext();
            }

            @Override
            public IdPrefixQuery next() {
                if (queryListIterator != null && queryListIterator.hasNext()) {
                    return queryListIterator.next();
                }

                Query q;
                if (first != null) {
                    q = first;
                    preQuery = q.copy();
                    first = null;
                } else {
                    if (this.subEls == null || !this.subEls.hasNext()) {
                        q = queries.next();
                        preQuery = q.copy();
                    } else {
                        q = preQuery.copy();
                    }
                }

                assert q instanceof ConditionQuery;
                ConditionQuery cq = (ConditionQuery) q;
                ConditionQuery originQuery = (ConditionQuery) q.copy();

                List<IdPrefixQuery> queryList = Lists.newArrayList();
                if (hugeGraph != null) {
                    for (ConditionQuery conditionQuery : ConditionQueryFlatten.flatten(cq)) {
                        Id label = conditionQuery.condition(HugeKeys.LABEL);

                        HugeType hugeType = conditionQuery.resultType();
                        if (hugeType != null && hugeType.isEdge() &&
                            !conditionQuery.conditions().isEmpty()) {

                            IdPrefixQuery idPrefixQuery = (IdPrefixQuery) queryWriter.apply(conditionQuery);
                            idPrefixQuery.setOriginQuery(originQuery);
                            queryList.add(idPrefixQuery);

                        }
                    }

                    queryListIterator = queryList.iterator();
                    if (queryListIterator.hasNext()) {
                        return queryListIterator.next();
                    }
                }

                Id ownerId = cq.condition(HugeKeys.OWNER_VERTEX);
                assert ownerId != null;
                BytesBuffer buffer = BytesBuffer.allocate(BytesBuffer.BUF_EDGE_ID);
                buffer.writeId(ownerId);

                //TODO: BinaryBackendEntry 修改成了  Public 其实此逻辑在此处没有用处，主要是先调通并发逻辑
                return new IdPrefixQuery(cq, new BinaryBackendEntry.BinaryId(
                    buffer.bytes(), ownerId));
            }

            private boolean matchEdgeSortKeys(ConditionQuery query,
                                              boolean matchAll,
                                              HugeGraph graph) {
                assert query.resultType().isEdge();
                Id label = query.condition(HugeKeys.LABEL);
                if (label == null) {
                    return false;
                }
                List<Id> sortKeys = graph.edgeLabel(label).sortKeys();
                if (sortKeys.isEmpty()) {
                    return false;
                }
                Set<Id> queryKeys = query.userpropKeys();
                for (int i = sortKeys.size(); i > 0; i--) {
                    List<Id> subFields = sortKeys.subList(0, i);
                    if (queryKeys.containsAll(subFields)) {
                        if (queryKeys.size() == subFields.size() || !matchAll) {
                            /*
                             * Return true if:
                             * matchAll=true and all queryKeys are in sortKeys
                             *  or
                             * partial queryKeys are in sortKeys
                             */
                            return true;
                        }
                    }
                }
                return false;
            }
        }
        Query first = queries.next();

        //Query 转换成 PrefixFilter
        QueryWrapper idPrefixQueries = new QueryWrapper(queries, first);

        return query(idPrefixQueries);
    }


    public Iterator<Iterator<BackendEntry>> query(Iterator<IdPrefixQuery> queries) {
        Lock readLock = this.storeLock.readLock();
        readLock.lock();
        try {
            this.checkOpened();
            HbaseSessions.HbaseSession session = this.sessions.session();
            // 此处用 Next 丢失
            //HbaseTable table = this.table(HbaseTable.tableType(queries.next()));
            HbaseTable table = this.table(HugeType.EDGE_OUT);// hard code TODO: add tableType

            Iterator<Iterator<BackendEntry>> iterators = table.query(session, queries, table.table());

            return iterators;
        } finally {
            readLock.unlock();
        }
    }

    @Override
    public Number queryNumber(Query query) {
        this.checkOpened();

        HbaseSessions.Session session = this.sessions.session();
        HbaseTable table = this.table(HbaseTable.tableType(query));
        return table.queryNumber(session, query);
    }

    @Override
    public void init() {
        this.checkConnectionOpened();

        // Create namespace
        try {
            this.sessions.createNamespace();
        } catch (NamespaceExistException ignored) {
            // Ignore due to both schema & graph store would create namespace
        } catch (IOException e) {
            throw new BackendException(
                      "Failed to create namespace '%s' for '%s' store",
                      e, this.namespace, this.store);
        }

        // Create tables
        for (String table : this.tableNames()) {
            try {
                if (table.equals("g_oe") || table.equals("g_ie")) {
                    this.sessions.createPreSplitTable(table, HbaseTable.cfs(),
                        this.edgeLogicPartitions);
                } else if (table.equals("g_v")) {
                    this.sessions.createPreSplitTable(table, HbaseTable.cfs(), 
                        this.vertexLogicPartitions);
                } else {
                    this.sessions.createTable(table, HbaseTable.cfs());
                }

            } catch (TableExistsException ignored) {
                continue;
            } catch (IOException e) {
                throw new BackendException(
                          "Failed to create table '%s' for '%s' store",
                          e, table, this.store);
            }
        }

        LOG.debug("Store initialized: {}", this.store);
    }

    @Override
    public void clear(boolean clearSpace) {
        this.checkConnectionOpened();

        // Return if not exists namespace
        try {
            if (!this.sessions.existsNamespace()) {
                return;
            }
        } catch (IOException e) {
            throw new BackendException(
                      "Exception when checking for the existence of '%s'",
                      e, this.namespace);
        }

        if (!clearSpace) {
            // Drop tables
            for (String table : this.tableNames()) {
                try {
                    this.sessions.dropTable(table);
                } catch (TableNotFoundException e) {
                    LOG.warn("The table '{}' of '{}' store does not exist " +
                             "when trying to drop", table, this.store);
                } catch (IOException e) {
                    throw new BackendException(
                              "Failed to drop table '%s' of '%s' store",
                              e, table, this.store);
                }
            }
        } else {
            // Drop namespace
            try {
                this.sessions.dropNamespace();
            } catch (IOException e) {
                String notEmpty = "Only empty namespaces can be removed";
                if (e.getCause().getMessage().contains(notEmpty)) {
                    LOG.debug("Can't drop namespace '{}': {}",
                              this.namespace, e);
                } else {
                    throw new BackendException(
                              "Failed to drop namespace '%s' of '%s' store",
                              e, this.namespace, this.store);
                }
            }
        }

        LOG.debug("Store cleared: {}", this.store);
    }

    @Override
    public boolean initialized() {
        this.checkConnectionOpened();

        try {
            if (!this.sessions.existsNamespace()) {
                return false;
            }
            for (String table : this.tableNames()) {
                if (!this.sessions.existsTable(table)) {
                    return false;
                }
            }
        } catch (IOException e) {
            throw new BackendException("Failed to obtain table info", e);
        }
        return true;
    }

    @Override
    public void truncate() {
        this.checkOpened();

        // Total time may cost 3 * TRUNCATE_TIMEOUT, due to there are 3 stores
        long timeout = this.sessions.config().get(HbaseOptions.TRUNCATE_TIMEOUT);
        long start = System.currentTimeMillis();

        BiFunction<String, Future<Void>, Void> wait = (table, future) -> {
            long elapsed = System.currentTimeMillis() - start;
            long remainingTime = timeout - elapsed / 1000L;
            try {
                return future.get(remainingTime, TimeUnit.SECONDS);
            } catch (Exception e) {
                throw new BackendException(
                          "Error when truncating table '%s' of '%s' store: %s",
                          table, this.store, e.toString());
            }
        };

        // Truncate tables
        List<String> tables = this.tableNames();
        Map<String, Future<Void>> futures = new HashMap<>(tables.size());

        try {
            // Disable tables async
            for (String table : tables) {
                futures.put(table, this.sessions.disableTableAsync(table));
            }
            for (Map.Entry<String, Future<Void>> entry : futures.entrySet()) {
                wait.apply(entry.getKey(), entry.getValue());
            }
        } catch (Exception e) {
            this.enableTables();
            throw new BackendException(
                      "Failed to disable table for '%s' store", e, this.store);
        }

        try {
            // Truncate tables async
            for (String table : tables) {
                futures.put(table, this.sessions.truncateTableAsync(table));
            }
            for (Map.Entry<String, Future<Void>> entry : futures.entrySet()) {
                wait.apply(entry.getKey(), entry.getValue());
            }
        } catch (Exception e) {
            this.enableTables();
            throw new BackendException(
                      "Failed to truncate table for '%s' store", e, this.store);
        }

        LOG.debug("Store truncated: {}", this.store);
    }

    private void enableTables() {
        for (String table : this.tableNames()) {
            try {
                this.sessions.enableTable(table);
            } catch (Exception e) {
                LOG.warn("Failed to enable table '{}' of '{}' store",
                         table, this.store, e);
            }
        }
    }

    @Override
    public void beginTx() {
        // pass
    }

    @Override
    public void commitTx() {
        this.checkOpened();
        HbaseSessions.Session session = this.sessions.session();

        session.commit();
    }

    @Override
    public void rollbackTx() {
        this.checkOpened();
        HbaseSessions.Session session = this.sessions.session();

        session.rollback();
    }

    private void checkConnectionOpened() {
        E.checkState(this.sessions != null && this.sessions.opened(),
                     "HBase store has not been initialized");
    }

    /***************************** Store defines *****************************/

    public static class HbaseSchemaStore extends HbaseStore {

        private final HbaseTables.Counters counters;

        public HbaseSchemaStore(HugeConfig config, BackendStoreProvider provider,
                                String namespace, String store) {
            super(provider, namespace, store, 
                  config.get(HbaseOptions.HBASE_ENABLE_PARTITION).booleanValue());

            this.counters = new HbaseTables.Counters();

            registerTableManager(HugeType.VERTEX_LABEL,
                                 new HbaseTables.VertexLabel());
            registerTableManager(HugeType.EDGE_LABEL,
                                 new HbaseTables.EdgeLabel());
            registerTableManager(HugeType.PROPERTY_KEY,
                                 new HbaseTables.PropertyKey());
            registerTableManager(HugeType.INDEX_LABEL,
                                 new HbaseTables.IndexLabel());

            registerTableManager(HugeType.SECONDARY_INDEX,
                                 new HbaseTables.SecondaryIndex(store));
        }

        @Override
        protected List<String> tableNames() {
            List<String> tableNames = super.tableNames();
            tableNames.add(this.counters.table());
            return tableNames;
        }

        @Override
        public void increaseCounter(HugeType type, long increment) {
            super.checkOpened();
            this.counters.increaseCounter(super.sessions.session(),
                                          type, increment);
        }

        @Override
        public long getCounter(HugeType type) {
            super.checkOpened();
            return this.counters.getCounter(super.sessions.session(), type);
        }

        @Override
        public boolean isSchemaStore() {
            return true;
        }
    }

    public static class HbaseGraphStore extends HbaseStore {
        private boolean enablePartition;
        public HbaseGraphStore(HugeConfig config, BackendStoreProvider provider,
                               String namespace, String store) {
            super(provider, namespace, store, 
                  config.get(HbaseOptions.HBASE_ENABLE_PARTITION).booleanValue());
            this.enablePartition = config.get(HbaseOptions.HBASE_ENABLE_PARTITION).booleanValue();
            registerTableManager(HugeType.VERTEX,
                                 new HbaseTables.Vertex(store, enablePartition));

            registerTableManager(HugeType.EDGE_OUT,
                                 HbaseTables.Edge.out(store, enablePartition));
            registerTableManager(HugeType.EDGE_IN,
                                 HbaseTables.Edge.in(store, enablePartition));

            registerTableManager(HugeType.SECONDARY_INDEX,
                                 new HbaseTables.SecondaryIndex(store));
            registerTableManager(HugeType.VERTEX_LABEL_INDEX,
                                 new HbaseTables.VertexLabelIndex(store));
            registerTableManager(HugeType.EDGE_LABEL_INDEX,
                                 new HbaseTables.EdgeLabelIndex(store));
            registerTableManager(HugeType.RANGE_INT_INDEX,
                                 HbaseTables.RangeIndex.rangeInt(store));
            registerTableManager(HugeType.RANGE_FLOAT_INDEX,
                                 HbaseTables.RangeIndex.rangeFloat(store));
            registerTableManager(HugeType.RANGE_LONG_INDEX,
                                 HbaseTables.RangeIndex.rangeLong(store));
            registerTableManager(HugeType.RANGE_DOUBLE_INDEX,
                                 HbaseTables.RangeIndex.rangeDouble(store));
            registerTableManager(HugeType.SEARCH_INDEX,
                                 new HbaseTables.SearchIndex(store));
            registerTableManager(HugeType.SHARD_INDEX,
                                 new HbaseTables.ShardIndex(store));
            registerTableManager(HugeType.UNIQUE_INDEX,
                                 new HbaseTables.UniqueIndex(store));
        }

        @Override
        public boolean isSchemaStore() {
            return false;
        }

        @Override
        public Id nextId(HugeType type) {
            throw new UnsupportedOperationException(
                      "HbaseGraphStore.nextId()");
        }

        @Override
        public void increaseCounter(HugeType type, long num) {
            throw new UnsupportedOperationException(
                      "HbaseGraphStore.increaseCounter()");
        }

        @Override
        public long getCounter(HugeType type) {
            throw new UnsupportedOperationException(
                      "HbaseGraphStore.getCounter()");
        }
    }

    public static class HbaseSystemStore extends HbaseGraphStore {

        private final HbaseTables.Meta meta;

        public HbaseSystemStore(HugeConfig config, BackendStoreProvider provider,
                                String namespace, String store) {
            super(config, provider, namespace, store);

            this.meta = new HbaseTables.Meta();
        }

        @Override
        protected List<String> tableNames() {
            List<String> tableNames = super.tableNames();
            tableNames.add(this.meta.table());
            return tableNames;
        }

        @Override
        public void init() {
            super.init();
            HbaseSessions.Session session = super.session(null);
            String driverVersion = this.provider().driverVersion();
            this.meta.writeVersion(session, driverVersion);
            LOG.info("Write down the backend version: {}", driverVersion);
        }

        @Override
        public String storedVersion() {
            HbaseSessions.Session session = super.session(null);
            return this.meta.readVersion(session);
        }
    }
}
