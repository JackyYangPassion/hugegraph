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

package org.apache.hugegraph.store.node.metrics;

import io.micrometer.core.instrument.MeterRegistry;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.scheduling.annotation.Scheduled;
import org.springframework.stereotype.Component;

@Component
public class MetricsRefresher {
    private static final Logger log = LoggerFactory.getLogger(MetricsRefresher.class);

    private final MeterRegistry registry;

    public MetricsRefresher(MeterRegistry registry) {
        this.registry = registry;
    }

    @Scheduled(fixedRate = 30000) // 每30秒刷新一次
    public void refreshMetrics() {
        log.info("Refreshing metrics");
        JRaftMetrics.init(registry,false);
        // 其他指标的初始化
    }
}
