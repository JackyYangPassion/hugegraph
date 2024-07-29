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

package com.codahale.metrics;


import com.codahale.metrics.jmx.DefaultObjectNameFactory;
import com.codahale.metrics.jmx.ObjectNameFactory;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.management.*;
import java.io.Closeable;
import java.lang.management.ManagementFactory;
import java.util.Collections;
import java.util.Iterator;
import java.util.Locale;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.TimeUnit;


public class JmxReporter implements Reporter, Closeable  {
    private static final Logger LOGGER = LoggerFactory.getLogger(JmxReporter.class);
    private final MetricRegistry registry;
    private final JmxListener listener;


    public static Builder forRegistry(MetricRegistry registry) {
        return new Builder(registry);
    }


    private JmxReporter(MBeanServer mBeanServer, String domain, MetricRegistry registry, MetricFilter filter, MetricTimeUnits timeUnits, ObjectNameFactory objectNameFactory) {
        this.registry = registry;
        this.listener = new JmxListener(mBeanServer, domain, filter, timeUnits, objectNameFactory);
    }


    public void start() {
        this.registry.addListener(this.listener);
    }


    public void stop() {
        this.registry.removeListener(this.listener);
        this.listener.unregisterAll();
    }


    public void close() {
        this.stop();
    }


    ObjectNameFactory getObjectNameFactory() {
        return this.listener.objectNameFactory;
    }


    private static class MetricTimeUnits {
        private final TimeUnit defaultRate;
        private final TimeUnit defaultDuration;
        private final Map<String, TimeUnit> rateOverrides;
        private final Map<String, TimeUnit> durationOverrides;


        MetricTimeUnits(TimeUnit defaultRate, TimeUnit defaultDuration, Map<String, TimeUnit> rateOverrides, Map<String, TimeUnit> durationOverrides) {
            this.defaultRate = defaultRate;
            this.defaultDuration = defaultDuration;
            this.rateOverrides = rateOverrides;
            this.durationOverrides = durationOverrides;
        }


        public TimeUnit durationFor(String name) {
            return (TimeUnit)this.durationOverrides.getOrDefault(name, this.defaultDuration);
        }


        public TimeUnit rateFor(String name) {
            return (TimeUnit)this.rateOverrides.getOrDefault(name, this.defaultRate);
        }
    }


    private static class JmxListener implements MetricRegistryListener {
        private final String name;
        private final MBeanServer mBeanServer;
        private final MetricFilter filter;
        private final MetricTimeUnits timeUnits;
        private final Map<ObjectName, ObjectName> registered;
        private final ObjectNameFactory objectNameFactory;


        private JmxListener(MBeanServer mBeanServer, String name, MetricFilter filter, MetricTimeUnits timeUnits, ObjectNameFactory objectNameFactory) {
            this.mBeanServer = mBeanServer;
            this.name = name;
            this.filter = filter;
            this.timeUnits = timeUnits;
            this.registered = new ConcurrentHashMap();
            this.objectNameFactory = objectNameFactory;
        }


        private void registerMBean(Object mBean, ObjectName objectName) throws InstanceAlreadyExistsException, JMException {
            ObjectInstance objectInstance = this.mBeanServer.registerMBean(mBean, objectName);
            if (objectInstance != null) {
                this.registered.put(objectName, objectInstance.getObjectName());
            } else {
                this.registered.put(objectName, objectName);
            }


        }


        private void unregisterMBean(ObjectName originalObjectName) throws InstanceNotFoundException, MBeanRegistrationException {
            ObjectName storedObjectName = (ObjectName)this.registered.remove(originalObjectName);
            if (storedObjectName != null) {
                this.mBeanServer.unregisterMBean(storedObjectName);
            } else {
                this.mBeanServer.unregisterMBean(originalObjectName);
            }


        }


        public void onGaugeAdded(String name, Gauge<?> gauge) {
            try {
                if (this.filter.matches(name, gauge)) {
                    ObjectName objectName = this.createName("gauges", name);
                    this.registerMBean(new JmxGauge(gauge, objectName), objectName);
                }
            } catch (InstanceAlreadyExistsException var4) {
                JmxReporter.LOGGER.debug("Unable to register gauge", var4);
            } catch (JMException var5) {
                JmxReporter.LOGGER.warn("Unable to register gauge", var5);
            }


        }


        public void onGaugeRemoved(String name) {
            try {
                ObjectName objectName = this.createName("gauges", name);
                this.unregisterMBean(objectName);
            } catch (InstanceNotFoundException var3) {
                JmxReporter.LOGGER.debug("Unable to unregister gauge", var3);
            } catch (MBeanRegistrationException var4) {
                JmxReporter.LOGGER.warn("Unable to unregister gauge", var4);
            }


        }


        public void onCounterAdded(String name, Counter counter) {
            try {
                if (this.filter.matches(name, counter)) {
                    ObjectName objectName = this.createName("counters", name);
                    this.registerMBean(new JmxCounter(counter, objectName), objectName);
                }
            } catch (InstanceAlreadyExistsException var4) {
                JmxReporter.LOGGER.debug("Unable to register counter", var4);
            } catch (JMException var5) {
                JmxReporter.LOGGER.warn("Unable to register counter", var5);
            }


        }


        public void onCounterRemoved(String name) {
            try {
                ObjectName objectName = this.createName("counters", name);
                this.unregisterMBean(objectName);
            } catch (InstanceNotFoundException var3) {
                JmxReporter.LOGGER.debug("Unable to unregister counter", var3);
            } catch (MBeanRegistrationException var4) {
                JmxReporter.LOGGER.warn("Unable to unregister counter", var4);
            }


        }


        public void onHistogramAdded(String name, Histogram histogram) {
            try {
                if (this.filter.matches(name, histogram)) {
                    ObjectName objectName = this.createName("histograms", name);
                    this.registerMBean(new JmxHistogram(histogram, objectName), objectName);
                }
            } catch (InstanceAlreadyExistsException var4) {
                JmxReporter.LOGGER.debug("Unable to register histogram", var4);
            } catch (JMException var5) {
                JmxReporter.LOGGER.warn("Unable to register histogram", var5);
            }


        }


        public void onHistogramRemoved(String name) {
            try {
                ObjectName objectName = this.createName("histograms", name);
                this.unregisterMBean(objectName);
            } catch (InstanceNotFoundException var3) {
                JmxReporter.LOGGER.debug("Unable to unregister histogram", var3);
            } catch (MBeanRegistrationException var4) {
                JmxReporter.LOGGER.warn("Unable to unregister histogram", var4);
            }


        }


        public void onMeterAdded(String name, Meter meter) {
            try {
                if (this.filter.matches(name, meter)) {
                    ObjectName objectName = this.createName("meters", name);
                    this.registerMBean(new JmxMeter(meter, objectName, this.timeUnits.rateFor(name)), objectName);
                }
            } catch (InstanceAlreadyExistsException var4) {
                JmxReporter.LOGGER.debug("Unable to register meter", var4);
            } catch (JMException var5) {
                JmxReporter.LOGGER.warn("Unable to register meter", var5);
            }


        }


        public void onMeterRemoved(String name) {
            try {
                ObjectName objectName = this.createName("meters", name);
                this.unregisterMBean(objectName);
            } catch (InstanceNotFoundException var3) {
                JmxReporter.LOGGER.debug("Unable to unregister meter", var3);
            } catch (MBeanRegistrationException var4) {
                JmxReporter.LOGGER.warn("Unable to unregister meter", var4);
            }


        }


        public void onTimerAdded(String name, Timer timer) {
            try {
                if (this.filter.matches(name, timer)) {
                    ObjectName objectName = this.createName("timers", name);
                    this.registerMBean(new JmxTimer(timer, objectName, this.timeUnits.rateFor(name), this.timeUnits.durationFor(name)), objectName);
                }
            } catch (InstanceAlreadyExistsException var4) {
                JmxReporter.LOGGER.debug("Unable to register timer", var4);
            } catch (JMException var5) {
                JmxReporter.LOGGER.warn("Unable to register timer", var5);
            }


        }


        public void onTimerRemoved(String name) {
            try {
                ObjectName objectName = this.createName("timers", name);
                this.unregisterMBean(objectName);
            } catch (InstanceNotFoundException var3) {
                JmxReporter.LOGGER.debug("Unable to unregister timer", var3);
            } catch (MBeanRegistrationException var4) {
                JmxReporter.LOGGER.warn("Unable to unregister timer", var4);
            }


        }


        private ObjectName createName(String type, String name) {
            return this.objectNameFactory.createName(type, this.name, name);
        }


        void unregisterAll() {
            Iterator var1 = this.registered.keySet().iterator();


            while(var1.hasNext()) {
                ObjectName name = (ObjectName)var1.next();


                try {
                    this.unregisterMBean(name);
                } catch (InstanceNotFoundException var4) {
                    JmxReporter.LOGGER.debug("Unable to unregister metric", var4);
                } catch (MBeanRegistrationException var5) {
                    JmxReporter.LOGGER.warn("Unable to unregister metric", var5);
                }
            }


            this.registered.clear();
        }
    }


    static class JmxTimer extends JmxMeter implements JmxTimerMBean {
        private final Timer metric;
        private final double durationFactor;
        private final String durationUnit;


        private JmxTimer(Timer metric, ObjectName objectName, TimeUnit rateUnit, TimeUnit durationUnit) {
            super(metric, objectName, rateUnit);
            this.metric = metric;
            this.durationFactor = 1.0 / (double)durationUnit.toNanos(1L);
            this.durationUnit = durationUnit.toString().toLowerCase(Locale.US);
        }


        public double get50thPercentile() {
            return this.metric.getSnapshot().getMedian() * this.durationFactor;
        }


        public double getMin() {
            return (double)this.metric.getSnapshot().getMin() * this.durationFactor;
        }


        public double getMax() {
            return (double)this.metric.getSnapshot().getMax() * this.durationFactor;
        }


        public double getMean() {
            return this.metric.getSnapshot().getMean() * this.durationFactor;
        }


        public double getStdDev() {
            return this.metric.getSnapshot().getStdDev() * this.durationFactor;
        }


        public double get75thPercentile() {
            return this.metric.getSnapshot().get75thPercentile() * this.durationFactor;
        }


        public double get95thPercentile() {
            return this.metric.getSnapshot().get95thPercentile() * this.durationFactor;
        }


        public double get98thPercentile() {
            return this.metric.getSnapshot().get98thPercentile() * this.durationFactor;
        }


        public double get99thPercentile() {
            return this.metric.getSnapshot().get99thPercentile() * this.durationFactor;
        }


        public double get999thPercentile() {
            return this.metric.getSnapshot().get999thPercentile() * this.durationFactor;
        }


        public long[] values() {
            return this.metric.getSnapshot().getValues();
        }


        public String getDurationUnit() {
            return this.durationUnit;
        }
    }


    public interface JmxTimerMBean extends JmxMeterMBean {
        double getMin();


        double getMax();


        double getMean();


        double getStdDev();


        double get50thPercentile();


        double get75thPercentile();


        double get95thPercentile();


        double get98thPercentile();


        double get99thPercentile();


        double get999thPercentile();


        long[] values();


        String getDurationUnit();
    }


    private static class JmxMeter extends AbstractBean implements JmxMeterMBean {
        private final Metered metric;
        private final double rateFactor;
        private final String rateUnit;


        private JmxMeter(Metered metric, ObjectName objectName, TimeUnit rateUnit) {
            super(objectName);
            this.metric = metric;
            this.rateFactor = (double)rateUnit.toSeconds(1L);
            this.rateUnit = ("events/" + this.calculateRateUnit(rateUnit)).intern();
        }


        public long getCount() {
            return this.metric.getCount();
        }


        public double getMeanRate() {
            return this.metric.getMeanRate() * this.rateFactor;
        }


        public double getOneMinuteRate() {
            return this.metric.getOneMinuteRate() * this.rateFactor;
        }


        public double getFiveMinuteRate() {
            return this.metric.getFiveMinuteRate() * this.rateFactor;
        }


        public double getFifteenMinuteRate() {
            return this.metric.getFifteenMinuteRate() * this.rateFactor;
        }


        public String getRateUnit() {
            return this.rateUnit;
        }


        private String calculateRateUnit(TimeUnit unit) {
            String s = unit.toString().toLowerCase(Locale.US);
            return s.substring(0, s.length() - 1);
        }
    }


    public interface JmxMeterMBean extends MetricMBean {
        long getCount();


        double getMeanRate();


        double getOneMinuteRate();


        double getFiveMinuteRate();


        double getFifteenMinuteRate();


        String getRateUnit();
    }


    private static class JmxHistogram implements JmxHistogramMBean {
        private final ObjectName objectName;
        private final Histogram metric;


        private JmxHistogram(Histogram metric, ObjectName objectName) {
            this.metric = metric;
            this.objectName = objectName;
        }


        public ObjectName objectName() {
            return this.objectName;
        }


        public double get50thPercentile() {
            return this.metric.getSnapshot().getMedian();
        }


        public long getCount() {
            return this.metric.getCount();
        }


        public long getMin() {
            return this.metric.getSnapshot().getMin();
        }


        public long getMax() {
            return this.metric.getSnapshot().getMax();
        }


        public double getMean() {
            return this.metric.getSnapshot().getMean();
        }


        public double getStdDev() {
            return this.metric.getSnapshot().getStdDev();
        }


        public double get75thPercentile() {
            return this.metric.getSnapshot().get75thPercentile();
        }


        public double get95thPercentile() {
            return this.metric.getSnapshot().get95thPercentile();
        }


        public double get98thPercentile() {
            return this.metric.getSnapshot().get98thPercentile();
        }


        public double get99thPercentile() {
            return this.metric.getSnapshot().get99thPercentile();
        }


        public double get999thPercentile() {
            return this.metric.getSnapshot().get999thPercentile();
        }


        public long[] values() {
            return this.metric.getSnapshot().getValues();
        }


        public long getSnapshotSize() {
            return (long)this.metric.getSnapshot().size();
        }
    }


    public interface JmxHistogramMBean extends MetricMBean {
        long getCount();


        long getMin();


        long getMax();


        double getMean();


        double getStdDev();


        double get50thPercentile();


        double get75thPercentile();


        double get95thPercentile();


        double get98thPercentile();


        double get99thPercentile();


        double get999thPercentile();


        long[] values();


        long getSnapshotSize();
    }


    private static class JmxCounter extends AbstractBean implements JmxCounterMBean {
        private final Counter metric;


        private JmxCounter(Counter metric, ObjectName objectName) {
            super(objectName);
            this.metric = metric;
        }


        public long getCount() {
            return this.metric.getCount();
        }
    }


    public interface JmxCounterMBean extends MetricMBean {
        long getCount();
    }


    private static class JmxGauge extends AbstractBean implements JmxGaugeMBean {
        private final Gauge<?> metric;


        private JmxGauge(Gauge<?> metric, ObjectName objectName) {
            super(objectName);
            this.metric = metric;
        }


        public Object getValue() {
            return this.metric.getValue();
        }


        public Number getNumber() {
            Object value = this.metric.getValue();
            return (Number)(value instanceof Number ? (Number)value : 0);
        }
    }


    public interface JmxGaugeMBean extends MetricMBean {
        Object getValue();


        Number getNumber();
    }


    private abstract static class AbstractBean implements MetricMBean {
        private final ObjectName objectName;


        AbstractBean(ObjectName objectName) {
            this.objectName = objectName;
        }


        public ObjectName objectName() {
            return this.objectName;
        }
    }


    public interface MetricMBean {
        ObjectName objectName();
    }


    public static class Builder {
        private final MetricRegistry registry;
        private MBeanServer mBeanServer;
        private TimeUnit rateUnit;
        private TimeUnit durationUnit;
        private ObjectNameFactory objectNameFactory;
        private MetricFilter filter;
        private String domain;
        private Map<String, TimeUnit> specificDurationUnits;
        private Map<String, TimeUnit> specificRateUnits;


        private Builder(MetricRegistry registry) {
            this.filter = MetricFilter.ALL;
            this.registry = registry;
            this.rateUnit = TimeUnit.SECONDS;
            this.durationUnit = TimeUnit.MILLISECONDS;
            this.domain = "metrics";
            this.objectNameFactory = new DefaultObjectNameFactory();
            this.specificDurationUnits = Collections.emptyMap();
            this.specificRateUnits = Collections.emptyMap();
        }


        public Builder registerWith(MBeanServer mBeanServer) {
            this.mBeanServer = mBeanServer;
            return this;
        }


        public Builder convertRatesTo(TimeUnit rateUnit) {
            this.rateUnit = rateUnit;
            return this;
        }


        public Builder createsObjectNamesWith(ObjectNameFactory onFactory) {
            if (onFactory == null) {
                throw new IllegalArgumentException("null objectNameFactory");
            } else {
                this.objectNameFactory = onFactory;
                return this;
            }
        }


        public Builder convertDurationsTo(TimeUnit durationUnit) {
            this.durationUnit = durationUnit;
            return this;
        }


        public Builder filter(MetricFilter filter) {
            this.filter = filter;
            return this;
        }


        public Builder inDomain(String domain) {
            this.domain = domain;
            return this;
        }


        public Builder specificDurationUnits(Map<String, TimeUnit> specificDurationUnits) {
            this.specificDurationUnits = Collections.unmodifiableMap(specificDurationUnits);
            return this;
        }


        public Builder specificRateUnits(Map<String, TimeUnit> specificRateUnits) {
            this.specificRateUnits = Collections.unmodifiableMap(specificRateUnits);
            return this;
        }


        public JmxReporter build() {
            MetricTimeUnits timeUnits = new MetricTimeUnits(this.rateUnit, this.durationUnit, this.specificRateUnits, this.specificDurationUnits);
            if (this.mBeanServer == null) {
                this.mBeanServer = ManagementFactory.getPlatformMBeanServer();
            }


            return new JmxReporter(this.mBeanServer, this.domain, this.registry, this.filter, timeUnits, this.objectNameFactory);
        }
    }
}
