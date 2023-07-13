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

package org.apache.hugegraph.traversal.optimize;

import java.util.List;
import java.util.Objects;

import org.apache.hugegraph.HugeGraph;
import org.apache.tinkerpop.gremlin.process.traversal.Step;
import org.apache.tinkerpop.gremlin.process.traversal.Traversal;
import org.apache.tinkerpop.gremlin.process.traversal.TraversalStrategy.ProviderOptimizationStrategy;
import org.apache.tinkerpop.gremlin.process.traversal.step.TraversalParent;
import org.apache.tinkerpop.gremlin.process.traversal.step.filter.RangeGlobalStep;
import org.apache.tinkerpop.gremlin.process.traversal.step.map.EdgeOtherVertexStep;
import org.apache.tinkerpop.gremlin.process.traversal.step.map.PathStep;
import org.apache.tinkerpop.gremlin.process.traversal.step.map.TreeStep;
import org.apache.tinkerpop.gremlin.process.traversal.step.map.VertexStep;
import org.apache.tinkerpop.gremlin.process.traversal.strategy.AbstractTraversalStrategy;
import org.apache.tinkerpop.gremlin.process.traversal.util.EmptyTraversal;
import org.apache.tinkerpop.gremlin.process.traversal.util.TraversalHelper;

public final class HugeVertexStepStrategy
             extends AbstractTraversalStrategy<ProviderOptimizationStrategy>
             implements ProviderOptimizationStrategy {

    private static final long serialVersionUID = 491355700217483162L;

    private static final HugeVertexStepStrategy INSTANCE;

    static {
        INSTANCE = new HugeVertexStepStrategy();
    }

    private HugeVertexStepStrategy() {
        // pass
    }

    @Override
    @SuppressWarnings({ "rawtypes", "unchecked" })
    public void apply(final Traversal.Admin<?, ?> traversal) {
        TraversalUtil.convAllHasSteps(traversal);

        List<VertexStep> steps = TraversalHelper.getStepsOfClass(
                                 VertexStep.class, traversal);


        // 判断是否有limit类似的Step
        boolean flag = isContainRangeGlobalStep(traversal);

        // 判断是否包含EdgeOtherVertexStep
        boolean isContainEdgeOtherVertexStep = isContainEdgeOtherVertexStep(traversal);

        // 判断最后一步是否是VertexStep
        boolean isEndVertexStep = isEndVertexStep(traversal);

        boolean batchOptimize = false;
        if (!steps.isEmpty()) {
            boolean withPath = HugeVertexStepStrategy.containsPath(traversal);
            boolean withTree = HugeVertexStepStrategy.containsTree(traversal);
            /*
             * The graph of traversal may be null when `__` step is followed
             * by `count().is(0)` step, like the following gremlin:
             * `g.V(id).repeat(in()).until(or(inE().count().is(0), loops().is(2)))`
             * TODO: remove this `graph!=null` check after fixed the bug #1699
             */
            boolean supportIn = false;
            HugeGraph graph = TraversalUtil.tryGetGraph(steps.get(0));
            if (graph != null) {
                supportIn = graph.backendStoreFeatures()
                                 .supportsQueryWithInCondition();
            }
            batchOptimize = !withTree && !withPath && supportIn;
        }

        //TODO:此处具体调用逻辑待定  isEndVertexStep标签计算逻辑
        for (VertexStep originStep : steps) {
            HugeVertexStep<?> newStep = batchOptimize ?
                                        new HugeVertexStepByBatch<>(originStep, flag, isEndVertexStep, isContainEdgeOtherVertexStep) :
                                        new HugeVertexStep<>(originStep, flag, isEndVertexStep, isContainEdgeOtherVertexStep);
            TraversalHelper.replaceStep(originStep, newStep, traversal);

            TraversalUtil.extractHasContainer(newStep, traversal);

            // TODO: support order-by optimize
            // TraversalUtil.extractOrder(newStep, traversal);

            TraversalUtil.extractRange(newStep, traversal, true);

            TraversalUtil.extractCount(newStep, traversal);
        }
    }

    /**
     * Does a Traversal contain any Path step
     * @param traversal
     * @return the traversal or its parents contain at least one Path step
     */
    protected static boolean containsPath(Traversal.Admin<?, ?> traversal) {
        boolean hasPath = TraversalHelper.getStepsOfClass(
                          PathStep.class, traversal).size() > 0;
        if (hasPath) {
            return true;
        } else if (traversal instanceof EmptyTraversal) {
            return false;
        }

        TraversalParent parent = traversal.getParent();
        return containsPath(parent.asStep().getTraversal());
    }

    /**
     * Does a Traversal contain any Tree step
     * @param traversal
     * @return the traversal or its parents contain at least one Tree step
     */
    protected static boolean containsTree(Traversal.Admin<?, ?> traversal) {
        boolean hasTree = TraversalHelper.getStepsOfClass(
                          TreeStep.class, traversal).size() > 0;
        if (hasTree) {
            return true;
        } else if (traversal instanceof EmptyTraversal) {
            return false;
        }

        TraversalParent parent = traversal.getParent();
        return containsTree(parent.asStep().getTraversal());
    }

    private boolean isContainRangeGlobalStep(final Traversal.Admin<?, ?> traversal) {
        // 判断是否有limit类似的Step
        List<RangeGlobalStep> stepsOfClass = TraversalHelper.getStepsOfClass(
            RangeGlobalStep.class, traversal);
        return stepsOfClass.size() > 0;

    }

    private boolean isContainEdgeOtherVertexStep(final Traversal.Admin<?, ?> traversal) {
        // 判断是否有EdgeOtherVertexStep类似的Step
        List<EdgeOtherVertexStep> stepsOfClass = TraversalHelper.getStepsOfClass(
            EdgeOtherVertexStep.class, traversal);
        return stepsOfClass.size() > 0;

    }

    private boolean isEndVertexStep(final Traversal.Admin<?, ?> traversal) {
        boolean isEndVertexStep = false;
        Step endStep = traversal.getSteps().get(traversal.getSteps().size() - 1);
        if (Objects.nonNull(endStep) && endStep instanceof VertexStep) {
            isEndVertexStep = true;
        }
        return isEndVertexStep;
    }

    public static HugeVertexStepStrategy instance() {
        return INSTANCE;
    }
}
