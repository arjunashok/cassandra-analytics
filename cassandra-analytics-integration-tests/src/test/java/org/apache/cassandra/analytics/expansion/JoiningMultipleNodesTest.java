/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.cassandra.analytics.expansion;

import java.util.Collection;
import java.util.concurrent.Callable;
import java.util.concurrent.CountDownLatch;

import com.google.common.util.concurrent.Uninterruptibles;
import org.junit.jupiter.api.extension.ExtendWith;

import com.datastax.driver.core.ConsistencyLevel;
import io.vertx.junit5.VertxExtension;
import io.vertx.junit5.VertxTestContext;
import net.bytebuddy.ByteBuddy;
import net.bytebuddy.description.type.TypeDescription;
import net.bytebuddy.dynamic.ClassFileLocator;
import net.bytebuddy.dynamic.TypeResolutionStrategy;
import net.bytebuddy.dynamic.loading.ClassLoadingStrategy;
import net.bytebuddy.implementation.MethodDelegation;
import net.bytebuddy.implementation.bind.annotation.SuperCall;
import net.bytebuddy.pool.TypePool;
import org.apache.cassandra.testing.CassandraIntegrationTest;
import org.apache.cassandra.testing.ConfigurableCassandraTestContext;
import org.apache.cassandra.utils.Shared;

import static net.bytebuddy.matcher.ElementMatchers.named;
import static net.bytebuddy.matcher.ElementMatchers.takesArguments;

@ExtendWith(VertxExtension.class)
public class JoiningMultipleNodesTest extends JoiningBaseTest
{
    @CassandraIntegrationTest(nodesPerDc = 3, newNodesPerDc = 2, network = true, gossip = true, buildCluster = false)
    void oneReadAllWrite(ConfigurableCassandraTestContext cassandraTestContext, VertxTestContext context)
    throws Exception
    {
        BBHelperMultipleJoiningNodes.reset();
        runJoiningTestScenario(context,
                               cassandraTestContext,
                               BBHelperMultipleJoiningNodes::install,
                               BBHelperMultipleJoiningNodes.transientStateStart,
                               BBHelperMultipleJoiningNodes.transientStateEnd,
                               ConsistencyLevel.ONE,
                               ConsistencyLevel.ALL,
                               false);
    }

    @CassandraIntegrationTest(nodesPerDc = 3, newNodesPerDc = 2, network = true, gossip = true, buildCluster = false)
    void oneReadAllWriteFailure(ConfigurableCassandraTestContext cassandraTestContext, VertxTestContext context)
    throws Exception
    {
        BBHelperMultipleJoiningNodesFailure.reset();
        runJoiningTestScenario(context,
                               cassandraTestContext,
                               BBHelperMultipleJoiningNodesFailure::install,
                               BBHelperMultipleJoiningNodesFailure.transientStateStart,
                               BBHelperMultipleJoiningNodesFailure.transientStateEnd,
                               ConsistencyLevel.ONE,
                               ConsistencyLevel.ALL,
                               true);
    }

    @CassandraIntegrationTest(nodesPerDc = 3, newNodesPerDc = 2, network = true, gossip = true, buildCluster = false)
    void quorumReadQuorumWrite(ConfigurableCassandraTestContext cassandraTestContext, VertxTestContext context)
    throws Exception
    {
        BBHelperMultipleJoiningNodes.reset();
        runJoiningTestScenario(context,
                               cassandraTestContext,
                               BBHelperMultipleJoiningNodes::install,
                               BBHelperMultipleJoiningNodes.transientStateStart,
                               BBHelperMultipleJoiningNodes.transientStateEnd,
                               ConsistencyLevel.QUORUM,
                               ConsistencyLevel.QUORUM,
                               false);
    }

    @CassandraIntegrationTest(nodesPerDc = 3, newNodesPerDc = 2, network = true, gossip = true, buildCluster = false)
    void quorumReadQuorumWriteFailure(ConfigurableCassandraTestContext cassandraTestContext, VertxTestContext context)
    throws Exception
    {
        BBHelperMultipleJoiningNodesFailure.reset();
        runJoiningTestScenario(context,
                               cassandraTestContext,
                               BBHelperMultipleJoiningNodesFailure::install,
                               BBHelperMultipleJoiningNodesFailure.transientStateStart,
                               BBHelperMultipleJoiningNodesFailure.transientStateEnd,
                               ConsistencyLevel.QUORUM,
                               ConsistencyLevel.QUORUM,
                               true);
    }

    /**
     * ByteBuddy helper for multiple joining nodes
     */
    @Shared
    public static class BBHelperMultipleJoiningNodes
    {
        static CountDownLatch transientStateStart = new CountDownLatch(2);
        static CountDownLatch transientStateEnd = new CountDownLatch(2);

        public static void install(ClassLoader cl, Integer nodeNumber)
        {
            // Test case involves 3 node cluster with a 2 joining nodes
            // We intercept the joining of nodes (4, 5) to validate token ranges
            if (nodeNumber > 3)
            {
                TypePool typePool = TypePool.Default.of(cl);
                TypeDescription description = typePool.describe("org.apache.cassandra.service.StorageService")
                                                      .resolve();
                new ByteBuddy().rebase(description, ClassFileLocator.ForClassLoader.of(cl))
                               .method(named("bootstrap").and(takesArguments(2)))
                               .intercept(MethodDelegation.to(BBHelperMultipleJoiningNodes.class))
                               // Defer class loading until all dependencies are loaded
                               .make(TypeResolutionStrategy.Lazy.INSTANCE, typePool)
                               .load(cl, ClassLoadingStrategy.Default.INJECTION);
            }
        }

        public static boolean bootstrap(Collection<?> tokens,
                                        long bootstrapTimeoutMillis,
                                        @SuperCall Callable<Boolean> orig) throws Exception
        {
            boolean result = orig.call();
            // trigger bootstrap start and wait until bootstrap is ready from test
            transientStateStart.countDown();
            Uninterruptibles.awaitUninterruptibly(transientStateEnd);
            return result;
        }

        public static void reset()
        {
            transientStateStart = new CountDownLatch(2);
            transientStateEnd = new CountDownLatch(2);
        }
    }

    /**
     * ByteBuddy helper for multiple joining nodes failure scenario
     */
    @Shared
    public static class BBHelperMultipleJoiningNodesFailure
    {
        static CountDownLatch transientStateStart = new CountDownLatch(2);
        static CountDownLatch transientStateEnd = new CountDownLatch(2);

        public static void install(ClassLoader cl, Integer nodeNumber)
        {
            // Test case involves 3 node cluster with a 2 joining nodes
            // We intercept the joining of nodes (4, 5) to validate token ranges
            if (nodeNumber > 3)
            {
                TypePool typePool = TypePool.Default.of(cl);
                TypeDescription description = typePool.describe("org.apache.cassandra.service.StorageService")
                                                      .resolve();
                new ByteBuddy().rebase(description, ClassFileLocator.ForClassLoader.of(cl))
                               .method(named("bootstrap").and(takesArguments(2)))
                               .intercept(MethodDelegation.to(BBHelperMultipleJoiningNodesFailure.class))
                               // Defer class loading until all dependencies are loaded
                               .make(TypeResolutionStrategy.Lazy.INSTANCE, typePool)
                               .load(cl, ClassLoadingStrategy.Default.INJECTION);
            }
        }

        public static boolean bootstrap(Collection<?> tokens,
                                        long bootstrapTimeoutMillis,
                                        @SuperCall Callable<Boolean> orig) throws Exception
        {
            boolean result = orig.call();
            // trigger bootstrap start and wait until bootstrap is ready from test
            transientStateStart.countDown();
            Uninterruptibles.awaitUninterruptibly(transientStateEnd);
            throw new UnsupportedOperationException("Simulated failure");
        }

        public static void reset()
        {
            transientStateStart = new CountDownLatch(2);
            transientStateEnd = new CountDownLatch(2);
        }
    }
}
