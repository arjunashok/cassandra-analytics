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
import org.apache.cassandra.distributed.UpgradeableCluster;
import org.apache.cassandra.testing.CassandraIntegrationTest;
import org.apache.cassandra.testing.ConfigurableCassandraTestContext;
import org.apache.cassandra.utils.Shared;

import static net.bytebuddy.matcher.ElementMatchers.named;
import static net.bytebuddy.matcher.ElementMatchers.takesArguments;

@ExtendWith(VertxExtension.class)
public class JoiningMultiDCSingleReplicatedTest extends JoiningBaseTest
{
    @CassandraIntegrationTest(nodesPerDc = 5, newNodesPerDc = 1, numDcs = 2, network = true, gossip = true, buildCluster = false)
    void allReadOneWrite(ConfigurableCassandraTestContext cassandraTestContext, VertxTestContext context) throws Exception
    {
        BBHelperMultiDC.reset();
        UpgradeableCluster cluster = getMultiDCCluster(BBHelperMultiDC::install, cassandraTestContext);

        runJoiningTestScenario(context,
                               BBHelperMultiDC.transientStateStart,
                               BBHelperMultiDC.transientStateEnd,
                               cluster,
                               false,
                               ConsistencyLevel.ALL,
                               ConsistencyLevel.ONE,
                               false);
    }

    @CassandraIntegrationTest(nodesPerDc = 5, newNodesPerDc = 1, numDcs = 2, network = true, gossip = true, buildCluster = false)
    void allReadOneWriteFailure(ConfigurableCassandraTestContext cassandraTestContext, VertxTestContext context) throws Exception
    {
        BBHelperMultiDCFailure.reset();
        UpgradeableCluster cluster = getMultiDCCluster(BBHelperMultiDCFailure::install, cassandraTestContext);

        runJoiningTestScenario(context,
                               BBHelperMultiDCFailure.transientStateStart,
                               BBHelperMultiDCFailure.transientStateEnd,
                               cluster,
                               false,
                               ConsistencyLevel.ALL,
                               ConsistencyLevel.ONE,
                               true);
    }

    @CassandraIntegrationTest(nodesPerDc = 5, newNodesPerDc = 1, numDcs = 2, network = true, gossip = true, buildCluster = false)
    void localQuorumReadLocalQuorumWrite(ConfigurableCassandraTestContext cassandraTestContext, VertxTestContext context) throws Exception
    {
        BBHelperMultiDC.reset();
        UpgradeableCluster cluster = getMultiDCCluster(BBHelperMultiDC::install, cassandraTestContext);

        runJoiningTestScenario(context,
                               BBHelperMultiDC.transientStateStart,
                               BBHelperMultiDC.transientStateEnd,
                               cluster,
                               false,
                               ConsistencyLevel.LOCAL_QUORUM,
                               ConsistencyLevel.LOCAL_QUORUM,
                               false);
    }

    @CassandraIntegrationTest(nodesPerDc = 5, newNodesPerDc = 1, numDcs = 2, network = true, gossip = true, buildCluster = false)
    void localQuorumReadLocalQuorumWriteFailure(ConfigurableCassandraTestContext cassandraTestContext, VertxTestContext context) throws Exception
    {
        BBHelperMultiDCFailure.reset();
        UpgradeableCluster cluster = getMultiDCCluster(BBHelperMultiDCFailure::install, cassandraTestContext);

        runJoiningTestScenario(context,
                               BBHelperMultiDCFailure.transientStateStart,
                               BBHelperMultiDCFailure.transientStateEnd,
                               cluster,
                               false,
                               ConsistencyLevel.LOCAL_QUORUM,
                               ConsistencyLevel.LOCAL_QUORUM,
                               true);
    }

    @CassandraIntegrationTest(nodesPerDc = 5, newNodesPerDc = 1, numDcs = 2, network = true, gossip = true, buildCluster = false)
    void eachQuorumReadLocalQuorumWrite(ConfigurableCassandraTestContext cassandraTestContext, VertxTestContext context) throws Exception
    {
        BBHelperMultiDC.reset();
        UpgradeableCluster cluster = getMultiDCCluster(BBHelperMultiDC::install, cassandraTestContext);

        runJoiningTestScenario(context,
                               BBHelperMultiDC.transientStateStart,
                               BBHelperMultiDC.transientStateEnd,
                               cluster,
                               false,
                               ConsistencyLevel.EACH_QUORUM,
                               ConsistencyLevel.LOCAL_QUORUM,
                               false);
    }

    @CassandraIntegrationTest(nodesPerDc = 5, newNodesPerDc = 1, numDcs = 2, network = true, gossip = true, buildCluster = false)
    void eachQuorumReadLocalQuorumWriteFailure(ConfigurableCassandraTestContext cassandraTestContext, VertxTestContext context) throws Exception
    {
        BBHelperMultiDCFailure.reset();
        UpgradeableCluster cluster = getMultiDCCluster(BBHelperMultiDCFailure::install, cassandraTestContext);

        runJoiningTestScenario(context,
                               BBHelperMultiDCFailure.transientStateStart,
                               BBHelperMultiDCFailure.transientStateEnd,
                               cluster,
                               false,
                               ConsistencyLevel.EACH_QUORUM,
                               ConsistencyLevel.LOCAL_QUORUM,
                               true);
    }

    @CassandraIntegrationTest(nodesPerDc = 5, newNodesPerDc = 1, numDcs = 2, network = true, gossip = true, buildCluster = false)
    void oneReadAllWrite(ConfigurableCassandraTestContext cassandraTestContext, VertxTestContext context) throws Exception
    {
        BBHelperMultiDC.reset();
        UpgradeableCluster cluster = getMultiDCCluster(BBHelperMultiDC::install, cassandraTestContext);

        runJoiningTestScenario(context,
                               BBHelperMultiDC.transientStateStart,
                               BBHelperMultiDC.transientStateEnd,
                               cluster,
                               false,
                               ConsistencyLevel.ONE,
                               ConsistencyLevel.ALL,
                               false);
    }

    @CassandraIntegrationTest(nodesPerDc = 5, newNodesPerDc = 1, numDcs = 2, network = true, gossip = true, buildCluster = false)
    void oneReadAllWriteFailure(ConfigurableCassandraTestContext cassandraTestContext, VertxTestContext context) throws Exception
    {
        BBHelperMultiDCFailure.reset();
        UpgradeableCluster cluster = getMultiDCCluster(BBHelperMultiDCFailure::install, cassandraTestContext);

        runJoiningTestScenario(context,
                               BBHelperMultiDCFailure.transientStateStart,
                               BBHelperMultiDCFailure.transientStateEnd,
                               cluster,
                               false,
                               ConsistencyLevel.ONE,
                               ConsistencyLevel.ALL,
                               true);
    }

    /**
     * ByteBuddy helper for multiple joining nodes
     */
    @Shared
    public static class BBHelperMultiDC
    {
        static CountDownLatch transientStateStart = new CountDownLatch(2);
        static CountDownLatch transientStateEnd = new CountDownLatch(2);

        public static void install(ClassLoader cl, Integer nodeNumber)
        {
            // Test case involves adding 2 nodes to a 10 node cluster (5 per DC)
            // We intercept the bootstrap of nodes (11,12) to validate token ranges
            if (nodeNumber > 10)
            {
                TypePool typePool = TypePool.Default.of(cl);
                TypeDescription description = typePool.describe("org.apache.cassandra.service.StorageService")
                                                      .resolve();
                new ByteBuddy().rebase(description, ClassFileLocator.ForClassLoader.of(cl))
                               .method(named("bootstrap").and(takesArguments(2)))
                               .intercept(MethodDelegation.to(BBHelperMultiDC.class))
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
     * ByteBuddy helper for multiple joining nodes failure
     */
    @Shared
    public static class BBHelperMultiDCFailure
    {
        static CountDownLatch transientStateStart = new CountDownLatch(2);
        static CountDownLatch transientStateEnd = new CountDownLatch(2);

        public static void install(ClassLoader cl, Integer nodeNumber)
        {
            // Test case involves adding 2 nodes to a 10 node cluster (5 per DC)
            // We intercept the bootstrap of nodes (11,12) to validate token ranges
            if (nodeNumber > 10)
            {
                TypePool typePool = TypePool.Default.of(cl);
                TypeDescription description = typePool.describe("org.apache.cassandra.service.StorageService")
                                                      .resolve();
                new ByteBuddy().rebase(description, ClassFileLocator.ForClassLoader.of(cl))
                               .method(named("bootstrap").and(takesArguments(2)))
                               .intercept(MethodDelegation.to(BBHelperMultiDCFailure.class))
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
