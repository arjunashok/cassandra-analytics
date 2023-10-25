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

package org.apache.cassandra.analytics.shrink;

import java.util.concurrent.Callable;
import java.util.concurrent.CountDownLatch;

import com.google.common.util.concurrent.Uninterruptibles;
import org.junit.jupiter.api.extension.ExtendWith;

import com.datastax.driver.core.ConsistencyLevel;
import io.vertx.junit5.VertxExtension;
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

@ExtendWith(VertxExtension.class)
class LeavingTestMultiDC extends LeavingBaseTest
{
    @CassandraIntegrationTest(nodesPerDc = 5, numDcs = 2, network = true, gossip = true, buildCluster = false)
    void allReadOneWrite(ConfigurableCassandraTestContext cassandraTestContext)
    throws Exception
    {
        BBHelperLeavingNodesMultiDC.reset();
        int leavingNodesPerDC = 1;
        UpgradeableCluster cluster = getMultiDCCluster(BBHelperLeavingNodesMultiDC::install, cassandraTestContext);

        runLeavingTestScenario(leavingNodesPerDC,
                               BBHelperLeavingNodesMultiDC.transientStateStart,
                               BBHelperLeavingNodesMultiDC.transientStateEnd,
                               cluster,
                               ConsistencyLevel.ALL,
                               ConsistencyLevel.ONE,
                               false);
    }

    @CassandraIntegrationTest(nodesPerDc = 5, numDcs = 2, network = true, gossip = true, buildCluster = false)
    void allReadOneWriteFailure(ConfigurableCassandraTestContext cassandraTestContext)
    throws Exception
    {
        BBHelperLeavingNodesMultiDCFailure.reset();
        int leavingNodesPerDC = 1;
        UpgradeableCluster cluster = getMultiDCCluster(BBHelperLeavingNodesMultiDCFailure::install, cassandraTestContext);

        runLeavingTestScenario(leavingNodesPerDC,
                               BBHelperLeavingNodesMultiDC.transientStateStart,
                               BBHelperLeavingNodesMultiDC.transientStateEnd,
                               cluster,
                               ConsistencyLevel.ALL,
                               ConsistencyLevel.ONE,
                               true);
    }

    @CassandraIntegrationTest(nodesPerDc = 5, numDcs = 2, network = true, gossip = true, buildCluster = false)
    void localQuorumReadLocalQuorumWrite(ConfigurableCassandraTestContext cassandraTestContext)
    throws Exception
    {
        BBHelperLeavingNodesMultiDC.reset();
        int leavingNodesPerDC = 1;
        UpgradeableCluster cluster = getMultiDCCluster(BBHelperLeavingNodesMultiDC::install, cassandraTestContext);

        runLeavingTestScenario(leavingNodesPerDC,
                               BBHelperLeavingNodesMultiDC.transientStateStart,
                               BBHelperLeavingNodesMultiDC.transientStateEnd,
                               cluster,
                               ConsistencyLevel.LOCAL_QUORUM,
                               ConsistencyLevel.LOCAL_QUORUM,
                               false);
    }

    @CassandraIntegrationTest(nodesPerDc = 5, numDcs = 2, network = true, gossip = true, buildCluster = false)
    void localQuorumReadLocalQuorumWriteFailure(ConfigurableCassandraTestContext cassandraTestContext)
    throws Exception
    {
        BBHelperLeavingNodesMultiDCFailure.reset();
        int leavingNodesPerDC = 1;
        UpgradeableCluster cluster = getMultiDCCluster(BBHelperLeavingNodesMultiDCFailure::install, cassandraTestContext);

        runLeavingTestScenario(leavingNodesPerDC,
                               BBHelperLeavingNodesMultiDC.transientStateStart,
                               BBHelperLeavingNodesMultiDC.transientStateEnd,
                               cluster,
                               ConsistencyLevel.LOCAL_QUORUM,
                               ConsistencyLevel.LOCAL_QUORUM,
                               true);
    }

    @CassandraIntegrationTest(nodesPerDc = 5, numDcs = 2, network = true, gossip = true, buildCluster = false)
    void localQuorumReadEachQuorumWrite(ConfigurableCassandraTestContext cassandraTestContext)
    throws Exception
    {
        BBHelperLeavingNodesMultiDC.reset();
        int leavingNodesPerDC = 1;
        UpgradeableCluster cluster = getMultiDCCluster(BBHelperLeavingNodesMultiDC::install, cassandraTestContext);

        runLeavingTestScenario(leavingNodesPerDC,
                               BBHelperLeavingNodesMultiDC.transientStateStart,
                               BBHelperLeavingNodesMultiDC.transientStateEnd,
                               cluster,
                               ConsistencyLevel.LOCAL_QUORUM,
                               ConsistencyLevel.EACH_QUORUM,
                               false);
    }

    @CassandraIntegrationTest(nodesPerDc = 5, numDcs = 2, network = true, gossip = true, buildCluster = false)
    void localQuorumReadEachQuorumWriteFailure(ConfigurableCassandraTestContext cassandraTestContext)
    throws Exception
    {
        BBHelperLeavingNodesMultiDCFailure.reset();
        int leavingNodesPerDC = 1;
        UpgradeableCluster cluster = getMultiDCCluster(BBHelperLeavingNodesMultiDCFailure::install, cassandraTestContext);

        runLeavingTestScenario(leavingNodesPerDC,
                               BBHelperLeavingNodesMultiDC.transientStateStart,
                               BBHelperLeavingNodesMultiDC.transientStateEnd,
                               cluster,
                               ConsistencyLevel.LOCAL_QUORUM,
                               ConsistencyLevel.EACH_QUORUM,
                               true);
    }

    @CassandraIntegrationTest(nodesPerDc = 5, numDcs = 2, network = true, gossip = true, buildCluster = false)
    void quorumReadQuorumWrite(ConfigurableCassandraTestContext cassandraTestContext)
    throws Exception
    {
        BBHelperLeavingNodesMultiDC.reset();
        int leavingNodesPerDC = 1;
        UpgradeableCluster cluster = getMultiDCCluster(BBHelperLeavingNodesMultiDC::install, cassandraTestContext);

        runLeavingTestScenario(leavingNodesPerDC,
                               BBHelperLeavingNodesMultiDC.transientStateStart,
                               BBHelperLeavingNodesMultiDC.transientStateEnd,
                               cluster,
                               ConsistencyLevel.QUORUM,
                               ConsistencyLevel.QUORUM,
                               false);
    }

    @CassandraIntegrationTest(nodesPerDc = 5, numDcs = 2, network = true, gossip = true, buildCluster = false)
    void quorumReadQuorumWriteFailure(ConfigurableCassandraTestContext cassandraTestContext)
    throws Exception
    {
        BBHelperLeavingNodesMultiDCFailure.reset();
        int leavingNodesPerDC = 1;
        UpgradeableCluster cluster = getMultiDCCluster(BBHelperLeavingNodesMultiDCFailure::install, cassandraTestContext);

        runLeavingTestScenario(leavingNodesPerDC,
                               BBHelperLeavingNodesMultiDC.transientStateStart,
                               BBHelperLeavingNodesMultiDC.transientStateEnd,
                               cluster,
                               ConsistencyLevel.QUORUM,
                               ConsistencyLevel.QUORUM,
                               true);
    }

    @CassandraIntegrationTest(nodesPerDc = 5, numDcs = 2, network = true, gossip = true, buildCluster = false)
    void oneReadAllWrite(ConfigurableCassandraTestContext cassandraTestContext)
    throws Exception
    {
        BBHelperLeavingNodesMultiDC.reset();
        int leavingNodesPerDC = 1;
        UpgradeableCluster cluster = getMultiDCCluster(BBHelperLeavingNodesMultiDC::install, cassandraTestContext);

        runLeavingTestScenario(leavingNodesPerDC,
                               BBHelperLeavingNodesMultiDC.transientStateStart,
                               BBHelperLeavingNodesMultiDC.transientStateEnd,
                               cluster,
                               ConsistencyLevel.ONE,
                               ConsistencyLevel.ALL,
                               false);
    }

    @CassandraIntegrationTest(nodesPerDc = 5, numDcs = 2, network = true, gossip = true, buildCluster = false)
    void oneReadAllWriteFailure(ConfigurableCassandraTestContext cassandraTestContext)
    throws Exception
    {
        BBHelperLeavingNodesMultiDCFailure.reset();
        int leavingNodesPerDC = 1;
        UpgradeableCluster cluster = getMultiDCCluster(BBHelperLeavingNodesMultiDCFailure::install, cassandraTestContext);

        runLeavingTestScenario(leavingNodesPerDC,
                               BBHelperLeavingNodesMultiDC.transientStateStart,
                               BBHelperLeavingNodesMultiDC.transientStateEnd,
                               cluster,
                               ConsistencyLevel.ONE,
                               ConsistencyLevel.ALL,
                               true);
    }

    /**
     * ByteBuddy helper for multiple leaving nodes multi-DC
     */
    @Shared
    public static class BBHelperLeavingNodesMultiDC
    {
        static CountDownLatch transientStateStart = new CountDownLatch(2);
        static CountDownLatch transientStateEnd = new CountDownLatch(2);

        public static void install(ClassLoader cl, Integer nodeNumber)
        {
            // Test case involves 10 node cluster (5 nodes per DC) with a 2 leaving nodes (1 per DC)
            // We intercept the shutdown of the leaving nodes (9, 10) to validate token ranges
            if (nodeNumber > 8)
            {
                TypePool typePool = TypePool.Default.of(cl);
                TypeDescription description = typePool.describe("org.apache.cassandra.service.StorageService")
                                                      .resolve();
                new ByteBuddy().rebase(description, ClassFileLocator.ForClassLoader.of(cl))
                               .method(named("unbootstrap"))
                               .intercept(MethodDelegation.to(BBHelperLeavingNodesMultiDC.class))
                               // Defer class loading until all dependencies are loaded
                               .make(TypeResolutionStrategy.Lazy.INSTANCE, typePool)
                               .load(cl, ClassLoadingStrategy.Default.INJECTION);
            }
        }

        @SuppressWarnings("unused")
        public static void unbootstrap(@SuperCall Callable<?> orig) throws Exception
        {
            transientStateStart.countDown();
            Uninterruptibles.awaitUninterruptibly(transientStateEnd);
            orig.call();
        }

        public static void reset()
        {
            transientStateStart = new CountDownLatch(2);
            transientStateEnd = new CountDownLatch(2);
        }
    }

    /**
     * ByteBuddy helper for multiple leaving nodes multi-DC failure scenario
     */
    @Shared
    public static class BBHelperLeavingNodesMultiDCFailure
    {
        static CountDownLatch transientStateStart = new CountDownLatch(2);
        static CountDownLatch transientStateEnd = new CountDownLatch(2);

        public static void install(ClassLoader cl, Integer nodeNumber)
        {
            // Test case involves 10 node cluster (5 nodes per DC) with a 2 leaving nodes (1 per DC)
            // We intercept the shutdown of the leaving nodes (9, 10) to validate token ranges
            if (nodeNumber > 8)
            {
                TypePool typePool = TypePool.Default.of(cl);
                TypeDescription description = typePool.describe("org.apache.cassandra.service.StorageService")
                                                      .resolve();
                new ByteBuddy().rebase(description, ClassFileLocator.ForClassLoader.of(cl))
                               .method(named("unbootstrap"))
                               .intercept(MethodDelegation.to(BBHelperLeavingNodesMultiDC.class))
                               // Defer class loading until all dependencies are loaded
                               .make(TypeResolutionStrategy.Lazy.INSTANCE, typePool)
                               .load(cl, ClassLoadingStrategy.Default.INJECTION);
            }
        }

        @SuppressWarnings("unused")
        public static void unbootstrap(@SuperCall Callable<?> orig) throws Exception
        {
            transientStateStart.countDown();
            Uninterruptibles.awaitUninterruptibly(transientStateEnd);
            orig.call();
            throw new UnsupportedOperationException("Simulate leave failure");
        }

        public static void reset()
        {
            transientStateStart = new CountDownLatch(2);
            transientStateEnd = new CountDownLatch(2);
        }
    }
}