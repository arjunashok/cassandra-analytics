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

package org.apache.cassandra.analytics.movement;

import java.io.IOException;
import java.util.concurrent.Callable;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.Future;

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
import org.apache.cassandra.utils.Shared;
import org.apache.cassandra.testing.CassandraIntegrationTest;
import org.apache.cassandra.testing.ConfigurableCassandraTestContext;

import static net.bytebuddy.matcher.ElementMatchers.named;

@ExtendWith(VertxExtension.class)
public class NodeMovementTestMultiDC extends NodeMovementBaseTest
{

    @CassandraIntegrationTest(nodesPerDc = 3, numDcs = 2, network = true, gossip = true, buildCluster = false)
    void moveNodeMultiDCTest(ConfigurableCassandraTestContext cassandraTestContext,
                             VertxTestContext context) throws Exception
    {
        BBHelperMovingNodeMultiDC.reset();
        runMovingNodeTest(context,
                          cassandraTestContext,
                          BBHelperMovingNodeMultiDC::install,
                          BBHelperMovingNodeMultiDC.transientStateStart,
                          BBHelperMovingNodeMultiDC.transientStateEnd,
                          true,
                          false,
                          ConsistencyLevel.LOCAL_QUORUM,
                          ConsistencyLevel.LOCAL_QUORUM);

    }

    @CassandraIntegrationTest(nodesPerDc = 3, numDcs = 2, network = true, gossip = true, buildCluster = false)
    void moveNodeMultiDCQuorumReadWrite(ConfigurableCassandraTestContext cassandraTestContext,
                                        VertxTestContext context) throws Exception
    {
        BBHelperMovingNodeMultiDC.reset();
        runMovingNodeTest(context,
                          cassandraTestContext,
                          BBHelperMovingNodeMultiDC::install,
                          BBHelperMovingNodeMultiDC.transientStateStart,
                          BBHelperMovingNodeMultiDC.transientStateEnd,
                          true,
                          false,
                          ConsistencyLevel.QUORUM,
                          ConsistencyLevel.QUORUM);

    }

    @CassandraIntegrationTest(nodesPerDc = 3, numDcs = 2, network = true, gossip = true, buildCluster = false)
    void moveNodeMultiDCEachQuorumWrite(ConfigurableCassandraTestContext cassandraTestContext,
                                        VertxTestContext context) throws Exception
    {
        BBHelperMovingNodeMultiDC.reset();
        runMovingNodeTest(context,
                          cassandraTestContext,
                          BBHelperMovingNodeMultiDC::install,
                          BBHelperMovingNodeMultiDC.transientStateStart,
                          BBHelperMovingNodeMultiDC.transientStateEnd,
                          true,
                          false,
                          ConsistencyLevel.EACH_QUORUM,
                          ConsistencyLevel.LOCAL_QUORUM);

    }

    @CassandraIntegrationTest(nodesPerDc = 3, numDcs = 2, network = true, gossip = true, buildCluster = false)
    void moveNodeMultiDCAllWriteOneRead(ConfigurableCassandraTestContext cassandraTestContext,
                                        VertxTestContext context) throws Exception
    {
        BBHelperMovingNodeMultiDC.reset();
        runMovingNodeTest(context,
                          cassandraTestContext,
                          BBHelperMovingNodeMultiDC::install,
                          BBHelperMovingNodeMultiDC.transientStateStart,
                          BBHelperMovingNodeMultiDC.transientStateEnd,
                          true,
                          false,
                          ConsistencyLevel.ALL,
                          ConsistencyLevel.ONE);

    }

    @CassandraIntegrationTest(nodesPerDc = 3, numDcs = 2, network = true, gossip = true, buildCluster = false)
    void moveNodeMultiDCOneWriteAllRead(ConfigurableCassandraTestContext cassandraTestContext,
                                        VertxTestContext context) throws Exception
    {
        BBHelperMovingNodeMultiDC.reset();
        runMovingNodeTest(context,
                          cassandraTestContext,
                          BBHelperMovingNodeMultiDC::install,
                          BBHelperMovingNodeMultiDC.transientStateStart,
                          BBHelperMovingNodeMultiDC.transientStateEnd,
                          true,
                          false,
                          ConsistencyLevel.ONE,
                          ConsistencyLevel.ALL);

    }

    @CassandraIntegrationTest(nodesPerDc = 3, numDcs = 2, network = true, gossip = true, buildCluster = false)
    void moveNodeFailureMultiDCTest(ConfigurableCassandraTestContext cassandraTestContext,
                                    VertxTestContext context) throws Exception
    {
        BBHelperMultiDCMovingNodeFailure.reset();
        runMovingNodeTest(context,
                          cassandraTestContext,
                          BBHelperMultiDCMovingNodeFailure::install,
                          BBHelperMultiDCMovingNodeFailure.transientStateStart,
                          BBHelperMultiDCMovingNodeFailure.transientStateEnd,
                          true,
                          true,
                          ConsistencyLevel.QUORUM,
                          ConsistencyLevel.QUORUM);

    }

    @CassandraIntegrationTest(nodesPerDc = 3, numDcs = 2, network = true, gossip = true, buildCluster = false)
    void moveNodeFailureMultiDCEachQuorumWrite(ConfigurableCassandraTestContext cassandraTestContext,
                                               VertxTestContext context) throws Exception
    {
        BBHelperMultiDCMovingNodeFailure.reset();
        runMovingNodeTest(context,
                          cassandraTestContext,
                          BBHelperMultiDCMovingNodeFailure::install,
                          BBHelperMultiDCMovingNodeFailure.transientStateStart,
                          BBHelperMultiDCMovingNodeFailure.transientStateEnd,
                          true,
                          true,
                          ConsistencyLevel.EACH_QUORUM,
                          ConsistencyLevel.LOCAL_QUORUM);

    }

    @CassandraIntegrationTest(nodesPerDc = 3, numDcs = 2, network = true, gossip = true, buildCluster = false)
    void moveNodeFailureMultiDCAllWriteOneRead(ConfigurableCassandraTestContext cassandraTestContext,
                                               VertxTestContext context) throws Exception
    {
        BBHelperMultiDCMovingNodeFailure.reset();
        runMovingNodeTest(context,
                          cassandraTestContext,
                          BBHelperMultiDCMovingNodeFailure::install,
                          BBHelperMultiDCMovingNodeFailure.transientStateStart,
                          BBHelperMultiDCMovingNodeFailure.transientStateEnd,
                          true,
                          true,
                          ConsistencyLevel.ALL,
                          ConsistencyLevel.ONE);

    }

    /**
     * ByteBuddy Helper for a single moving node
     */
    @Shared
    public static class BBHelperMovingNodeMultiDC
    {
        static CountDownLatch transientStateStart = new CountDownLatch(1);
        static CountDownLatch transientStateEnd = new CountDownLatch(1);

        public static void install(ClassLoader cl, Integer nodeNumber)
        {
            // Moving the 5th node in the test case
            if (nodeNumber == MULTI_DC_MOVING_NODE_IDX)
            {
                TypePool typePool = TypePool.Default.of(cl);
                TypeDescription description = typePool.describe("org.apache.cassandra.service.RangeRelocator")
                                                      .resolve();
                new ByteBuddy().rebase(description, ClassFileLocator.ForClassLoader.of(cl))
                               .method(named("stream"))
                               .intercept(MethodDelegation.to(BBHelperMovingNodeMultiDC.class))
                               // Defer class loading until all dependencies are loaded
                               .make(TypeResolutionStrategy.Lazy.INSTANCE, typePool)
                               .load(cl, ClassLoadingStrategy.Default.INJECTION);
            }
        }

        @SuppressWarnings("unused")
        public static Future<?> stream(@SuperCall Callable<Future<?>> orig) throws Exception
        {
            Future<?> res = orig.call();
            transientStateStart.countDown();
            Uninterruptibles.awaitUninterruptibly(transientStateEnd);
            return res;
        }

        public static void reset()
        {
            transientStateStart = new CountDownLatch(1);
            transientStateEnd = new CountDownLatch(1);
        }
    }

    /**
     * ByteBuddy Helper for a single moving node
     */
    @Shared
    public static class BBHelperMultiDCMovingNodeFailure
    {
        static CountDownLatch transientStateStart = new CountDownLatch(1);
        static CountDownLatch transientStateEnd = new CountDownLatch(1);

        public static void install(ClassLoader cl, Integer nodeNumber)
        {
            // Moving the 5th node in the test case
            if (nodeNumber == MULTI_DC_MOVING_NODE_IDX)
            {
                TypePool typePool = TypePool.Default.of(cl);
                TypeDescription description = typePool.describe("org.apache.cassandra.service.RangeRelocator")
                                                      .resolve();
                new ByteBuddy().rebase(description, ClassFileLocator.ForClassLoader.of(cl))
                               .method(named("stream"))
                               .intercept(MethodDelegation.to(BBHelperMultiDCMovingNodeFailure.class))
                               // Defer class loading until all dependencies are loaded
                               .make(TypeResolutionStrategy.Lazy.INSTANCE, typePool)
                               .load(cl, ClassLoadingStrategy.Default.INJECTION);
            }
        }

        @SuppressWarnings("unused")
        public static Future<?> stream(@SuperCall Callable<Future<?>> orig) throws Exception
        {
            Future<?> res = orig.call();
            transientStateStart.countDown();
            Uninterruptibles.awaitUninterruptibly(transientStateEnd);

            throw new IOException("Simulated node movement failure"); // Throws exception to nodetool
        }

        public static void reset()
        {
            transientStateStart = new CountDownLatch(1);
            transientStateEnd = new CountDownLatch(1);
        }
    }
}
