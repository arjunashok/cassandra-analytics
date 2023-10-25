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
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import java.util.function.BiConsumer;

import com.google.common.util.concurrent.Uninterruptibles;

import com.datastax.driver.core.ConsistencyLevel;
import com.datastax.driver.core.Session;
import o.a.c.analytics.sidecar.shaded.testing.common.data.QualifiedTableName;
import org.apache.cassandra.analytics.ResiliencyTestBase;
import org.apache.cassandra.analytics.TestTokenSupplier;
import org.apache.cassandra.distributed.UpgradeableCluster;
import org.apache.cassandra.distributed.api.IUpgradeableInstance;
import org.apache.cassandra.distributed.api.TokenSupplier;
import org.apache.cassandra.distributed.shared.ClusterUtils;
import org.apache.cassandra.testing.CassandraIntegrationTest;
import org.apache.cassandra.testing.ConfigurableCassandraTestContext;

public class NodeMovementBaseTest extends ResiliencyTestBase
{
    public static final int MOVING_NODE_IDX = 5;
    void runMovingNodeTest(ConfigurableCassandraTestContext cassandraTestContext,
                           BiConsumer<ClassLoader, Integer> instanceInitializer,
                           CountDownLatch transientStateStart,
                           CountDownLatch transientStateEnd,
                           boolean isFailure) throws IOException
    {
        CassandraIntegrationTest annotation = sidecarTestContext.cassandraTestContext().annotation;
        TokenSupplier tokenSupplier = TestTokenSupplier.evenlyDistributedTokens(annotation.nodesPerDc(),
                                                                                annotation.newNodesPerDc(),
                                                                                annotation.numDcs(),
                                                                                1);

        UpgradeableCluster cluster = cassandraTestContext.configureAndStartCluster(builder -> {
            builder.withInstanceInitializer(instanceInitializer);
            builder.withTokenSupplier(tokenSupplier);
        });

        QualifiedTableName schema;
        long moveTarget = getMoveTargetToken(cluster);
        int movingNodeIndex = MOVING_NODE_IDX;
        IUpgradeableInstance movingNode = cluster.get(movingNodeIndex);

        try
        {
            IUpgradeableInstance seed = cluster.get(1);
            new Thread(() -> movingNode.nodetoolResult("move", "--", Long.toString(moveTarget))
                                       .asserts()
                                       .success()).start();

            // Wait until nodes have reached expected state
            Uninterruptibles.awaitUninterruptibly(transientStateStart, 2, TimeUnit.MINUTES);
            ClusterUtils.awaitRingState(seed, movingNode, "Moving");

            schema = bulkWriteData();
        }
        finally
        {
            transientStateEnd.countDown();
        }

        if (!isFailure)
        {
            ClusterUtils.awaitRingState(cluster.get(1), movingNode, "Normal");
        }
        Session session = maybeGetSession();
        validateData(session, schema.tableName(), ConsistencyLevel.QUORUM);

        if (isFailure)
        {
            ClusterUtils.awaitRingState(cluster.get(1), movingNode, "Moving");

        }
    }

    protected long getMoveTargetToken(UpgradeableCluster cluster)
    {
        CassandraIntegrationTest annotation = sidecarTestContext.cassandraTestContext().annotation;
        IUpgradeableInstance seed = cluster.get(1);
        // The target token to move the node to is calculated by adding an offset to the seed node token which
        // is half of the range between 2 tokens.
        // For multi-DC case (specifically 2 DCs), since neighbouring tokens can be consecutive, we use tokens 1
        // and 3 to calculate the offset
        int nextIndex = (annotation.numDcs() > 1) ? 3 : 2;
        long t2 = Long.parseLong(seed.config().getString("initial_token"));
        long t3 = Long.parseLong(cluster.get(nextIndex).config().getString("initial_token"));
        return (t2 + ((t3 - t2) / 2));
    }

}
