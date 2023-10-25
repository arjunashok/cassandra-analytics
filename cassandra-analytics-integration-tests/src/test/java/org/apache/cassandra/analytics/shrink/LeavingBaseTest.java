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

import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.CountDownLatch;

import com.google.common.collect.ImmutableMap;
import com.google.common.util.concurrent.Uninterruptibles;

import com.datastax.driver.core.ConsistencyLevel;
import com.datastax.driver.core.Session;
import o.a.c.analytics.sidecar.shaded.testing.common.data.QualifiedTableName;
import org.apache.cassandra.analytics.ResiliencyTestBase;
import org.apache.cassandra.distributed.UpgradeableCluster;
import org.apache.cassandra.distributed.api.IUpgradeableInstance;
import org.apache.cassandra.distributed.shared.ClusterUtils;
import org.apache.cassandra.testing.CassandraIntegrationTest;
import org.apache.spark.SparkConf;
import org.apache.spark.sql.DataFrameWriter;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;

import static org.junit.Assert.assertNotNull;

class LeavingBaseTest extends ResiliencyTestBase
{
    void runLeavingTestScenario(int leavingNodesPerDC,
                                CountDownLatch transientStateStart,
                                CountDownLatch transientStateEnd,
                                UpgradeableCluster cluster,
                                ConsistencyLevel readCL,
                                ConsistencyLevel writeCL,
                                boolean isFailure)
    {
        CassandraIntegrationTest annotation = sidecarTestContext.cassandraTestContext().annotation;
        QualifiedTableName table = null;
        try
        {
            IUpgradeableInstance seed = cluster.get(1);

            List<IUpgradeableInstance> leavingNodes = new ArrayList<>();
            for (int i = 0; i < leavingNodesPerDC * annotation.numDcs(); i++)
            {
                IUpgradeableInstance node = cluster.get(cluster.size() - i);
                new Thread(() -> node.nodetoolResult("decommission").asserts().success()).start();
                leavingNodes.add(node);
            }

            // Wait until nodes have reached expected state
            Uninterruptibles.awaitUninterruptibly(transientStateStart);

            for (IUpgradeableInstance node : leavingNodes)
            {
                ClusterUtils.awaitRingState(seed, node, "Leaving");
            }

            if (!isFailure)
            {
                table = bulkWriteData(annotation.numDcs() > 1, writeCL);
                Session session = maybeGetSession();
                assertNotNull(table);
                validateData(session, table.tableName(), readCL);
            }
        }
        finally
        {
            if (!isFailure)
            {
                for (int i = 0; i < leavingNodesPerDC; i++)
                {
                    transientStateEnd.countDown();
                }
            }
        }
        if (isFailure)
        {
            table = bulkWriteData(annotation.numDcs() > 1, writeCL, leavingNodesPerDC, transientStateEnd);
            Session session = maybeGetSession();
            assertNotNull(table);
            validateData(session, table.tableName(), readCL);
            // TODO: node normal state
            // TODO: transient node got the data
        }
    }

    protected QualifiedTableName bulkWriteData(boolean isCrossDCKeyspace, ConsistencyLevel writeCL, int leavingNodesPerDC, CountDownLatch transientStateEnd)
    {
        CassandraIntegrationTest annotation = sidecarTestContext.cassandraTestContext().annotation;
        List<String> sidecarInstances = generateSidecarInstances((annotation.nodesPerDc() + annotation.newNodesPerDc()) * annotation.numDcs());

        ImmutableMap<String, Integer> rf;
        if (annotation.numDcs() > 1 && isCrossDCKeyspace)
        {
            rf = ImmutableMap.of("datacenter1", DEFAULT_RF, "datacenter2", DEFAULT_RF);
        }
        else
        {
            rf = ImmutableMap.of("datacenter1", DEFAULT_RF);
        }

        QualifiedTableName schema = initializeSchema(rf);

        SparkConf sparkConf = generateSparkConf();
        SparkSession spark = generateSparkSession(sparkConf);
        Dataset<Row> df = generateData(spark);

        DataFrameWriter<Row> dfWriter = df.write()
                                          .format("org.apache.cassandra.spark.sparksql.CassandraDataSink")
                                          .option("bulk_writer_cl", writeCL.name())
                                          .option("local_dc", "datacenter1")
                                          .option("sidecar_instances", String.join(",", sidecarInstances))
                                          .option("sidecar_port", String.valueOf(server.actualPort()))
                                          .option("keyspace", schema.keyspace())
                                          .option("table", schema.tableName())
                                          .option("number_splits", "-1")
                                          .mode("append");
        if (!isCrossDCKeyspace)
        {
            dfWriter.option("local_dc", "datacenter1");
        }

        for (int i = 0; i < leavingNodesPerDC; i++)
        {
            transientStateEnd.countDown();
        }
        dfWriter.save();
        return schema;
    }
}