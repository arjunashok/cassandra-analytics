/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *   http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */

package org.apache.cassandra.spark.bulkwriter;

import java.math.BigInteger;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.stream.Collectors;
import javax.validation.constraints.NotNull;

import com.google.common.collect.ArrayListMultimap;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.Multimap;
import com.google.common.collect.Range;

import org.apache.cassandra.sidecar.common.data.RingEntry;
import org.apache.cassandra.spark.bulkwriter.token.CassandraRing;
import org.apache.cassandra.spark.bulkwriter.token.RangeUtils;
import org.apache.cassandra.spark.bulkwriter.token.TokenRangeMapping;
import org.apache.cassandra.spark.data.ReplicationFactor;
import org.apache.cassandra.spark.data.partitioner.Partitioner;


public final class RingUtils
{
    private RingUtils()
    {
        throw new IllegalStateException(getClass() + " is static utility class and shall not be instantiated");
    }

    public static CassandraRing buildRing(final String dataCenter, final String keyspace)
    {
        ImmutableMap<String, Integer> rfByDC = ImmutableMap.of(dataCenter, 3);
        return buildRing(rfByDC, keyspace);
    }

    @NotNull
    static CassandraRing buildRing(final ImmutableMap<String, Integer> rfByDC, final String keyspace)
    {

        ReplicationFactor replicationFactor = getReplicationFactor(rfByDC);
        return new CassandraRing(Partitioner.Murmur3Partitioner, keyspace, replicationFactor);
    }

    public static TokenRangeMapping<RingInstance> buildTokenRangeMapping(final int initialToken, final ImmutableMap<String, Integer> rfByDC, int instancesPerDC)
    {

        final List<RingInstance> instances = getInstances(initialToken, rfByDC, instancesPerDC);

        ReplicationFactor replicationFactor = getReplicationFactor(rfByDC);
        Map<String, Set<String>> writeReplicas = instances.stream()
                                                          .collect(Collectors.groupingBy(RingInstance::getDataCenter,
                                                                                         Collectors.mapping(RingInstance::getNodeName,
                                                                                                            Collectors.toSet())));
        writeReplicas.replaceAll((key, value) -> {
            value.removeIf(e -> value.size() > 3);
            return value;
        });

        Multimap<RingInstance, Range<BigInteger>> tokenRanges = setupTokenRangeMap(Partitioner.Murmur3Partitioner, replicationFactor, instances);
        return new TokenRangeMapping<>(Partitioner.Murmur3Partitioner,
                                       writeReplicas,
                                       Collections.emptyMap(),
                                       tokenRanges,
                                       Collections.emptySet(),
                                       Collections.emptySet());
    }

    public static Multimap<RingInstance, Range<BigInteger>> setupTokenRangeMap(Partitioner partitioner,
                                                                               ReplicationFactor replicationFactor,
                                                                               List<RingInstance> instances)
    {
        ArrayListMultimap<RingInstance, Range<BigInteger>> tokenRangeMap = ArrayListMultimap.create();

        if (replicationFactor.getReplicationStrategy() == ReplicationFactor.ReplicationStrategy.SimpleStrategy)
        {
            tokenRangeMap.putAll(RangeUtils.calculateTokenRanges(instances,
                                                                 replicationFactor.getTotalReplicationFactor(),
                                                                 partitioner));
        }
        else if (replicationFactor.getReplicationStrategy() == ReplicationFactor.ReplicationStrategy.NetworkTopologyStrategy)
        {
            for (final String dataCenter : replicationFactor.getOptions().keySet())
            {
                final int rf = replicationFactor.getOptions().get(dataCenter);
                if (rf == 0)
                {
                    // Apparently, its valid to have zero replication factor in Cassandra
                    continue;
                }
                List<RingInstance> dcInstances = instances.stream()
                                                          .filter(instance -> instance.getDataCenter().matches(dataCenter))
                                                          .collect(Collectors.toList());
                tokenRangeMap.putAll(RangeUtils.calculateTokenRanges(dcInstances,
                                                                     replicationFactor.getOptions().get(dataCenter),
                                                                     partitioner));
            }
        }
        else
        {
            throw new UnsupportedOperationException("Unsupported replication strategy");
        }
        return tokenRangeMap;
    }

    @NotNull
    private static ReplicationFactor getReplicationFactor(Map<String, Integer> rfByDC)
    {
        ImmutableMap.Builder<String, String> optionsBuilder = ImmutableMap.<String, String>builder()
                                                                          .put("class", "org.apache.cassandra.locator.NetworkTopologyStrategy");
        rfByDC.forEach((key, value) -> optionsBuilder.put(key, value.toString()));
        return new ReplicationFactor(optionsBuilder.build());
    }

    @NotNull
    public static List<RingInstance> getInstances(int initialToken, Map<String, Integer> rfByDC, int instancesPerDc)
    {
        ArrayList<RingInstance> instances = new ArrayList<>();
        int dcOffset = 0;
        for (Map.Entry<String, Integer> rfForDC : rfByDC.entrySet())
        {
            final String datacenter = rfForDC.getKey();
            for (int i = 0; i < instancesPerDc; i++)
            {
                RingEntry.Builder ringEntry = new RingEntry.Builder()
                                              .address("127.0." + dcOffset + "." + i)
                                              .datacenter(datacenter)
                                              .load("0")
                                              .token(Integer.toString(initialToken + dcOffset + 100_000 * i))
                                              .fqdn(datacenter + "-i" + i)
                                              .rack("Rack")
                                              .hostId("")
                                              .status("UP")
                                              .state("NORMAL")
                                              .owns("");
                instances.add(new RingInstance(ringEntry.build()));
            }
            dcOffset++;
        }
        return instances;
    }
}
