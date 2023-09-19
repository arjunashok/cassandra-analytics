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

package org.apache.cassandra.spark.bulkwriter.token;

import java.io.Serializable;
import java.math.BigInteger;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.stream.Collectors;

import com.google.common.base.Preconditions;
import com.google.common.collect.Multimap;
import com.google.common.collect.Range;
import com.google.common.collect.RangeMap;
import com.google.common.collect.TreeRangeMap;

import org.apache.cassandra.sidecar.common.data.TokenRangeReplicasResponse.ReplicaMetadata;
import org.apache.cassandra.spark.bulkwriter.RingInstance;
import org.apache.cassandra.spark.common.model.CassandraInstance;
import org.apache.cassandra.spark.data.partitioner.Partitioner;

public class TokenRangeMapping<Instance extends CassandraInstance> implements Serializable
{

    private final Partitioner partitioner;

    private transient Set<RingInstance> blockedInstances;
    private transient Set<RingInstance> replacementInstances;
    private transient RangeMap<BigInteger, List<Instance>> replicasByTokenRange;
    private transient Multimap<Instance, Range<BigInteger>> tokenRangeMap;
    private transient Map<String, Set<String>> writeReplicasByDC;
    private transient Map<String, Set<String>> pendingReplicasByDC;
    private transient List<ReplicaMetadata> replicaMetadata;
    public TokenRangeMapping(final Partitioner partitioner,
                             final Map<String, Set<String>> writeReplicasByDC,
                             final Map<String, Set<String>> pendingReplicasByDC,
                             final Multimap<Instance, Range<BigInteger>> tokenRanges,
                             final List<ReplicaMetadata> replicaMetadata,
                             final Set<RingInstance> blockedInstances,
                             final Set<RingInstance> replacementInstances)
    {
        this.partitioner = partitioner;
        this.tokenRangeMap = tokenRanges;
        this.pendingReplicasByDC = pendingReplicasByDC;
        this.writeReplicasByDC = writeReplicasByDC;
        this.replicaMetadata = replicaMetadata;
        this.blockedInstances = blockedInstances;
        this.replacementInstances = replacementInstances;
        this.replicaMetadata = replicaMetadata;
        // Populate reverse mapping of ranges to replicas
        this.replicasByTokenRange = TreeRangeMap.create();
        populateReplicas();
    }

    /**
     * Add a replica with given range to replicaMap (RangeMap pointing to replicas).
     * <p>
     * replicaMap starts with full range (representing complete ring) with empty list of replicas. So, it is
     * guaranteed that range will match one or many ranges in replicaMap.
     * <p>
     * Scheme to add a new replica for a range
     * * Find overlapping rangeMap entries from replicaMap
     * * For each overlapping range, create new replica list by adding new replica to the existing list and add it
     * back to replicaMap
     */
    private static <Instance extends CassandraInstance> void addReplica(final Instance replica,
                                                                        final Range<BigInteger> range,
                                                                        final RangeMap<BigInteger, List<Instance>> replicaMap)
    {
        Preconditions.checkArgument(range.lowerEndpoint().compareTo(range.upperEndpoint()) <= 0,
                                    "Range calculations assume range is not wrapped");

        final RangeMap<BigInteger, List<Instance>> replicaRanges = replicaMap.subRangeMap(range);
        final RangeMap<BigInteger, List<Instance>> mappingsToAdd = TreeRangeMap.create();

        replicaRanges.asMapOfRanges().forEach((key, value) -> {
            final List<Instance> replicas = new ArrayList<>(value);
            replicas.add(replica);
            mappingsToAdd.put(key, replicas);
        });
        replicaMap.putAll(mappingsToAdd);
    }

    public List<ReplicaMetadata> getReplicaMetadata()
    {
        return replicaMetadata;
    }

    public Set<String> getPendingReplicas()
    {
        return (pendingReplicasByDC == null || pendingReplicasByDC.isEmpty())
               ? Collections.emptySet() : pendingReplicasByDC.values().stream().flatMap(Set::stream).collect(Collectors.toSet());
    }

    public Set<String> getPendingReplicas(String datacenter)
    {
        return (pendingReplicasByDC == null || pendingReplicasByDC.isEmpty())
               ? Collections.emptySet() : pendingReplicasByDC.get(datacenter).stream().collect(Collectors.toSet());
    }

    public Set<String> getWriteReplicas()
    {
        return (writeReplicasByDC == null || writeReplicasByDC.isEmpty())
               ? Collections.emptySet() : writeReplicasByDC.values().stream().flatMap(Set::stream).collect(Collectors.toSet());
    }

    public Set<String> getWriteReplicas(String datacenter)
    {
        return (writeReplicasByDC == null || writeReplicasByDC.isEmpty())
               ? Collections.emptySet() : writeReplicasByDC.get(datacenter).stream().collect(Collectors.toSet());
    }

    public Set<String> getBlockedInstances()
    {
        return blockedInstances.stream()
                               .map(RingInstance::getIpAddress)
                               .collect(Collectors.toSet());

    }

    public Set<String> getBlockedInstances(String datacenter)
    {
        return blockedInstances.stream()
                                   .filter(r -> r.getDataCenter().equalsIgnoreCase(datacenter))
                                   .map(RingInstance::getIpAddress)
                                   .collect(Collectors.toSet());
    }


    public Set<String> getReplacementInstances()
    {
        return replacementInstances.stream()
                                   .map(RingInstance::getIpAddress)
                                   .collect(Collectors.toSet());
    }

    public Set<String> getReplacementInstances(String datacenter)
    {
        return replacementInstances.stream()
                                   .filter(r -> r.getDataCenter().equalsIgnoreCase(datacenter))
                                   .map(RingInstance::getIpAddress)
                                   .collect(Collectors.toSet());
    }

    // Used for writes
    public RangeMap<BigInteger, List<Instance>> getRangeMap()
    {
        return this.replicasByTokenRange;
    }

    public RangeMap<BigInteger, List<Instance>> getSubRanges(final Range<BigInteger> tokenRange)
    {
        return replicasByTokenRange.subRangeMap(tokenRange);
    }

    public Multimap<Instance, Range<BigInteger>> getTokenRanges()
    {
        return this.tokenRangeMap;
    }

    private void populateReplicas()
    {
        // Calculate token range to replica mapping
        this.replicasByTokenRange.put(Range.closed(this.partitioner.minToken(), this.partitioner.maxToken()), Collections.emptyList());
        this.tokenRangeMap.asMap().forEach((inst, ranges) -> ranges.forEach(range -> addReplica(inst, range, this.replicasByTokenRange)));
    }

    @Override
    public boolean equals(final Object other)
    {
        if (this == other)
        {
            return true;
        }
        if (other == null || getClass() != other.getClass())
        {
            return false;
        }

        final TokenRangeMapping<?> that = (TokenRangeMapping<?>) other;

        if (!writeReplicasByDC.equals(that.writeReplicasByDC)
            || !pendingReplicasByDC.equals(that.pendingReplicasByDC))
        {
            return false;
        }

        return true;
    }

    @Override
    public int hashCode()
    {
        int result = tokenRangeMap.hashCode();
        result = 31 * result + pendingReplicasByDC.hashCode();
        result = 31 * result + writeReplicasByDC.hashCode();
        result = 31 * result + replicasByTokenRange.hashCode();
        return result;
    }
}
