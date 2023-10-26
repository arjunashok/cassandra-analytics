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

import java.io.Closeable;
import java.math.BigInteger;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Comparator;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicReference;
import java.util.function.Function;
import java.util.stream.Collectors;

import com.google.common.annotations.VisibleForTesting;
import com.google.common.collect.ArrayListMultimap;
import com.google.common.collect.Multimap;
import com.google.common.collect.Range;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import org.apache.cassandra.bridge.CassandraVersionFeatures;
import org.apache.cassandra.clients.Sidecar;
import org.apache.cassandra.sidecar.client.SidecarInstance;
import org.apache.cassandra.sidecar.client.SidecarInstanceImpl;
import org.apache.cassandra.sidecar.common.NodeSettings;
import org.apache.cassandra.sidecar.common.data.GossipInfoResponse;
import org.apache.cassandra.sidecar.common.data.RingEntry;
import org.apache.cassandra.sidecar.common.data.SchemaResponse;
import org.apache.cassandra.sidecar.common.data.TimeSkewResponse;
import org.apache.cassandra.sidecar.common.data.TokenRangeReplicasResponse;
import org.apache.cassandra.sidecar.common.data.TokenRangeReplicasResponse.ReplicaInfo;
import org.apache.cassandra.sidecar.common.data.TokenRangeReplicasResponse.ReplicaMetadata;
import org.apache.cassandra.spark.bulkwriter.token.TokenRangeMapping;
import org.apache.cassandra.spark.common.client.InstanceState;
import org.apache.cassandra.spark.common.client.InstanceStatus;
import org.apache.cassandra.spark.data.ReplicationFactor;
import org.apache.cassandra.spark.data.partitioner.Partitioner;
import org.apache.cassandra.spark.utils.CqlUtils;
import org.apache.cassandra.spark.utils.FutureUtils;
import org.jetbrains.annotations.NotNull;
import org.sparkproject.guava.net.HostAndPort;

public class CassandraClusterInfo implements ClusterInfo, Closeable
{
    private static final long serialVersionUID = -6944818863462956767L;
    private static final Logger LOGGER = LoggerFactory.getLogger(CassandraClusterInfo.class);

    protected final BulkSparkConf conf;
    protected String cassandraVersion;
    protected Partitioner partitioner;

    protected transient TokenRangeMapping<RingInstance> tokenRangeReplicas;
    protected transient String keyspaceSchema;
    protected transient GossipInfoResponse gossipInfo;
    protected transient CassandraContext cassandraContext;
    protected final transient AtomicReference<NodeSettings> nodeSettings;
    protected final transient List<CompletableFuture<NodeSettings>> allNodeSettingFutures;

    public CassandraClusterInfo(BulkSparkConf conf)
    {
        this.conf = conf;
        this.cassandraContext = buildCassandraContext();
        LOGGER.info("Getting Cassandra versions from all nodes");
        this.nodeSettings = new AtomicReference<>(null);
        this.allNodeSettingFutures = Sidecar.allNodeSettings(cassandraContext.getSidecarClient(),
                                                             cassandraContext.clusterConfig);
    }

    @Override
    public void checkBulkWriterIsEnabledOrThrow()
    {
        // DO NOTHING
    }

    public String getVersion()
    {
        return CassandraClusterInfo.class.getPackage().getImplementationVersion();
    }

    @Override
    public boolean instanceIsAvailable(RingInstance ringInstance)
    {
        return instanceIsUp(ringInstance.getRingInstance())
               && instanceIsNormal(ringInstance.getRingInstance())
               && !instanceIsBlocked(ringInstance);
    }

    @Override
    public InstanceState getInstanceState(RingInstance ringInstance)
    {
        return InstanceState.valueOf(ringInstance.getRingInstance().state().toUpperCase());
    }

    public CassandraContext getCassandraContext()
    {
        CassandraContext currentCassandraContext = cassandraContext;
        if (currentCassandraContext != null)
        {
            return currentCassandraContext;
        }

        synchronized (this)
        {
            if (cassandraContext == null)
            {
                cassandraContext = buildCassandraContext();
            }
            return cassandraContext;
        }
    }

    /**
     * Gets a Cassandra Context
     * <p>
     * NOTE: The caller of this method is required to call `shutdown` on the returned CassandraContext instance
     *
     * @return an instance of CassandraContext based on the configuration settings
     */
    protected CassandraContext buildCassandraContext()
    {
        return buildCassandraContext(conf);
    }

    private static CassandraContext buildCassandraContext(BulkSparkConf conf)
    {
        return CassandraContext.create(conf);
    }

    @Override
    public void close()
    {
        synchronized (this)
        {
            LOGGER.info("Closing {}", this);
            getCassandraContext().close();
        }
    }

    @Override
    public Partitioner getPartitioner()
    {
        Partitioner currentPartitioner = partitioner;
        if (currentPartitioner != null)
        {
            return currentPartitioner;
        }

        synchronized (this)
        {
            if (partitioner == null)
            {
                try
                {
                    String partitionerString;
                    NodeSettings currentNodeSettings = nodeSettings.get();
                    if (currentNodeSettings != null)
                    {
                        partitionerString = currentNodeSettings.partitioner();
                    }
                    else
                    {
                        partitionerString = getCassandraContext().getSidecarClient().nodeSettings().get().partitioner();
                    }
                    partitioner = Partitioner.from(partitionerString);
                }
                catch (ExecutionException | InterruptedException exception)
                {
                    throw new RuntimeException("Unable to retrieve partitioner information", exception);
                }
            }
            return partitioner;
        }
    }

    @Override
    public TimeSkewResponse getTimeSkew(List<RingInstance> replicas)
    {
        try
        {
            List<SidecarInstance> instances = replicas
                                              .stream()
                                              .map(replica -> new SidecarInstanceImpl(replica.getNodeName(), conf.getSidecarPort()))
                                              .collect(Collectors.toList());
            return getCassandraContext().getSidecarClient().timeSkew(instances).get();
        }
        catch (InterruptedException | ExecutionException exception)
        {
            throw new RuntimeException(exception);
        }
    }

    @Override
    public void refreshClusterInfo()
    {
        synchronized (this)
        {
            // Set backing stores to null and let them lazy-load on the next call
            gossipInfo = null;
            keyspaceSchema = null;
            getCassandraContext().refreshClusterConfig();
        }
    }

    protected String getCurrentKeyspaceSchema() throws Exception
    {
        SchemaResponse schemaResponse = getCassandraContext().getSidecarClient().schema(conf.keyspace).get();
        return schemaResponse.schema();
    }

    private TokenRangeReplicasResponse getTokenRangesAndReplicaSets() throws ExecutionException, InterruptedException
    {
        CassandraContext context = getCassandraContext();
        return context.getSidecarClient().tokenRangeReplicas(new ArrayList<>(context.getCluster()), conf.keyspace).get();
    }

    private Set<String> readReplicasFromTokenRangeResponse(TokenRangeReplicasResponse response)
    {
        return response.readReplicas().stream()
                       .flatMap(rr -> rr.replicasByDatacenter().values().stream())
                       .flatMap(List::stream).collect(Collectors.toSet());
    }

    @NotNull
    protected ReplicationFactor getReplicationFactor()
    {
        String keyspaceSchema = getKeyspaceSchema(true);
        if (keyspaceSchema == null)
        {
            throw new RuntimeException(String.format("Could not keyspace schema information for keyspace %s",
                                                     conf.keyspace));
        }
        return CqlUtils.extractReplicationFactor(keyspaceSchema, conf.keyspace);
    }

    @Override
    public String getKeyspaceSchema(boolean cached)
    {
        String currentKeyspaceSchema = keyspaceSchema;
        if (cached && currentKeyspaceSchema != null)
        {
            return currentKeyspaceSchema;
        }

        synchronized (this)
        {
            if (!cached || keyspaceSchema == null)
            {
                try
                {
                    keyspaceSchema = getCurrentKeyspaceSchema();
                }
                catch (Exception exception)
                {
                    throw new RuntimeException("Unable to initialize schema information for keyspace " + conf.keyspace,
                                               exception);
                }
            }
            return keyspaceSchema;
        }
    }

    @Override
    public TokenRangeMapping<RingInstance> getTokenRangeMapping(boolean cached)
    {
        TokenRangeMapping<RingInstance> tokenRangeReplicas = this.tokenRangeReplicas;
        if (cached && tokenRangeReplicas != null)
        {
            return tokenRangeReplicas;
        }

        synchronized (this)
        {
            if (!cached || this.tokenRangeReplicas == null)
            {
                try
                {
                    this.tokenRangeReplicas = getTokenRangeReplicas();
                }
                catch (Exception exception)
                {
                    throw new RuntimeException("Unable to initialize ring information", exception);
                }
            }
            return this.tokenRangeReplicas;
        }
    }

    @Override
    public String getLowestCassandraVersion()
    {
        String currentCassandraVersion = cassandraVersion;
        if (currentCassandraVersion != null)
        {
            return currentCassandraVersion;
        }

        synchronized (this)
        {
            if (cassandraVersion == null)
            {
                String versionFromFeature = getVersionFromFeature();
                if (versionFromFeature != null)
                {
                    // Forcing writer to use a particular version
                    cassandraVersion = versionFromFeature;
                }
                else
                {
                    cassandraVersion = getVersionFromSidecar();
                }
            }
        }
        return cassandraVersion;
    }

    @Override
    public Map<RingInstance, InstanceAvailability> getInstanceAvailability()
    {
        TokenRangeMapping<RingInstance> mapping = getTokenRangeMapping(true);
        Map<RingInstance, InstanceAvailability> result =
        mapping.getReplicaMetadata()
               .stream()
               .map(RingInstance::new)
               .collect(Collectors.toMap(Function.identity(), this::determineInstanceAvailability));

        if (LOGGER.isDebugEnabled())
        {
            result.forEach((inst, avail) -> LOGGER.debug("Instance {} has availability {}", inst, avail));
        }
        return result;
    }

    private InstanceAvailability determineInstanceAvailability(RingInstance instance)
    {
        if (!instanceIsUp(instance.getRingInstance()))
        {
            return InstanceAvailability.UNAVAILABLE_DOWN;
        }
        if (instanceIsBlocked(instance))
        {
            return InstanceAvailability.UNAVAILABLE_BLOCKED;
        }
        if (instanceIsNormal(instance.getRingInstance()) ||
            instanceIsInTransientState(instance.getRingInstance()) ||
            instanceIsBeingReplaced(instance.getRingInstance()))
        {
            return InstanceAvailability.AVAILABLE;
        }

        LOGGER.info("No valid state found for instance {}", instance);
        // If it's not one of the above, it's inherently INVALID.
        return InstanceAvailability.INVALID_STATE;
    }

    private TokenRangeMapping<RingInstance> getTokenRangeReplicas()
    {
        Map<String, Set<String>> writeReplicasByDC;
        Map<String, Set<String>> pendingReplicasByDC;
        List<ReplicaMetadata> replicaMetadata;
        Set<RingInstance> blockedInstances;
        Set<RingInstance> replacementInstances;
        Multimap<RingInstance, Range<BigInteger>> tokenRangesByInstance;
        try
        {
            TokenRangeReplicasResponse response = getTokenRangesAndReplicaSets();
            replicaMetadata = response.replicaMetadata();

            tokenRangesByInstance = getTokenRangesByInstance(response.writeReplicas(), response.replicaMetadata());
            LOGGER.info("Retrieved token ranges for {} instances from write replica set ",
                        tokenRangesByInstance.size());

            replacementInstances = response.replicaMetadata()
                                           .stream()
                                           .filter(m -> m.state().equalsIgnoreCase(InstanceState.REPLACING.toString()))
                                           .map(RingInstance::new)
                                           .collect(Collectors.toSet());

            blockedInstances = response.replicaMetadata().stream()
                                       .map(RingInstance::new)
                                       .filter(this::instanceIsBlocked)
                                       .collect(Collectors.toSet());

            Set<String> blockedIps = blockedInstances.stream().map(i -> i.getRingInstance().address())
                                                     .collect(Collectors.toSet());

            // Each token range has hosts by DC. We collate them across all ranges into all hosts by DC
            writeReplicasByDC = response.writeReplicas()
                                        .stream()
                                        .flatMap(wr -> wr.replicasByDatacenter().entrySet().stream())
                                        .collect(Collectors.toMap(Map.Entry::getKey, e -> new HashSet<>(e.getValue()),
                                                                  (l1, l2) -> filterAndMergeInstances(l1, l2, blockedIps)));

            pendingReplicasByDC = getPendingReplicas(response, writeReplicasByDC);

            if (LOGGER.isDebugEnabled())
            {
                LOGGER.debug("Fetched token-ranges with dcs={}, write_replica_count={}, pending_replica_count={}",
                             writeReplicasByDC.keySet(),
                             writeReplicasByDC.values().stream().flatMap(Collection::stream).collect(Collectors.toSet()).size(),
                             pendingReplicasByDC.values().stream().flatMap(Collection::stream).collect(Collectors.toSet()).size());
            }
        }
        catch (ExecutionException | InterruptedException exception)
        {
            LOGGER.error("Failed to get token ranges, ", exception);
            throw new RuntimeException(exception);
        }

        // Include availability info so CL checks can use it to exclude replacement hosts
        return new TokenRangeMapping<>(getPartitioner(),
                                       getReplicationFactor(),
                                       writeReplicasByDC,
                                       pendingReplicasByDC,
                                       tokenRangesByInstance,
                                       replicaMetadata,
                                       blockedInstances,
                                       replacementInstances);
    }

    private Set<String> filterAndMergeInstances(Set<String> instancesList1, Set<String> instancesList2, Set<String> blockedIPs)
    {
        Set<String> merged = new HashSet<>();
        // Removes blocked instances. If this is included, remove blockedInstances from CL checks
        merged.addAll(instancesList1.stream().filter(i -> !blockedIPs.contains(i)).collect(Collectors.toSet()));
        merged.addAll(instancesList2.stream().filter(i -> !blockedIPs.contains(i)).collect(Collectors.toSet()));

        return merged;
    }

    // Pending replicas are currently calculated by extracting the non-read-replicas from the write-replica-set
    // This will be replaced by the instance state metadata when it is supported by the token-ranges API
    private Map<String, Set<String>> getPendingReplicas(TokenRangeReplicasResponse response, Map<String, Set<String>> writeReplicasByDC)
    {
        Set<String> readReplicas = readReplicasFromTokenRangeResponse(response);
        return writeReplicasByDC.entrySet()
                                .stream()
                                .filter(entry -> entry.getValue().stream().noneMatch(readReplicas::contains))
                                .collect(Collectors.toMap(Map.Entry::getKey, Map.Entry::getValue));
    }

    private Multimap<RingInstance, Range<BigInteger>> getTokenRangesByInstance(List<ReplicaInfo> writeReplicas,
                                                                               List<ReplicaMetadata> replicaMetadata)
    {
        Multimap<RingInstance, Range<BigInteger>> instanceToRangeMap = ArrayListMultimap.create();
        for (ReplicaInfo rInfo : writeReplicas)
        {
            Range<BigInteger> range = Range.openClosed(new BigInteger(rInfo.start()), new BigInteger(rInfo.end()));
            for (Map.Entry<String, List<String>> dcReplicaEntry : rInfo.replicasByDatacenter().entrySet())
            {
                // For each writeReplica, get metadata and update map to include range
                dcReplicaEntry.getValue().forEach(ipAddress -> {
                    HostAndPort hap = HostAndPort.fromString(ipAddress);
                    // Get metadata for this IP; Create RingInstance
                    ReplicaMetadata replica = replicaMetadata.stream()
                                                             .filter(r -> r.address().equals(hap.getHostText()))
                                                             .findFirst().get();
                    instanceToRangeMap.put(new RingInstance(replica), range);
                });
            }
        }
        return instanceToRangeMap;
    }

    public String getVersionFromFeature()
    {
        return null;
    }

    protected List<NodeSettings> getAllNodeSettings()
    {
        List<NodeSettings> allNodeSettings = FutureUtils.bestEffortGet(allNodeSettingFutures,
                                                                       conf.getSidecarRequestMaxRetryDelayInSeconds(),
                                                                       TimeUnit.SECONDS);

        if (allNodeSettings.isEmpty())
        {
            throw new RuntimeException(String.format("Unable to determine the node settings. 0/%d instances available.",
                                                     allNodeSettingFutures.size()));
        }
        else if (allNodeSettings.size() < allNodeSettingFutures.size())
        {
            LOGGER.warn("{}/{} instances were used to determine the node settings",
                        allNodeSettings.size(), allNodeSettingFutures.size());
        }

        return allNodeSettings;
    }

    public String getVersionFromSidecar()
    {
        NodeSettings nodeSettings = this.nodeSettings.get();
        if (nodeSettings != null)
        {
            return nodeSettings.releaseVersion();
        }

        return getLowestVersion(getAllNodeSettings());
    }

    @VisibleForTesting
    public String getLowestVersion(List<NodeSettings> allNodeSettings)
    {
        NodeSettings ns = this.nodeSettings.get();
        if (ns != null)
        {
            return ns.releaseVersion();
        }

        // It is possible to run the below computation multiple times. Since the computation is local-only, it is OK.
        ns = allNodeSettings
             .stream()
             .filter(settings -> !settings.releaseVersion().equalsIgnoreCase("unknown"))
             .min(Comparator.comparing(settings ->
                                       CassandraVersionFeatures.cassandraVersionFeaturesFromCassandraVersion(settings.releaseVersion())))
             .orElseThrow(() -> new RuntimeException("No valid Cassandra Versions were returned from Cassandra Sidecar"));
        nodeSettings.compareAndSet(null, ns);
        return ns.releaseVersion();
    }

    protected boolean instanceIsBlocked(RingInstance ignored)
    {
        return false;
    }

    protected boolean instanceIsNormal(RingEntry ringEntry)
    {
        return InstanceState.NORMAL.name().equalsIgnoreCase(ringEntry.state());
    }

    protected boolean instanceIsUp(RingEntry ringEntry)
    {
        return InstanceStatus.UP.name().equalsIgnoreCase(ringEntry.status());
    }

    protected boolean instanceIsBeingReplaced(RingEntry ringEntry)
    {
        return InstanceState.REPLACING.name().equalsIgnoreCase(ringEntry.state());
    }

    private boolean instanceIsInTransientState(RingEntry ringEntry)
    {
        return InstanceState.JOINING.name().equalsIgnoreCase(ringEntry.state()) ||
               InstanceState.LEAVING.name().equalsIgnoreCase(ringEntry.state()) ||
               InstanceState.MOVING.name().equalsIgnoreCase(ringEntry.state());
    }

    // Startup Validation

    @Override
    public void startupValidate()
    {
        getCassandraContext().startupValidate();
    }
}
