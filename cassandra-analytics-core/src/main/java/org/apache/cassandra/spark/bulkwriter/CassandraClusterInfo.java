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
import java.util.Collections;
import java.util.Comparator;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Set;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;
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
import org.apache.cassandra.sidecar.common.data.RingResponse;
import org.apache.cassandra.sidecar.common.data.SchemaResponse;
import org.apache.cassandra.sidecar.common.data.TimeSkewResponse;
import org.apache.cassandra.sidecar.common.data.TokenRangeReplicasResponse;
import org.apache.cassandra.spark.bulkwriter.token.CassandraRing;
import org.apache.cassandra.spark.bulkwriter.token.TokenRangeMapping;
import org.apache.cassandra.spark.common.client.InstanceState;
import org.apache.cassandra.spark.common.client.InstanceStatus;
import org.apache.cassandra.spark.data.ReplicationFactor;
import org.apache.cassandra.spark.data.partitioner.Partitioner;
import org.apache.cassandra.spark.utils.CqlUtils;
import org.apache.cassandra.spark.utils.FutureUtils;
import org.jetbrains.annotations.NotNull;

public class CassandraClusterInfo implements ClusterInfo, Closeable
{
    private static final long serialVersionUID = -6944818863462956767L;
    private static final Logger LOGGER = LoggerFactory.getLogger(CassandraClusterInfo.class);

    protected final BulkSparkConf conf;
    protected String cassandraVersion;
    protected Partitioner partitioner;
    protected transient CassandraRing ring;

    protected transient TokenRangeMapping<RingInstance> tokenRangeReplicas;
    protected  transient Map<RingInstance, InstanceAvailability> availability;
    protected transient String keyspaceSchema;
    protected transient volatile RingResponse ringResponse;
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
     *
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
            ringResponse = null;
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

    @NotNull
    protected CassandraRing getCurrentRing()
    {
        ReplicationFactor replicationFactor = getReplicationFactor();
        return new CassandraRing(getPartitioner(), conf.keyspace, replicationFactor);
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
    public CassandraRing getRing(boolean cached)
    {
        CassandraRing currentRing = ring;
        if (cached && currentRing != null)
        {
            return currentRing;
        }

        synchronized (this)
        {
            if (!cached || ring == null)
            {
                try
                {
                    ring = getCurrentRing();
                }
                catch (Exception exception)
                {
                    throw new RuntimeException("Unable to initialize ring information", exception);
                }
            }
            return ring;
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
            if (!cached || tokenRangeReplicas == null)
            {
                try
                {
                    tokenRangeReplicas = getTokenRangeReplicas();
                }
                catch (Exception exception)
                {
                    throw new RuntimeException("Unable to initialize ring information", exception);
                }
            }
            return tokenRangeReplicas;
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
    public Map<RingInstance, InstanceAvailability> getInstanceAvailability(boolean cached)
    {
        Map<RingInstance, InstanceAvailability> availability = this.availability;
        if (cached && availability != null)
        {
            return availability;
        }

        synchronized (this)
        {
            if (!cached || availability == null)
            {
                try
                {
                    availability = getInstanceAvailabilityFromRing();
                }
                catch (Exception exception)
                {
                    throw new RuntimeException("Unable to initialize ring information", exception);
                }
            }
            return availability;
        }
    }

    private Map<RingInstance, InstanceAvailability> getInstanceAvailabilityFromRing()
    {
        RingResponse ringResp = getRingResponse();
        final Map<RingInstance, InstanceAvailability> result = ringResp.stream()
                                                                       .map(RingInstance::new)
                                                                       .collect(
                                                                       Collectors.toMap(Function.identity(),
                                                                                        this::determineInstanceAvailability));
        if (LOGGER.isDebugEnabled())
        {
            result.forEach((inst, avail) -> LOGGER.debug("Instance {} has availability {}", inst, avail));
        }
        return result;
    }

    private InstanceAvailability determineInstanceAvailability(final RingInstance instance)
    {
        if (!instanceIsUp(instance.getRingInstance()))
        {
            return InstanceAvailability.UNAVAILABLE_DOWN;
        }
        if (instanceIsJoining(instance.getRingInstance()) && isReplacement(instance))
        {
            return InstanceAvailability.UNAVAILABLE_REPLACEMENT;
        }
        if (instanceIsBlocked(instance))
        {
            return InstanceAvailability.UNAVAILABLE_BLOCKED;
        }
        if (instanceIsNormal(instance.getRingInstance()))
        {
            return InstanceAvailability.AVAILABLE;
        }

        LOGGER.info("No valid state found for instance {}", instance);
        // If it's not one of the above, it's inherently INVALID.
        return InstanceAvailability.INVALID_STATE;
    }

    private TokenRangeMapping<RingInstance> getTokenRangeReplicas()
    {
        final Map<String, Set<String>> writeReplicasByDC;
        final Map<String, Set<String>> pendingReplicasByDC;
        final Set<RingInstance> blockedInstances;
        final Set<RingInstance> replacementInstances;
        Multimap<RingInstance, Range<BigInteger>> tokenRangesByInstance;
        try
        {
            TokenRangeReplicasResponse response = getTokenRangesAndReplicaSets();

            tokenRangesByInstance = getTokenRangesByInstance(response.writeReplicas());
            LOGGER.info("Retrieved token ranges for {} instances from write replica set ",
                        tokenRangesByInstance.size());

            // Write-replica-set includes available and pending instances; is used to prepare the token-range map
            Map<InstanceAvailability, Set<RingInstance>> availability = getInstanceAvailability(false)
                                                                        .entrySet()
                                                                        .stream()
                                                                        .collect(Collectors.groupingBy(Map.Entry::getValue,
                                                                                                       Collectors.mapping(Map.Entry::getKey,
                                                                                                                          Collectors.toSet())));
            // TODO: Make this by DC; for instances to be added to "failed" set during CL checks
            blockedInstances = availability.getOrDefault(InstanceAvailability.UNAVAILABLE_BLOCKED, Collections.emptySet());
            replacementInstances = availability.getOrDefault(InstanceAvailability.UNAVAILABLE_REPLACEMENT, Collections.emptySet());

            Set<String> blockedIPs = blockedInstances.stream().map(RingInstance::getIpAddress).collect(Collectors.toSet());

            // Each token range has hosts by DC. We collate them into all hosts by DC
            writeReplicasByDC = response.writeReplicas()
                                        .stream()
                                        .flatMap(wr -> wr.replicasByDatacenter().entrySet().stream())
                                        .collect(Collectors.toMap(Map.Entry::getKey, e -> new HashSet<>(e.getValue()),
                                                                  (l1, l2) -> filterAndMergeInstances(l1, l2, blockedIPs)));

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
                                       writeReplicasByDC,
                                       pendingReplicasByDC,
                                       tokenRangesByInstance,
                                       blockedInstances,
                                       replacementInstances);
    }

    private Set<String> filterAndMergeInstances(Set<String> instancesList1, Set<String> instancesList2, Set<String> blockedIPs)
    {
        Set<String> merged = new HashSet<>();
        // Removes blocked instances. If this is included, remove blockedInstances from CL checks
//        merged.addAll(instancesList1.stream().filter(i -> !blockedIPs.contains(i)).collect(Collectors.toSet()));
//        merged.addAll(instancesList2.stream().filter(i -> !blockedIPs.contains(i)).collect(Collectors.toSet()));

        // Blocked instances are included in write-replicas. TODO: Remove from write-path
        merged.addAll(instancesList1);
        merged.addAll(instancesList2);
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

    private Multimap<RingInstance, Range<BigInteger>> getTokenRangesByInstance(List<TokenRangeReplicasResponse.ReplicaInfo> writeReplicas)
    {
        Multimap<RingInstance, Range<BigInteger>> tokenRanges = ArrayListMultimap.create();
        for (TokenRangeReplicasResponse.ReplicaInfo rInfo: writeReplicas)
        {
            // TODO: These are open-closed ranges
            Range<BigInteger> range = Range.openClosed(new BigInteger(rInfo.start()), new BigInteger(rInfo.end()));
            for (Map.Entry<String, List<String>> dcReplicaEntry: rInfo.replicasByDatacenter().entrySet())
            {
                dcReplicaEntry.getValue().forEach(ipAddress -> {
                    // For a given range, we can expect the instances to be unique => create instance each time
                    String hostIp = ipAddress.contains(":") ? ipAddress.split(":")[0] : ipAddress;

                    // We  query the sidecar ring endpoint for the list of instances as they include the instance metadata
                    // which is not currently available in the token-range API response.
                    Optional<RingInstance> ringInstance = getRingResponse().stream()
                                                                           .filter(e -> e.address().equals(hostIp))
                                                                           .map(RingInstance::new)
                                                                           .findAny();

                    // Note: Instances returned from token-ranges endpoint should match those from the ring endpoint
                    // This should not happen. In the event that they do not match, we log as error for debugging.
                    if (!ringInstance.isPresent())
//                        || skippedInstances.contains(ringInstance.get()))
                    {
                        LOGGER.error("Instance {} with token range not found in the ring", hostIp);
                    }
                    else
                    {
                        RingEntry entry = ringInstance.get().getRingInstance();
                        tokenRanges.put(new RingInstance(entry), range);
                    }
                });
            }
        }
        return tokenRanges;
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

    protected RingResponse getRingResponse()
    {
        RingResponse currentRingResponse = ringResponse;
        if (currentRingResponse != null)
        {
            return currentRingResponse;
        }

        synchronized (this)
        {
            if (ringResponse == null)
            {
                try
                {
                    ringResponse = getCurrentRingResponse();
                }
                catch (Exception exception)
                {
                    LOGGER.error("Failed to load Cassandra ring", exception);
                    throw new RuntimeException(exception);
                }
            }
            return ringResponse;
        }
    }

    private RingResponse getCurrentRingResponse() throws Exception
    {
        return getCassandraContext().getSidecarClient().ring(conf.keyspace).get();
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

    private boolean instanceIsJoining(RingEntry ringEntry)
    {
        return InstanceState.JOINING.name().equalsIgnoreCase(ringEntry.state());
    }

    private boolean isReplacement(final RingInstance instance)
    {
        GossipInfoResponse gossipInfo = getGossipInfo(true);
        LOGGER.info("Gossipinfo: {} while checking for instance with IP: {} and fqdn: {}",
                    gossipInfo,
                    instance.getIpAddress(),
                    instance.getNodeName());

        String gossipKeySuffix = "/" + instance.getIpAddress();
        String gossipKey = (instance.getNodeName().isEmpty() || instance.getNodeName().equals("?"))
                           ? gossipKeySuffix
                           : instance.getNodeName() + gossipKeySuffix;

        LOGGER.info("Looking up gossip key {} GossipInfo keyset: {}", gossipKey, gossipInfo.keySet());
        GossipInfoResponse.GossipInfo hostInfo = gossipInfo.get(gossipKey);
        if (hostInfo != null)
        {
            LOGGER.info("Found hostinfo: {}", hostInfo);
            String hostStatus = hostInfo.status();
            if (hostStatus != null)
            {
                // If status has gone to NORMAL, we can't determine here if this was a host replacement or not.
                // CassandraRingManager will handle detecting the ring change if it's gone NORMAL after the job starts.
                return hostStatus.startsWith("BOOT_REPLACE,") || hostStatus.equals("NORMAL");
            }
        }
        return false;
    }

    protected GossipInfoResponse getGossipInfo(boolean forceRefresh)
    {
        GossipInfoResponse currentGossipInfo = this.gossipInfo;
        if (!forceRefresh && currentGossipInfo != null)
        {
            return currentGossipInfo;
        }
        else
        {
            synchronized (this)
            {
                if (forceRefresh || this.gossipInfo == null)
                {
                    try
                    {
                        this.gossipInfo = this.cassandraContext.getSidecarClient()
                                                               .gossipInfo()
                                                               .get(this.conf.getHttpResponseTimeoutMs(), TimeUnit.MILLISECONDS);
                    }
                    catch (InterruptedException | ExecutionException var6)
                    {
                        LOGGER.error("Failed to retrieve gossip information");
                        throw new RuntimeException("Failed to retrieve gossip information", var6);
                    }
                    catch (TimeoutException var7)
                    {
                        Thread.currentThread().interrupt();
                        throw new RuntimeException("Failed to retrieve gossip information", var7);
                    }
                }

                return this.gossipInfo;
            }
        }
    }
}
