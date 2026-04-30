package com.mongodb.analyzer;

import java.util.*;
import java.util.stream.Collectors;

/**
 * Top-level container for a full sharded cluster per-host analysis.
 * Call compute() after populating all input fields to derive imbalance metrics.
 */
public class ClusterAnalysisReport {

    // --- Input data ---
    private List<HostAnalysisResult> hostResults = new ArrayList<>();
    private List<ChunkDistributionStats> chunkDistribution = new ArrayList<>();
    // shardName -> list of zone names (from config.shards tags field)
    private Map<String, List<String>> shardZones = new LinkedHashMap<>();
    // namespace -> list of zone names that have zone ranges (from config.tags)
    private Map<String, List<String>> collectionZones = new LinkedHashMap<>();

    // --- Computed results ---
    private Map<String, ShardTotals> shardTotalsMap = new LinkedHashMap<>();
    // Full distribution: namespace -> shardName -> per-shard stats (from primaries only)
    private Map<String, Map<String, NamespaceShardEntry>> namespaceDistribution = new LinkedHashMap<>();
    private List<DataImbalanceEntry> dataImbalances = new ArrayList<>();
    private List<ChunkImbalanceEntry> chunkImbalances = new ArrayList<>();
    private List<ActivityImbalanceEntry> activityImbalances = new ArrayList<>();

    private static final double WARNING_THRESHOLD = 60.0;
    private static final double CRITICAL_THRESHOLD = 75.0;

    // -------------------------------------------------------------------------
    // Getters / setters for input data
    // -------------------------------------------------------------------------

    public List<HostAnalysisResult> getHostResults() { return hostResults; }
    public void setHostResults(List<HostAnalysisResult> hostResults) { this.hostResults = hostResults; }

    public List<ChunkDistributionStats> getChunkDistribution() { return chunkDistribution; }
    public void setChunkDistribution(List<ChunkDistributionStats> chunkDistribution) { this.chunkDistribution = chunkDistribution; }

    public Map<String, List<String>> getShardZones() { return shardZones; }
    public void setShardZones(Map<String, List<String>> shardZones) { this.shardZones = shardZones; }

    public Map<String, List<String>> getCollectionZones() { return collectionZones; }
    public void setCollectionZones(Map<String, List<String>> collectionZones) { this.collectionZones = collectionZones; }

    // -------------------------------------------------------------------------
    // Getters for computed results
    // -------------------------------------------------------------------------

    public Map<String, ShardTotals> getShardTotalsMap() { return shardTotalsMap; }
    public Map<String, Map<String, NamespaceShardEntry>> getNamespaceDistribution() { return namespaceDistribution; }
    public List<DataImbalanceEntry> getDataImbalances() { return dataImbalances; }
    public List<ChunkImbalanceEntry> getChunkImbalances() { return chunkImbalances; }
    public List<ActivityImbalanceEntry> getActivityImbalances() { return activityImbalances; }

    public long getTotalShards() {
        return hostResults.stream().map(HostAnalysisResult::getShardName).distinct().count();
    }

    public long getTotalHosts() { return hostResults.size(); }

    public long getTotalPrimaryHosts() {
        return hostResults.stream().filter(h -> "PRIMARY".equals(h.getRole())).count();
    }

    public long getClusterTotalDataSize() {
        return shardTotalsMap.values().stream().mapToLong(s -> s.totalDataSize).sum();
    }

    public long getClusterTotalStorageSize() {
        return shardTotalsMap.values().stream().mapToLong(s -> s.totalStorageSize).sum();
    }

    public long getClusterTotalOps() {
        return shardTotalsMap.values().stream().mapToLong(s -> s.totalOps).sum();
    }

    public long getTotalChunks() {
        return chunkDistribution.stream().mapToLong(ChunkDistributionStats::getTotalChunks).sum();
    }

    public boolean hasZones() {
        return shardZones.values().stream().anyMatch(z -> !z.isEmpty());
    }

    public Set<String> getZoneNames() {
        return shardZones.values().stream()
            .flatMap(Collection::stream)
            .collect(Collectors.toCollection(TreeSet::new));
    }

    // -------------------------------------------------------------------------
    // Compute derived metrics
    // -------------------------------------------------------------------------

    public void compute() {
        buildNamespaceDistribution();
        computeShardTotals();
        computeDataImbalances();
        computeChunkImbalances();
        computeActivityImbalances();
    }

    private void buildNamespaceDistribution() {
        namespaceDistribution.clear();
        for (HostAnalysisResult host : hostResults) {
            if (!"PRIMARY".equals(host.getRole())) continue;
            String shardName = host.getShardName();
            for (AnalysisResult dbResult : host.getDatabaseResults()) {
                for (CollectionStats coll : dbResult.getCollectionStats()) {
                    NamespaceShardEntry e = new NamespaceShardEntry();
                    e.storageSize = coll.getStorageSize();
                    e.documentCount = coll.getCount();
                    e.bytesReadIntoCache = coll.getBytesReadIntoCache();
                    e.bytesWrittenFromCache = coll.getBytesWrittenFromCache();
                    namespaceDistribution
                        .computeIfAbsent(coll.getNamespace(), k -> new TreeMap<>())
                        .put(shardName, e);
                }
            }
        }
    }

    private void computeShardTotals() {
        shardTotalsMap.clear();

        for (HostAnalysisResult host : hostResults) {
            String shardName = host.getShardName();
            ShardTotals totals = shardTotalsMap.computeIfAbsent(shardName, ShardTotals::new);
            totals.hostCount++;
            totals.zones = shardZones.getOrDefault(shardName, Collections.emptyList());

            // Cache stats: sum across all hosts in shard
            if (host.getWiredTigerStats() != null) {
                WiredTigerStats wt = host.getWiredTigerStats();
                totals.totalBytesReadIntoCache += wt.getBytesReadIntoCache();
                totals.totalBytesWrittenFromCache += wt.getBytesWrittenFromCache();
            }

            // Index ops: sum across all hosts in shard (captures read activity on secondaries)
            for (AnalysisResult dbResult : host.getDatabaseResults()) {
                for (IndexStats idx : dbResult.getIndexStats()) {
                    totals.totalOps += idx.getOps();
                }
            }

            // Data sizes: only from primary (authoritative for what lives on this shard)
            if ("PRIMARY".equals(host.getRole())) {
                for (AnalysisResult dbResult : host.getDatabaseResults()) {
                    for (CollectionStats coll : dbResult.getCollectionStats()) {
                        totals.totalDataSize += coll.getSize();
                        totals.totalStorageSize += coll.getStorageSize();
                        totals.totalIndexSize += coll.getTotalIndexSize();
                        totals.totalDocuments += coll.getCount();
                        totals.totalBytesReadIntoCacheByCollection += coll.getBytesReadIntoCache();
                        totals.totalBytesWrittenFromCacheByCollection += coll.getBytesWrittenFromCache();
                    }
                }
            }
        }
    }

    private void computeDataImbalances() {
        dataImbalances.clear();

        for (Map.Entry<String, Map<String, NamespaceShardEntry>> entry : namespaceDistribution.entrySet()) {
            String ns = entry.getKey();
            Map<String, NamespaceShardEntry> shardEntries = entry.getValue();
            if (shardEntries.size() <= 1) continue;

            List<String> nsZones = collectionZones.getOrDefault(ns, Collections.emptyList());
            Map<String, List<String>> zoneGroups = groupShardsByZone(new ArrayList<>(shardEntries.keySet()), nsZones);

            for (Map.Entry<String, List<String>> zg : zoneGroups.entrySet()) {
                String zoneName = zg.getKey();
                List<String> shardsInZone = zg.getValue();
                if (shardsInZone.size() <= 1) continue;

                long total = shardsInZone.stream()
                    .mapToLong(s -> shardEntries.containsKey(s) ? shardEntries.get(s).storageSize : 0L).sum();
                if (total == 0) continue;

                String maxShard = shardsInZone.stream()
                    .max(Comparator.comparingLong(s -> shardEntries.containsKey(s) ? shardEntries.get(s).storageSize : 0L))
                    .orElse(null);
                if (maxShard == null) continue;

                long maxVal = shardEntries.get(maxShard).storageSize;
                double maxPct = maxVal * 100.0 / total;

                if (maxPct >= WARNING_THRESHOLD) {
                    DataImbalanceEntry e = new DataImbalanceEntry();
                    e.namespace = ns;
                    e.zoneGroup = zoneName;
                    e.maxShard = maxShard;
                    e.maxPercent = maxPct;
                    e.totalSize = total;
                    e.maxSize = maxVal;
                    e.isCritical = maxPct >= CRITICAL_THRESHOLD;
                    e.shardEntries = shardEntries;
                    dataImbalances.add(e);
                }
            }
        }

        dataImbalances.sort((a, b) -> Double.compare(b.maxPercent, a.maxPercent));
    }

    private void computeChunkImbalances() {
        chunkImbalances.clear();

        for (ChunkDistributionStats cds : chunkDistribution) {
            if (cds.getChunksPerShard().size() <= 1) continue;

            String ns = cds.getNamespace();
            List<String> nsZones = collectionZones.getOrDefault(ns, Collections.emptyList());
            Map<String, List<String>> zoneGroups =
                groupShardsByZone(new ArrayList<>(cds.getChunksPerShard().keySet()), nsZones);

            for (Map.Entry<String, List<String>> zg : zoneGroups.entrySet()) {
                String zoneName = zg.getKey();
                List<String> shardsInZone = zg.getValue();
                if (shardsInZone.size() <= 1) continue;

                long total = shardsInZone.stream()
                    .mapToLong(s -> cds.getChunksPerShard().getOrDefault(s, 0L)).sum();
                if (total == 0) continue;

                String maxShard = shardsInZone.stream()
                    .max(Comparator.comparingLong(s -> cds.getChunksPerShard().getOrDefault(s, 0L)))
                    .orElse(null);
                if (maxShard == null) continue;

                long maxVal = cds.getChunksPerShard().get(maxShard);
                double maxPct = maxVal * 100.0 / total;

                if (maxPct >= WARNING_THRESHOLD) {
                    ChunkImbalanceEntry e = new ChunkImbalanceEntry();
                    e.namespace = ns;
                    e.zoneGroup = zoneName;
                    e.maxShard = maxShard;
                    e.maxPercent = maxPct;
                    e.totalChunks = total;
                    e.maxChunks = maxVal;
                    e.isCritical = maxPct >= CRITICAL_THRESHOLD;
                    e.chunksPerShard = new TreeMap<>(cds.getChunksPerShard());
                    chunkImbalances.add(e);
                }
            }
        }

        chunkImbalances.sort((a, b) -> Double.compare(b.maxPercent, a.maxPercent));
    }

    private void computeActivityImbalances() {
        activityImbalances.clear();

        long clusterTotal = getClusterTotalOps();
        if (clusterTotal == 0) return;

        for (ShardTotals st : shardTotalsMap.values()) {
            double pct = st.totalOps * 100.0 / clusterTotal;
            ActivityImbalanceEntry e = new ActivityImbalanceEntry();
            e.shardName = st.shardName;
            e.zones = st.zones;
            e.totalOps = st.totalOps;
            e.percentOfCluster = pct;
            e.isCritical = pct >= CRITICAL_THRESHOLD;
            e.isWarning = pct >= WARNING_THRESHOLD;
            activityImbalances.add(e);
        }

        activityImbalances.sort((a, b) -> Double.compare(b.totalOps, a.totalOps));
    }

    // Groups shards by zone: shards in same zone go in same group.
    // If no zones configured for this collection, all shards go in "all" group.
    private Map<String, List<String>> groupShardsByZone(List<String> shards, List<String> nsZones) {
        if (nsZones.isEmpty()) {
            return Collections.singletonMap("all", shards);
        }

        Map<String, List<String>> groups = new LinkedHashMap<>();
        List<String> unzoned = new ArrayList<>();

        for (String shard : shards) {
            List<String> sZones = shardZones.getOrDefault(shard, Collections.emptyList());
            List<String> common = sZones.stream().filter(nsZones::contains).collect(Collectors.toList());
            if (common.isEmpty()) {
                unzoned.add(shard);
            } else {
                groups.computeIfAbsent(common.get(0), k -> new ArrayList<>()).add(shard);
            }
        }

        if (!unzoned.isEmpty()) {
            groups.put("default", unzoned);
        }

        return groups;
    }

    // -------------------------------------------------------------------------
    // Inner data classes
    // -------------------------------------------------------------------------

    /** Per-shard per-namespace entry built from primary hosts. */
    public static class NamespaceShardEntry {
        public long storageSize;
        public long documentCount;
        public long bytesReadIntoCache;
        public long bytesWrittenFromCache;

        public double getStoragePercent(long total) {
            return total > 0 ? storageSize * 100.0 / total : 0;
        }
    }

    public static class ShardTotals {
        public String shardName;
        public List<String> zones = new ArrayList<>();
        public int hostCount;
        public long totalDataSize;
        public long totalStorageSize;
        public long totalIndexSize;
        public long totalDocuments;
        public long totalOps;
        // Server-level WT cache bytes (from serverStatus, all hosts)
        public long totalBytesReadIntoCache;
        public long totalBytesWrittenFromCache;
        // Collection-level WT cache bytes (from collStats, primary only)
        public long totalBytesReadIntoCacheByCollection;
        public long totalBytesWrittenFromCacheByCollection;

        ShardTotals(String shardName) { this.shardName = shardName; }

        public double getPercentOfClusterData(long clusterTotal) {
            if (clusterTotal <= 0) return 0;
            return totalStorageSize * 100.0 / clusterTotal;
        }

        public double getPercentOfClusterOps(long clusterTotal) {
            if (clusterTotal <= 0) return 0;
            return totalOps * 100.0 / clusterTotal;
        }

        public String getZonesDisplay() {
            return zones.isEmpty() ? "-" : String.join(", ", zones);
        }
    }

    public static class DataImbalanceEntry {
        public String namespace;
        public String zoneGroup;
        public String maxShard;
        public double maxPercent;
        public long totalSize;
        public long maxSize;
        public boolean isCritical;
        // Reference to the namespaceDistribution entries for this namespace
        public Map<String, NamespaceShardEntry> shardEntries;

        public String getSeverity() { return isCritical ? "CRITICAL" : "WARNING"; }
    }

    public static class ChunkImbalanceEntry {
        public String namespace;
        public String zoneGroup;
        public String maxShard;
        public double maxPercent;
        public long totalChunks;
        public long maxChunks;
        public boolean isCritical;
        public Map<String, Long> chunksPerShard = new TreeMap<>();

        public String getSeverity() { return isCritical ? "CRITICAL" : "WARNING"; }
    }

    public static class ActivityImbalanceEntry {
        public String shardName;
        public List<String> zones = new ArrayList<>();
        public long totalOps;
        public double percentOfCluster;
        public boolean isWarning;
        public boolean isCritical;

        public String getSeverity() {
            return isCritical ? "CRITICAL" : isWarning ? "WARNING" : "OK";
        }

        public String getZonesDisplay() {
            return zones.isEmpty() ? "-" : String.join(", ", zones);
        }
    }
}
