package com.mongodb.analyzer;

import com.jakewharton.fliptables.FlipTable;

import java.text.NumberFormat;
import java.util.ArrayList;
import java.util.List;
import java.util.Locale;
import java.util.Map;
import java.util.Set;

public class TableOutputFormatter implements OutputFormatter {
    
    private static final NumberFormat NUMBER_FORMAT = NumberFormat.getNumberInstance(Locale.US);
    
    @Override
    public void format(AnalysisResult result) {
        System.out.println("\n=== MongoDB Analysis Report ===");
        System.out.println("Database: " + result.getDatabaseName());
        System.out.println();
        
        if (!result.getCollectionStats().isEmpty()) {
            formatCollectionStats(result.getCollectionStats());
        }

        if (!result.getShardedCollections().isEmpty()) {
            formatShardStats(result.getShardedCollections());
        }

        if (!result.getIndexStats().isEmpty()) {
            formatIndexStats(result.getIndexStats());
        }
    }
    
    private void formatCollectionStats(List<CollectionStats> stats) {
        System.out.println("=== Collection Statistics ===");

        boolean hasWt = stats.stream().anyMatch(CollectionStats::hasWiredTigerStats);

        String[] headers = hasWt
            ? new String[]{"Collection", "Documents", "Data Size", "Storage Size",
                "Avg Doc Size", "Indexes", "Index Size", "Sharded",
                "Cache Reads", "Cache Writes"}
            : new String[]{"Collection", "Documents", "Data Size", "Storage Size",
                "Avg Doc Size", "Indexes", "Index Size", "Sharded"};

        String[][] data = new String[stats.size()][];

        for (int i = 0; i < stats.size(); i++) {
            CollectionStats stat = stats.get(i);
            String collectionName = extractCollectionName(stat.getNamespace());

            if (hasWt) {
                data[i] = new String[]{
                    collectionName,
                    NUMBER_FORMAT.format(stat.getCount()),
                    stat.getSizeFormatted(),
                    stat.getStorageSizeFormatted(),
                    String.format("%.1f B", stat.getAvgObjSize()),
                    stat.getIndexes().toString(),
                    stat.getTotalIndexSizeFormatted(),
                    stat.isSharded() ? "Yes (" + stat.getShardCount() + ")" : "No",
                    formatBytes(stat.getBytesReadIntoCache()),
                    formatBytes(stat.getBytesWrittenFromCache())
                };
            } else {
                data[i] = new String[]{
                    collectionName,
                    NUMBER_FORMAT.format(stat.getCount()),
                    stat.getSizeFormatted(),
                    stat.getStorageSizeFormatted(),
                    String.format("%.1f B", stat.getAvgObjSize()),
                    stat.getIndexes().toString(),
                    stat.getTotalIndexSizeFormatted(),
                    stat.isSharded() ? "Yes (" + stat.getShardCount() + ")" : "No"
                };
            }
        }

        System.out.println(FlipTable.of(headers, data));
        System.out.println();
        
        // Summary statistics
        long totalDocs = stats.stream().mapToLong(CollectionStats::getCount).sum();
        long totalSize = stats.stream().mapToLong(CollectionStats::getSize).sum();
        long totalStorageSize = stats.stream().mapToLong(CollectionStats::getStorageSize).sum();
        long totalIndexSize = stats.stream().mapToLong(CollectionStats::getTotalIndexSize).sum();
        int totalIndexes = stats.stream().mapToInt(CollectionStats::getIndexes).sum();

        // Sharded vs unsharded breakdown
        long shardedCount = stats.stream().filter(CollectionStats::isSharded).count();
        long unshardedCount = stats.size() - shardedCount;
        long shardedDataSize = stats.stream().filter(CollectionStats::isSharded).mapToLong(CollectionStats::getSize).sum();
        long unshardedDataSize = stats.stream().filter(c -> !c.isSharded()).mapToLong(CollectionStats::getSize).sum();

        System.out.println("Summary:");
        System.out.println("  Total Collections: " + stats.size());
        System.out.println("  Total Documents: " + NUMBER_FORMAT.format(totalDocs));
        System.out.println("  Total Data Size: " + formatBytes(totalSize));
        System.out.println("  Total Storage Size: " + formatBytes(totalStorageSize));
        System.out.println("  Total Index Size: " + formatBytes(totalIndexSize));
        System.out.println("  Total Indexes: " + totalIndexes);
        if (shardedCount > 0) {
            System.out.println();
            System.out.println("  Sharded Collections: " + shardedCount + " (" + formatBytes(shardedDataSize) + ")");
            System.out.println("  Unsharded Collections: " + unshardedCount + " (" + formatBytes(unshardedDataSize) + ")");
        }
        System.out.println();
    }

    private void formatShardStats(List<CollectionStats> shardedCollections) {
        System.out.println("=== Shard Distribution ===");

        // Build flat list of rows: one per shard
        List<String[]> rows = new ArrayList<>();
        String lastCollection = null;

        for (CollectionStats collStats : shardedCollections) {
            String collectionName = extractCollectionName(collStats.getNamespace());
            long totalDocs = collStats.getCount();
            long totalSize = collStats.getSize();

            boolean isFirstShard = true;
            for (Map.Entry<String, ShardStats> entry : collStats.getShardStats().entrySet()) {
                ShardStats shard = entry.getValue();

                // Calculate percentages
                double docsPct = totalDocs > 0 ? (shard.getCount() * 100.0 / totalDocs) : 0;
                double sizePct = totalSize > 0 ? (shard.getSize() * 100.0 / totalSize) : 0;

                // Imbalance warning for >50% on single shard (when >2 shards)
                String warning = "";
                if (collStats.getShardCount() > 2 && (docsPct > 50 || sizePct > 50)) {
                    warning = " !!";
                }

                rows.add(new String[] {
                    isFirstShard ? collectionName : "",
                    shard.getShardName(),
                    NUMBER_FORMAT.format(shard.getCount()),
                    shard.getSizeFormatted(),
                    shard.getStorageSizeFormatted(),
                    String.format("%.1f%%", docsPct),
                    String.format("%.1f%%%s", sizePct, warning)
                });

                isFirstShard = false;
            }
        }

        if (rows.isEmpty()) {
            System.out.println("No sharded collections with shard statistics found.");
            System.out.println();
            return;
        }

        String[] headers = {
            "Collection", "Shard", "Documents", "Size", "Storage", "% Docs", "% Size"
        };

        String[][] data = rows.toArray(new String[0][]);
        System.out.println(FlipTable.of(headers, data));
        System.out.println();

        // Summary
        int totalShards = shardedCollections.stream()
                .mapToInt(c -> c.getShardStats().size())
                .sum();
        long imbalancedCount = shardedCollections.stream()
                .filter(c -> {
                    if (c.getShardCount() <= 2) return false;
                    long totalDocs = c.getCount();
                    long totalSize = c.getSize();
                    for (ShardStats s : c.getShardStats().values()) {
                        double docsPct = totalDocs > 0 ? (s.getCount() * 100.0 / totalDocs) : 0;
                        double sizePct = totalSize > 0 ? (s.getSize() * 100.0 / totalSize) : 0;
                        if (docsPct > 50 || sizePct > 50) return true;
                    }
                    return false;
                })
                .count();

        System.out.println("Shard Distribution Summary:");
        System.out.println("  Sharded Collections: " + shardedCollections.size());
        System.out.println("  Total Shards: " + totalShards);
        if (imbalancedCount > 0) {
            System.out.println("  Imbalanced Collections: " + imbalancedCount + " (marked with !!)");
        }
        System.out.println();
    }

    private void formatIndexStats(List<IndexStats> stats) {
        System.out.println("=== Index Usage Statistics (Replica Set Aggregated) ===");
        
        String[] headers = {
            "Collection", "Index Name", "Index Key", "Type", "Size", "Total Ops", "Member Breakdown", "TTL", "Since"
        };
        
        String[][] data = new String[stats.size()][];
        
        for (int i = 0; i < stats.size(); i++) {
            IndexStats stat = stats.get(i);
            String collectionName = extractCollectionName(stat.getNamespace());
            String indexKey = formatIndexKey(stat.getIndexKey());
            
            // Show total operations across all members
            Long totalOps = stat.getTotalOpsAcrossMembers();
            String opsDisplay = totalOps > 0 ? NUMBER_FORMAT.format(totalOps) : NUMBER_FORMAT.format(stat.getOps());
            
            // Show member breakdown if available
            String memberBreakdown = stat.getMemberOpsBreakdown();
            if (memberBreakdown.length() > 50) {
                memberBreakdown = memberBreakdown.substring(0, 47) + "...";
            }
            
            data[i] = new String[] {
                collectionName,
                stat.getIndexName(),
                indexKey,
                stat.getIndexType(),
                stat.getIndexSizeFormatted(),
                opsDisplay,
                memberBreakdown,
                stat.getTtlDescription(),
                stat.getSince() != null ? stat.getSince().toString().substring(0, 19) : "N/A"
            };
        }
        
        System.out.println(FlipTable.of(headers, data));
        System.out.println();
        
        // Enhanced index analysis
        long totalOps = stats.stream().mapToLong(s -> s.getTotalOpsAcrossMembers() > 0 ? s.getTotalOpsAcrossMembers() : s.getOps()).sum();
        long totalIndexSize = stats.stream().mapToLong(IndexStats::getIndexSize).sum();
        long unusedIndexes = stats.stream().mapToLong(s -> (s.getTotalOpsAcrossMembers() == 0 && s.getOps() == 0) ? 1 : 0).sum();
        long ttlIndexes = stats.stream().mapToLong(s -> s.isTtl() ? 1 : 0).sum();
        long uniqueIndexes = stats.stream().mapToLong(s -> s.isUnique() ? 1 : 0).sum();
        
        System.out.println("Index Usage Summary:");
        System.out.println("  Total Indexes: " + stats.size());
        System.out.println("  Total Index Size: " + formatBytes(totalIndexSize));
        System.out.println("  Total Index Operations: " + NUMBER_FORMAT.format(totalOps));
        System.out.println("  Unused Indexes: " + unusedIndexes);
        System.out.println("  TTL Indexes: " + ttlIndexes);
        System.out.println("  Unique Indexes: " + uniqueIndexes);
        if (unusedIndexes > 0) {
            System.out.println("  ⚠️  Consider reviewing unused indexes for potential removal");
        }
        System.out.println();
    }
    
    private String extractCollectionName(String namespace) {
        if (namespace == null) return "unknown";
        int dotIndex = namespace.lastIndexOf('.');
        return dotIndex >= 0 ? namespace.substring(dotIndex + 1) : namespace;
    }
    
    private String formatIndexKey(String indexKey) {
        if (indexKey == null || indexKey.isEmpty()) return "{}";
        
        // Simplify the JSON representation for table display
        String simplified = indexKey
            .replaceAll("\\{\"", "")
            .replaceAll("\":", ":")
            .replaceAll("\"}", "")
            .replaceAll("\",\"", ", ");
            
        // Truncate if too long for table display
        if (simplified.length() > 40) {
            simplified = simplified.substring(0, 37) + "...";
        }
        
        return simplified;
    }
    
    private String formatBytes(long bytes) {
        if (bytes < 1024) return bytes + " B";
        if (bytes < 1024 * 1024) return String.format("%.1f KB", bytes / 1024.0);
        if (bytes < 1024 * 1024 * 1024) return String.format("%.1f MB", bytes / (1024.0 * 1024));
        return String.format("%.1f GB", bytes / (1024.0 * 1024 * 1024));
    }
    
    @Override
    public void formatMultiple(List<AnalysisResult> results) {
        System.out.println("\n=== MongoDB Multi-Database Analysis Report ===");
        System.out.println("Analyzed " + results.size() + " databases");
        System.out.println();
        
        // Combine all collection stats
        List<CollectionStats> allCollectionStats = new ArrayList<>();
        List<IndexStats> allIndexStats = new ArrayList<>();
        List<CollectionStats> allShardedCollections = new ArrayList<>();

        for (AnalysisResult result : results) {
            allCollectionStats.addAll(result.getCollectionStats());
            allIndexStats.addAll(result.getIndexStats());
            allShardedCollections.addAll(result.getShardedCollections());
        }

        if (!allCollectionStats.isEmpty()) {
            formatCollectionStats(allCollectionStats);
        }

        if (!allShardedCollections.isEmpty()) {
            formatShardStats(allShardedCollections);
        }

        if (!allIndexStats.isEmpty()) {
            formatIndexStats(allIndexStats);
        }

        // Shard-level summary (if sharded cluster)
        boolean hasShardInfo = results.stream().anyMatch(r -> r.getPrimaryShard() != null);
        if (hasShardInfo) {
            formatShardSummary(results);
        }

        // Per-shard database breakdown (if sharded cluster with raw stats)
        boolean hasShardDbStats = results.stream()
            .anyMatch(r -> r.getDatabaseStats() != null && r.getDatabaseStats().hasShardStats());
        if (hasShardDbStats) {
            formatShardDatabaseBreakdown(results);
        }

        // Database-level summary
        formatDatabaseSummary(results);
    }

    private void formatShardSummary(List<AnalysisResult> results) {
        System.out.println("=== Shard Summary ===");

        // Aggregate per-shard totals from db.stats().raw
        Map<String, long[]> shardTotals = new java.util.TreeMap<>();
        // Array: [dataSize, storageSize, indexSize]

        for (AnalysisResult result : results) {
            DatabaseStats dbStats = result.getDatabaseStats();
            if (dbStats != null && dbStats.hasShardStats()) {
                for (Map.Entry<String, DatabaseStats.ShardDbStats> entry : dbStats.getShardStats().entrySet()) {
                    String shardName = entry.getKey();
                    DatabaseStats.ShardDbStats stats = entry.getValue();
                    long[] totals = shardTotals.computeIfAbsent(shardName, k -> new long[3]);
                    totals[0] += stats.getDataSize();
                    totals[1] += stats.getStorageSize();
                    totals[2] += stats.getIndexSize();
                }
            }
        }

        if (shardTotals.isEmpty()) {
            System.out.println("No shard information available.");
            System.out.println();
            return;
        }

        String[] headers = {
            "Shard", "Data Size", "Storage Size", "Index Size", "Total Size"
        };

        String[][] data = new String[shardTotals.size()][];
        int i = 0;
        for (Map.Entry<String, long[]> entry : shardTotals.entrySet()) {
            long[] totals = entry.getValue();
            long totalSize = totals[1] + totals[2]; // storage + index

            data[i++] = new String[] {
                entry.getKey(),
                formatBytes(totals[0]),
                formatBytes(totals[1]),
                formatBytes(totals[2]),
                formatBytes(totalSize)
            };
        }

        System.out.println(FlipTable.of(headers, data));
        System.out.println();
    }

    private void formatDatabaseSummary(List<AnalysisResult> results) {
        System.out.println("=== Database Summary ===");

        // Check if any database has primary shard info
        boolean hasShardInfo = results.stream().anyMatch(r -> r.getPrimaryShard() != null);

        String[] headers;
        if (hasShardInfo) {
            headers = new String[] {
                "Database", "Primary Shard", "Collections",
                "Data Size", "Storage Size", "Index Size", "Indexes"
            };
        } else {
            headers = new String[] {
                "Database", "Collections", "Documents", "Data Size",
                "Storage Size", "Index Size", "Indexes"
            };
        }

        String[][] data = new String[results.size()][];

        for (int i = 0; i < results.size(); i++) {
            AnalysisResult result = results.get(i);
            DatabaseStats dbStats = result.getDatabaseStats();

            if (dbStats != null) {
                if (hasShardInfo) {
                    data[i] = new String[] {
                        result.getDatabaseName(),
                        result.getPrimaryShard() != null ? result.getPrimaryShard() : "-",
                        String.valueOf(dbStats.getCollections()),
                        formatBytes(dbStats.getDataSize()),
                        formatBytes(dbStats.getStorageSize()),
                        formatBytes(dbStats.getIndexSize()),
                        String.valueOf(dbStats.getIndexes())
                    };
                } else {
                    data[i] = new String[] {
                        result.getDatabaseName(),
                        String.valueOf(dbStats.getCollections()),
                        NUMBER_FORMAT.format(dbStats.getObjects()),
                        formatBytes(dbStats.getDataSize()),
                        formatBytes(dbStats.getStorageSize()),
                        formatBytes(dbStats.getIndexSize()),
                        String.valueOf(dbStats.getIndexes())
                    };
                }
            } else {
                // Fallback to collection stats if db.stats() not available
                List<CollectionStats> stats = result.getCollectionStats();
                long totalDocs = stats.stream().mapToLong(CollectionStats::getCount).sum();
                long totalSize = stats.stream().mapToLong(CollectionStats::getSize).sum();
                long totalStorageSize = stats.stream().mapToLong(CollectionStats::getStorageSize).sum();
                long totalIndexSize = stats.stream().mapToLong(CollectionStats::getTotalIndexSize).sum();
                int totalIndexes = stats.stream().mapToInt(CollectionStats::getIndexes).sum();

                if (hasShardInfo) {
                    data[i] = new String[] {
                        result.getDatabaseName(),
                        result.getPrimaryShard() != null ? result.getPrimaryShard() : "-",
                        String.valueOf(stats.size()),
                        formatBytes(totalSize),
                        formatBytes(totalStorageSize),
                        formatBytes(totalIndexSize),
                        String.valueOf(totalIndexes)
                    };
                } else {
                    data[i] = new String[] {
                        result.getDatabaseName(),
                        String.valueOf(stats.size()),
                        NUMBER_FORMAT.format(totalDocs),
                        formatBytes(totalSize),
                        formatBytes(totalStorageSize),
                        formatBytes(totalIndexSize),
                        String.valueOf(totalIndexes)
                    };
                }
            }
        }

        System.out.println(FlipTable.of(headers, data));
        System.out.println();
    }

    @Override
    public void formatShardedCluster(ClusterAnalysisReport report) {
        formatClusterExecutiveSummary(report);
        formatCacheStats(report);
        formatShardTotals(report);
        if (!report.getChunkDistribution().isEmpty()) {
            formatChunkDistribution(report);
        }
        formatNamespaceDistribution(report);
        formatActivityImbalances(report);
        formatPerHostDetail(report);
    }

    private void formatClusterExecutiveSummary(ClusterAnalysisReport report) {
        System.out.println("\n╔══════════════════════════════════════════════════════════════════╗");
        System.out.println("  MongoDB Sharded Cluster Analysis — Executive Summary");
        System.out.println("╚══════════════════════════════════════════════════════════════════╝");
        System.out.println();

        long totalPrimaries = report.getTotalPrimaryHosts();
        long totalSecondaries = report.getTotalHosts() - totalPrimaries;
        System.out.printf("  Shards: %d  |  Hosts: %d (%d primary, %d secondary)%n",
            report.getTotalShards(), report.getTotalHosts(), totalPrimaries, totalSecondaries);
        System.out.printf("  Total Data: %s  |  Total Storage: %s%n",
            formatBytes(report.getClusterTotalDataSize()),
            formatBytes(report.getClusterTotalStorageSize()));
        System.out.printf("  Total Cluster Ops (all hosts): %s%n",
            NUMBER_FORMAT.format(report.getClusterTotalOps()));
        if (!report.getChunkDistribution().isEmpty()) {
            System.out.printf("  Total Chunks: %s%n", NUMBER_FORMAT.format(report.getTotalChunks()));
        }
        if (report.hasZones()) {
            System.out.printf("  Zones: %s%n", String.join(", ", report.getZoneNames()));
        }

        System.out.println();
        System.out.println("  Issues:");
        boolean anyIssue = false;
        long criticalData = report.getDataImbalances().stream().filter(e -> e.isCritical).count();
        long warnData = report.getDataImbalances().size() - criticalData;
        if (criticalData > 0) {
            System.out.println("  [CRITICAL] " + criticalData + " collection(s) with severe data imbalance (>75% on one shard)");
            anyIssue = true;
        }
        if (warnData > 0) {
            System.out.println("  [WARNING]  " + warnData + " collection(s) with data imbalance (>60% on one shard)");
            anyIssue = true;
        }

        long criticalChunks = report.getChunkImbalances().stream().filter(e -> e.isCritical).count();
        long warnChunks = report.getChunkImbalances().size() - criticalChunks;
        if (criticalChunks > 0) {
            System.out.println("  [CRITICAL] " + criticalChunks + " collection(s) with severe chunk imbalance");
            anyIssue = true;
        }
        if (warnChunks > 0) {
            System.out.println("  [WARNING]  " + warnChunks + " collection(s) with chunk imbalance");
            anyIssue = true;
        }

        long criticalActivity = report.getActivityImbalances().stream().filter(e -> e.isCritical).count();
        long warnActivity = report.getActivityImbalances().stream().filter(e -> e.isWarning && !e.isCritical).count();
        if (criticalActivity > 0) {
            System.out.println("  [CRITICAL] " + criticalActivity + " shard(s) handling >75% of cluster operations");
            anyIssue = true;
        }
        if (warnActivity > 0) {
            System.out.println("  [WARNING]  " + warnActivity + " shard(s) handling >60% of cluster operations");
            anyIssue = true;
        }

        long jumboCollections = report.getChunkDistribution().stream().filter(ChunkDistributionStats::isHasJumboChunks).count();
        if (jumboCollections > 0) {
            System.out.println("  [WARNING]  " + jumboCollections + " collection(s) have jumbo chunks");
            anyIssue = true;
        }

        if (!anyIssue) {
            System.out.println("  [OK] No significant imbalances detected");
        }
        System.out.println();
    }

    private void formatCacheStats(ClusterAnalysisReport report) {
        System.out.println("=== WiredTiger Cache Statistics (per host) ===");

        List<HostAnalysisResult> hosts = report.getHostResults();
        String[] headers = {"Shard", "Host", "Role", "Bytes Read into Cache",
            "Bytes Written from Cache", "Cache Used", "Cache Max"};
        String[][] data = new String[hosts.size()][];

        for (int i = 0; i < hosts.size(); i++) {
            HostAnalysisResult h = hosts.get(i);
            WiredTigerStats wt = h.getWiredTigerStats();
            if (wt == null) wt = new WiredTigerStats();
            data[i] = new String[]{
                h.getShardName(), h.getHostName(), h.getRole(),
                formatBytes(wt.getBytesReadIntoCache()),
                formatBytes(wt.getBytesWrittenFromCache()),
                String.format("%.1f%%", wt.getCacheUsedPercent()),
                formatBytes(wt.getMaxBytesConfigured())
            };
        }

        System.out.println(FlipTable.of(headers, data));
        System.out.println();
    }

    private void formatShardTotals(ClusterAnalysisReport report) {
        System.out.println("=== Shard Totals (aggregated across all hosts per shard) ===");

        long clusterStorage = report.getClusterTotalStorageSize();
        long clusterOps = report.getClusterTotalOps();

        List<ClusterAnalysisReport.ShardTotals> shards = new ArrayList<>(report.getShardTotalsMap().values());
        String[] headers = {"Shard", "Zone(s)", "Hosts", "Data Size", "Storage Size",
            "Index Size", "Documents", "Total Ops", "% Ops",
            "Cache Reads", "Cache Writes"};
        String[][] data = new String[shards.size()][];

        for (int i = 0; i < shards.size(); i++) {
            ClusterAnalysisReport.ShardTotals st = shards.get(i);
            data[i] = new String[]{
                st.shardName,
                st.getZonesDisplay(),
                String.valueOf(st.hostCount),
                formatBytes(st.totalDataSize),
                formatBytes(st.totalStorageSize),
                formatBytes(st.totalIndexSize),
                NUMBER_FORMAT.format(st.totalDocuments),
                NUMBER_FORMAT.format(st.totalOps),
                String.format("%.1f%%", st.getPercentOfClusterOps(clusterOps)),
                formatBytes(st.totalBytesReadIntoCache),
                formatBytes(st.totalBytesWrittenFromCache)
            };
        }

        System.out.println(FlipTable.of(headers, data));
        System.out.println();
    }

    private void formatChunkDistribution(ClusterAnalysisReport report) {
        System.out.println("=== Chunk Distribution ===");

        List<ChunkDistributionStats> chunks = report.getChunkDistribution();
        // One row per (namespace, shard) pair
        List<String[]> rows = new ArrayList<>();
        for (ChunkDistributionStats cds : chunks) {
            boolean first = true;
            for (Map.Entry<String, Long> e : cds.getChunksPerShard().entrySet()) {
                double pct = cds.getShardPercent(e.getKey());
                String status = "";
                if (pct >= 75) status = "CRITICAL";
                else if (pct >= 60) status = "WARNING";

                // Show zone for this shard
                List<String> zones = report.getShardZones().getOrDefault(e.getKey(), java.util.Collections.emptyList());
                String zoneDisplay = zones.isEmpty() ? "-" : String.join(",", zones);

                rows.add(new String[]{
                    first ? cds.getNamespace() : "",
                    e.getKey(),
                    zoneDisplay,
                    first ? String.valueOf(cds.getTotalChunks()) : "",
                    NUMBER_FORMAT.format(e.getValue()),
                    String.format("%.1f%%", pct),
                    first && cds.isHasJumboChunks() ? "YES" : (first ? "no" : ""),
                    status
                });
                first = false;
            }
        }

        if (rows.isEmpty()) {
            System.out.println("No chunk data available.");
        } else {
            String[] headers = {"Namespace", "Shard", "Zone", "Total Chunks", "Shard Chunks", "% Chunks", "Jumbo", "Status"};
            System.out.println(FlipTable.of(headers, rows.toArray(new String[0][])));
        }
        System.out.println();
    }

    private static final int CONSOLE_NS_LIMIT = 100;

    private void formatNamespaceDistribution(ClusterAnalysisReport report) {
        Map<String, Map<String, ClusterAnalysisReport.NamespaceShardEntry>> dist =
            report.getNamespaceDistribution();

        if (dist.isEmpty()) {
            return;
        }

        // Build a sortable list: namespace, total storage, max shard %, max shard name, zone, status
        // Sort: imbalanced first (by maxPct desc), then by total storage desc
        List<String> shardNames = new ArrayList<>(report.getShardTotalsMap().keySet());

        // Compute summary per namespace
        List<Object[]> rows = new ArrayList<>();
        Set<String> imbalancedNs = new java.util.HashSet<>();
        for (ClusterAnalysisReport.DataImbalanceEntry e : report.getDataImbalances()) {
            imbalancedNs.add(e.namespace);
        }

        for (Map.Entry<String, Map<String, ClusterAnalysisReport.NamespaceShardEntry>> entry : dist.entrySet()) {
            String ns = entry.getKey();
            Map<String, ClusterAnalysisReport.NamespaceShardEntry> shardData = entry.getValue();
            long total = shardData.values().stream().mapToLong(e -> e.storageSize).sum();
            long totalDocs = shardData.values().stream().mapToLong(e -> e.documentCount).sum();
            long maxSize = shardData.values().stream().mapToLong(e -> e.storageSize).max().orElse(0);
            double maxPct = total > 0 ? maxSize * 100.0 / total : 0;
            String maxShard = shardData.entrySet().stream()
                .max(java.util.Comparator.comparingLong(e -> e.getValue().storageSize))
                .map(Map.Entry::getKey).orElse("-");
            List<String> nsZones = report.getCollectionZones().getOrDefault(ns, java.util.Collections.emptyList());
            String zones = nsZones.isEmpty() ? "-" : String.join(",", nsZones);
            boolean isImbalanced = imbalancedNs.contains(ns);
            rows.add(new Object[]{ns, total, totalDocs, maxPct, maxShard, zones, isImbalanced});
        }

        // Sort: imbalanced first, then by total storage desc
        rows.sort((a, b) -> {
            boolean aImb = (Boolean) a[6];
            boolean bImb = (Boolean) b[6];
            if (aImb != bImb) return bImb ? 1 : -1;
            return Long.compare((Long) b[1], (Long) a[1]);
        });

        int total = rows.size();
        List<Object[]> display = rows.size() > CONSOLE_NS_LIMIT ? rows.subList(0, CONSOLE_NS_LIMIT) : rows;

        System.out.println("=== Namespace Shard Distribution (" + total + " namespaces"
            + (total > CONSOLE_NS_LIMIT ? ", showing top " + CONSOLE_NS_LIMIT + " — imbalanced first, then by size" : "")
            + ") ===");

        // Build shardName header columns
        String[] shardCols = shardNames.toArray(new String[0]);
        String[] headers = new String[5 + shardCols.length];
        headers[0] = "Namespace";
        headers[1] = "Zone";
        headers[2] = "Total Storage";
        headers[3] = "Documents";
        for (int i = 0; i < shardCols.length; i++) {
            headers[4 + i] = shardCols[i] + " %";
        }
        headers[4 + shardCols.length] = "Status";

        String[][] tableData = new String[display.size()][];
        for (int i = 0; i < display.size(); i++) {
            Object[] r = display.get(i);
            String ns = (String) r[0];
            long totalStorage = (Long) r[1];
            long totalDocs2 = (Long) r[2];
            Map<String, ClusterAnalysisReport.NamespaceShardEntry> shardData = dist.get(ns);
            boolean isImb = (Boolean) r[6];

            String[] row = new String[5 + shardCols.length];
            row[0] = ns;
            row[1] = (String) r[5];
            row[2] = formatBytes(totalStorage);
            row[3] = NUMBER_FORMAT.format(totalDocs2);
            for (int j = 0; j < shardCols.length; j++) {
                ClusterAnalysisReport.NamespaceShardEntry e = shardData.get(shardCols[j]);
                row[4 + j] = e != null ? String.format("%.1f%%", e.getStoragePercent(totalStorage)) : "—";
            }
            row[4 + shardCols.length] = isImb ? "IMBALANCED" : "ok";
            tableData[i] = row;
        }

        System.out.println(FlipTable.of(headers, tableData));

        long imbalancedCount = report.getDataImbalances().size();
        if (imbalancedCount > 0) {
            System.out.printf("  Imbalanced namespaces: %d of %d%n", imbalancedCount, total);
        }
        System.out.println();
    }

    private void formatActivityImbalances(ClusterAnalysisReport report) {
        System.out.println("=== Shard Activity (total ops across all members per shard) ===");

        List<ClusterAnalysisReport.ActivityImbalanceEntry> entries = report.getActivityImbalances();
        String[] headers = {"Shard", "Zone(s)", "Total Ops (all hosts)", "% of Cluster", "Status"};
        String[][] data = new String[entries.size()][];

        for (int i = 0; i < entries.size(); i++) {
            ClusterAnalysisReport.ActivityImbalanceEntry e = entries.get(i);
            data[i] = new String[]{
                e.shardName,
                e.getZonesDisplay(),
                NUMBER_FORMAT.format(e.totalOps),
                String.format("%.1f%%", e.percentOfCluster),
                e.getSeverity()
            };
        }

        System.out.println(FlipTable.of(headers, data));
        System.out.println();
    }

    private void formatPerHostDetail(ClusterAnalysisReport report) {
        System.out.println("=== Per-Host Collection & Index Detail ===");
        System.out.println();

        String currentShard = null;

        for (HostAnalysisResult hostResult : report.getHostResults()) {
            if (!hostResult.getShardName().equals(currentShard)) {
                currentShard = hostResult.getShardName();
                System.out.println("┌─ Shard: " + currentShard);
            }

            System.out.println("│  Host: " + hostResult.getHostName() + " (" + hostResult.getRole() + ")");

            for (AnalysisResult dbResult : hostResult.getDatabaseResults()) {
                List<CollectionStats> collStats = dbResult.getCollectionStats();
                List<IndexStats> idxStats = dbResult.getIndexStats();
                if (collStats.isEmpty() && idxStats.isEmpty()) continue;

                System.out.println("│    Database: " + dbResult.getDatabaseName());

                if (dbResult.getDatabaseStats() != null) {
                    DatabaseStats ds = dbResult.getDatabaseStats();
                    System.out.printf("│      objects: %s  dataSize: %s  storageSize: %s  indexSize: %s%n",
                        NUMBER_FORMAT.format(ds.getObjects()),
                        formatBytes(ds.getDataSize()),
                        formatBytes(ds.getStorageSize()),
                        formatBytes(ds.getIndexSize()));
                }
                System.out.println();

                if (!collStats.isEmpty()) {
                    formatCollectionStats(collStats);
                }
                if (!idxStats.isEmpty()) {
                    formatIndexStats(idxStats);
                }
            }
            System.out.println();
        }
    }

    private void formatShardDatabaseBreakdown(List<AnalysisResult> results) {
        System.out.println("=== Per-Shard Database Breakdown ===");

        // Collect all shard names
        java.util.Set<String> allShards = new java.util.TreeSet<>();
        for (AnalysisResult result : results) {
            DatabaseStats dbStats = result.getDatabaseStats();
            if (dbStats != null && dbStats.hasShardStats()) {
                allShards.addAll(dbStats.getShardStats().keySet());
            }
        }

        if (allShards.isEmpty()) {
            System.out.println("No per-shard breakdown available.");
            System.out.println();
            return;
        }

        // Build flat table: Database | Shard | Data Size | Storage Size | Index Size
        List<String[]> rows = new ArrayList<>();
        for (AnalysisResult result : results) {
            DatabaseStats dbStats = result.getDatabaseStats();
            if (dbStats != null && dbStats.hasShardStats()) {
                boolean isFirst = true;
                for (Map.Entry<String, DatabaseStats.ShardDbStats> entry : dbStats.getShardStats().entrySet()) {
                    DatabaseStats.ShardDbStats shardStats = entry.getValue();
                    rows.add(new String[] {
                        isFirst ? result.getDatabaseName() : "",
                        entry.getKey(),
                        formatBytes(shardStats.getDataSize()),
                        formatBytes(shardStats.getStorageSize()),
                        formatBytes(shardStats.getIndexSize())
                    });
                    isFirst = false;
                }
            }
        }

        if (rows.isEmpty()) {
            System.out.println("No per-shard breakdown available.");
            System.out.println();
            return;
        }

        String[] headers = { "Database", "Shard", "Data Size", "Storage Size", "Index Size" };
        String[][] data = rows.toArray(new String[0][]);
        System.out.println(FlipTable.of(headers, data));
        System.out.println();
    }
}