package com.mongodb.analyzer;

import com.mongodb.client.MongoClient;
import com.mongodb.client.MongoClients;
import com.mongodb.client.MongoCollection;
import com.mongodb.client.MongoDatabase;
import com.mongodb.client.MongoIterable;
import org.bson.Document;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import picocli.CommandLine;
import picocli.CommandLine.Command;
import picocli.CommandLine.Option;
import picocli.CommandLine.Parameters;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.HashSet;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.Callable;

@Command(name = "mongo-analyzer", mixinStandardHelpOptions = true, version = "1.0",
        description = "Analyzes MongoDB collection statistics and index usage")
public class MongoAnalyzer implements Callable<Integer> {

    private static final Logger logger = LoggerFactory.getLogger(MongoAnalyzer.class);

    @Option(names = {"-c", "--connection"}, description = "MongoDB connection string", 
            defaultValue = "mongodb://localhost:27017")
    private String connectionString;

    @Option(names = {"-d", "--database"}, description = "Database name to analyze (if not specified, analyzes all databases)")
    private String databaseName;

    @Option(names = {"-o", "--output"}, description = "Output format (table, json, html)", 
            defaultValue = "table")
    private String outputFormat;

    @Option(names = {"--include-collections"}, description = "Comma-separated list of collections to include")
    private String includeCollections;

    @Option(names = {"--exclude-collections"}, description = "Comma-separated list of collections to exclude")
    private String excludeCollections;

    @Option(names = {"--exclude-databases"}, description = "Comma-separated list of databases to exclude")
    private String excludeDatabases;

    @Option(names = {"--stats-only"}, description = "Only show collection statistics (no index stats)")
    private boolean statsOnly;

    @Option(names = {"--index-only"}, description = "Only show index statistics (no collection stats)")
    private boolean indexOnly;

    // Set of sharded collection namespaces (queried from config.collections)
    private Set<String> shardedCollectionNamespaces = new HashSet<>();

    public static void main(String[] args) {
        int exitCode = new CommandLine(new MongoAnalyzer()).execute(args);
        System.exit(exitCode);
    }

    @Override
    public Integer call() throws Exception {
        try {
            logger.info("Starting MongoDB analysis...");
            logger.info("Connection string: {}", connectionString);
            
            try (MongoClient mongoClient = MongoClients.create(connectionString)) {
                if (ShardedClusterClient.isMongos(mongoClient)) {
                    logger.info("Detected mongos — running per-host sharded cluster analysis");
                    analyzeShardedClusterPerHost(mongoClient);
                } else if (databaseName != null) {
                    analyzeDatabase(mongoClient, databaseName);
                } else {
                    analyzeAllDatabases(mongoClient);
                }
            }
            
            return 0;
        } catch (Exception e) {
            logger.error("Error during analysis", e);
            return 1;
        }
    }

    private void analyzeShardedClusterPerHost(MongoClient mongosClient) {
        ShardedClusterClient clusterClient = new ShardedClusterClient(mongosClient, connectionString);

        List<ShardInfo> shards = clusterClient.getShards();
        if (shards.isEmpty()) {
            logger.error("No shards found in config.shards");
            System.err.println("Error: No shards found in config.shards");
            return;
        }
        logger.info("Found {} shards", shards.size());

        // Gather cluster-wide metadata from mongos before connecting to individual hosts
        logger.info("Reading chunk distribution...");
        List<ChunkDistributionStats> chunkDistribution = clusterClient.getChunkDistribution();
        logger.info("Reading zone assignments...");
        Map<String, List<String>> collectionZones = clusterClient.getCollectionZoneRanges();

        Set<String> excludeDbSet = new HashSet<>();
        if (excludeDatabases != null && !excludeDatabases.trim().isEmpty()) {
            for (String db : excludeDatabases.split(",")) {
                excludeDbSet.add(db.trim());
            }
        }

        List<HostAnalysisResult> allHostResults = new ArrayList<>();

        for (ShardInfo shard : shards) {
            List<String> hosts = shard.getHosts();
            logger.info("Shard {}: {} hosts", shard.getShardName(), hosts.size());

            for (String host : hosts) {
                logger.info("Connecting directly to host: {}", host);
                MongoClient directClient = null;
                try {
                    directClient = clusterClient.createDirectConnection(host);
                    String role = clusterClient.getHostRole(directClient);
                    logger.info("Host {} role: {}", host, role);

                    HostAnalysisResult hostResult = new HostAnalysisResult();
                    hostResult.setShardName(shard.getShardName());
                    hostResult.setHostName(host);
                    hostResult.setRole(role);

                    // Gather WiredTiger cache stats
                    hostResult.setWiredTigerStats(clusterClient.getWiredTigerStats(directClient));

                    List<String> databases = new ArrayList<>();
                    for (String dbName : directClient.listDatabaseNames()) {
                        if (!excludeDbSet.contains(dbName)) {
                            if (databaseName == null || databaseName.equals(dbName)) {
                                databases.add(dbName);
                            }
                        }
                    }

                    logger.debug("Analyzing {} databases on host {}", databases.size(), host);

                    final MongoClient client = directClient;
                    for (String dbName : databases) {
                        try {
                            MongoDatabase db = client.getDatabase(dbName);
                            DatabaseAnalyzer analyzer = new DatabaseAnalyzer(db);

                            AnalysisResult dbResult = analyzer.analyzeDirectConnection(
                                includeCollections, excludeCollections, !statsOnly);
                            dbResult.setDatabaseStats(analyzer.getDatabaseStats());

                            hostResult.addDatabaseResult(dbResult);
                        } catch (Exception e) {
                            logger.warn("Failed to analyze database {} on host {}: {}", dbName, host, e.getMessage());
                        }
                    }

                    allHostResults.add(hostResult);
                } catch (Exception e) {
                    logger.warn("Failed to connect to host {}: {}", host, e.getMessage());
                } finally {
                    if (directClient != null) {
                        directClient.close();
                    }
                }
            }
        }

        // Build the cluster report and compute derived metrics
        ClusterAnalysisReport report = new ClusterAnalysisReport();
        report.setHostResults(allHostResults);
        report.setChunkDistribution(chunkDistribution);
        report.setShardZones(clusterClient.buildShardZonesMap(shards));
        report.setCollectionZones(collectionZones);
        report.compute();

        switch (outputFormat.toLowerCase()) {
            case "json":
                new JsonOutputFormatter().formatShardedCluster(report);
                break;
            case "html":
                new HtmlOutputFormatter().formatShardedCluster(report);
                break;
            case "table":
            default:
                new TableOutputFormatter().formatShardedCluster(report);
                break;
        }
    }

    private void analyzeAllDatabases(MongoClient mongoClient) {
        logger.info("Analyzing all databases...");

        List<String> databasesToAnalyze = getFilteredDatabases(mongoClient);

        if (databasesToAnalyze.isEmpty()) {
            System.out.println("No databases found to analyze after applying filters.");
            return;
        }

        logger.info("Found {} databases to analyze: {}", databasesToAnalyze.size(), databasesToAnalyze);

        // Get primary shard mapping from config.databases
        Map<String, String> primaryShardMap = getPrimaryShardMapping(mongoClient);

        // Load sharded collection namespaces from config.collections
        loadShardedCollectionNamespaces(mongoClient);

        // Get bulk collection stats using $_internalAllCollectionStats (single call for all databases)
        Map<String, List<CollectionStats>> bulkStats = getAllCollectionStats(mongoClient, null);

        List<AnalysisResult> allResults = new ArrayList<>();

        for (String dbName : databasesToAnalyze) {
            try {
                logger.debug("Analyzing database: {}", dbName);
                MongoDatabase database = mongoClient.getDatabase(dbName);
                DatabaseAnalyzer analyzer = new DatabaseAnalyzer(database);

                // Get collection stats from bulk results
                List<CollectionStats> dbCollectionStats = bulkStats.getOrDefault(dbName, new ArrayList<>());
                dbCollectionStats = filterCollectionStats(dbCollectionStats);

                AnalysisResult result = analyzer.analyze(
                    includeCollections,
                    excludeCollections,
                    false,  // Don't get collection stats the old way
                    !statsOnly   // includeIndexStats
                );

                // Add the bulk-gathered collection stats
                if (!indexOnly) {
                    for (CollectionStats stats : dbCollectionStats) {
                        result.addCollectionStats(stats);
                    }
                }

                // Get database-level stats using db.stats()
                result.setDatabaseStats(analyzer.getDatabaseStats());

                // Set primary shard if available
                result.setPrimaryShard(primaryShardMap.get(dbName));

                allResults.add(result);

            } catch (Exception e) {
                logger.warn("Failed to analyze database {}: {}", dbName, e.getMessage());
            }
        }

        // Format combined results
        formatCombinedResults(allResults);
    }
    
    private List<String> getFilteredDatabases(MongoClient mongoClient) {
        MongoIterable<String> allDatabases = mongoClient.listDatabaseNames();
        List<String> databases = new ArrayList<>();

        // User-specified excludes only (no default excludes)
        Set<String> excludes = new HashSet<>();
        if (excludeDatabases != null && !excludeDatabases.trim().isEmpty()) {
            String[] userExcludes = excludeDatabases.split(",");
            for (String exclude : userExcludes) {
                excludes.add(exclude.trim());
            }
        }

        for (String dbName : allDatabases) {
            if (!excludes.contains(dbName)) {
                databases.add(dbName);
            }
        }

        return databases;
    }

    private Map<String, String> getPrimaryShardMapping(MongoClient mongoClient) {
        Map<String, String> mapping = new HashMap<>();
        try {
            MongoDatabase configDb = mongoClient.getDatabase("config");
            MongoCollection<Document> databases = configDb.getCollection("databases");

            for (Document doc : databases.find()) {
                String dbName = doc.getString("_id");
                String primaryShard = doc.getString("primary");
                if (dbName != null && primaryShard != null) {
                    mapping.put(dbName, primaryShard);
                }
            }
            logger.debug("Found primary shard mapping for {} databases", mapping.size());
        } catch (Exception e) {
            logger.debug("Could not read config.databases (not a sharded cluster?): {}", e.getMessage());
        }
        return mapping;
    }

    /**
     * Query config.collections to get the set of sharded collection namespaces.
     * Only actually sharded collections appear in config.collections.
     * The _id field contains the full namespace (db.collection).
     */
    private void loadShardedCollectionNamespaces(MongoClient mongoClient) {
        shardedCollectionNamespaces.clear();
        try {
            MongoDatabase configDb = mongoClient.getDatabase("config");
            MongoCollection<Document> collections = configDb.getCollection("collections");

            for (Document doc : collections.find()) {
                String namespace = doc.getString("_id");
                if (namespace != null) {
                    shardedCollectionNamespaces.add(namespace);
                }
            }
            logger.debug("Found {} sharded collections in config.collections", shardedCollectionNamespaces.size());
        } catch (Exception e) {
            logger.debug("Could not read config.collections (not a sharded cluster?): {}", e.getMessage());
        }
    }

    private void formatCombinedResults(List<AnalysisResult> results) {
        switch (outputFormat.toLowerCase()) {
            case "json":
                new JsonOutputFormatter().formatMultiple(results);
                break;
            case "html":
                new HtmlOutputFormatter().formatMultiple(results);
                break;
            case "table":
            default:
                new TableOutputFormatter().formatMultiple(results);
                break;
        }
    }

    private void analyzeDatabase(MongoClient mongoClient, String dbName) {
        logger.info("Analyzing database: {}", dbName);

        MongoDatabase database = mongoClient.getDatabase(dbName);
        DatabaseAnalyzer analyzer = new DatabaseAnalyzer(database);

        // Load sharded collection namespaces from config.collections
        loadShardedCollectionNamespaces(mongoClient);

        // Get bulk collection stats using $_internalAllCollectionStats (filtered to this database)
        Map<String, List<CollectionStats>> bulkStats = getAllCollectionStats(mongoClient, dbName);
        List<CollectionStats> dbCollectionStats = bulkStats.getOrDefault(dbName, new ArrayList<>());

        // Apply collection filters if specified
        dbCollectionStats = filterCollectionStats(dbCollectionStats);

        AnalysisResult result = analyzer.analyze(
            includeCollections,
            excludeCollections,
            false,  // Don't get collection stats the old way
            !statsOnly   // includeIndexStats
        );

        // Add the bulk-gathered collection stats
        if (!indexOnly) {
            for (CollectionStats stats : dbCollectionStats) {
                result.addCollectionStats(stats);
            }
        }

        // Get database-level stats using db.stats()
        result.setDatabaseStats(analyzer.getDatabaseStats());

        // Get primary shard for this database
        Map<String, String> primaryShardMap = getPrimaryShardMapping(mongoClient);
        result.setPrimaryShard(primaryShardMap.get(dbName));

        switch (outputFormat.toLowerCase()) {
            case "json":
                new JsonOutputFormatter().format(result);
                break;
            case "html":
                new HtmlOutputFormatter().format(result);
                break;
            case "table":
            default:
                new TableOutputFormatter().format(result);
                break;
        }
    }

    /**
     * Get all collection stats using $_internalAllCollectionStats aggregation.
     * This is more efficient than calling collStats per collection.
     *
     * @param mongoClient the MongoDB client
     * @param filterDatabase optional database name to filter results (null for all databases)
     */
    private Map<String, List<CollectionStats>> getAllCollectionStats(MongoClient mongoClient, String filterDatabase) {
        Map<String, List<CollectionStats>> result = new HashMap<>();

        try {
            MongoDatabase adminDb = mongoClient.getDatabase("admin");

            // Build the $_internalAllCollectionStats pipeline
            Document statsOptions = new Document("stats",
                new Document("storageStats",
                    new Document("verbose", false)
                        .append("waitForLock", true)
                        .append("numericOnly", false)));

            Document pipeline = new Document("$_internalAllCollectionStats", statsOptions);

            List<Document> pipelineStages = new ArrayList<>();
            pipelineStages.add(pipeline);

            // Add $match filter if specific database requested
            if (filterDatabase != null) {
                Document match = new Document("$match",
                    new Document("ns", new Document("$regex", "^" + filterDatabase + "\\.")));
                pipelineStages.add(match);
            }

            Document command = new Document("aggregate", 1)
                .append("pipeline", pipelineStages)
                .append("cursor", new Document());

            logger.info("Running $_internalAllCollectionStats{}...",
                filterDatabase != null ? " for database " + filterDatabase : " for all databases");

            Document response = adminDb.runCommand(command);

            // Parse the cursor results
            Document cursor = response.get("cursor", Document.class);
            if (cursor != null) {
                @SuppressWarnings("unchecked")
                List<Document> firstBatch = (List<Document>) cursor.get("firstBatch");
                if (firstBatch != null) {
                    result = parseAllCollectionStats(firstBatch);
                }

                // Handle cursor continuation if there are more results
                Long cursorId = cursor.getLong("id");
                if (cursorId != null && cursorId != 0) {
                    String cursorNs = cursor.getString("ns");
                    result = fetchRemainingCursorResults(adminDb, cursorId, cursorNs, result);
                }
            }

            int totalCollections = result.values().stream().mapToInt(List::size).sum();
            logger.info("Retrieved {} collections across {} databases via bulk stats",
                totalCollections, result.size());

        } catch (Exception e) {
            logger.warn("Failed to get bulk collection stats via $_internalAllCollectionStats: {}", e.getMessage());
            logger.debug("Will fall back to per-collection stats if needed", e);
        }

        return result;
    }

    /**
     * Fetch remaining results from a cursor using getMore.
     */
    private Map<String, List<CollectionStats>> fetchRemainingCursorResults(
            MongoDatabase adminDb, Long cursorId, String cursorNs,
            Map<String, List<CollectionStats>> existingStats) {

        while (cursorId != null && cursorId != 0) {
            try {
                Document getMore = new Document("getMore", cursorId)
                    .append("collection", "$cmd.aggregate");

                Document response = adminDb.runCommand(getMore);
                Document cursor = response.get("cursor", Document.class);

                if (cursor != null) {
                    @SuppressWarnings("unchecked")
                    List<Document> nextBatch = (List<Document>) cursor.get("nextBatch");
                    if (nextBatch != null && !nextBatch.isEmpty()) {
                        Map<String, List<CollectionStats>> batchStats = parseAllCollectionStats(nextBatch);
                        mergeStats(existingStats, batchStats);
                    }
                    cursorId = cursor.getLong("id");
                } else {
                    break;
                }
            } catch (Exception e) {
                logger.warn("Error fetching cursor results: {}", e.getMessage());
                break;
            }
        }

        return existingStats;
    }

    /**
     * Parse the raw documents from $_internalAllCollectionStats into CollectionStats objects.
     * Groups by namespace and aggregates stats across shards for sharded collections.
     */
    private Map<String, List<CollectionStats>> parseAllCollectionStats(List<Document> docs) {
        // First pass: group by namespace with shard data
        Map<String, Map<String, Document>> namespaceShardDocs = new LinkedHashMap<>();

        for (Document doc : docs) {
            String ns = doc.getString("ns");
            if (ns == null) continue;

            String shard = doc.getString("shard");
            String shardKey = shard != null ? shard : "default";

            namespaceShardDocs
                .computeIfAbsent(ns, k -> new LinkedHashMap<>())
                .put(shardKey, doc);
        }

        // Second pass: build CollectionStats, aggregating across shards
        Map<String, List<CollectionStats>> result = new HashMap<>();

        for (Map.Entry<String, Map<String, Document>> entry : namespaceShardDocs.entrySet()) {
            String ns = entry.getKey();
            Map<String, Document> shardDocs = entry.getValue();

            // Extract database name from namespace
            int dotIndex = ns.indexOf('.');
            if (dotIndex <= 0) continue;

            String dbName = ns.substring(0, dotIndex);

            CollectionStats stats = buildCollectionStats(ns, shardDocs);
            result.computeIfAbsent(dbName, k -> new ArrayList<>()).add(stats);
        }

        return result;
    }

    /**
     * Build a CollectionStats object from shard documents.
     * Aggregates counts and sizes across shards.
     */
    private CollectionStats buildCollectionStats(String ns, Map<String, Document> shardDocs) {
        CollectionStats stats = new CollectionStats();
        stats.setNamespace(ns);

        long totalCount = 0;
        long totalSize = 0;
        long totalStorageSize = 0;
        long totalFreeStorageSize = 0;
        long totalNumOrphanDocs = 0;
        double avgObjSize = 0;
        boolean capped = false;
        long maxSize = 0;

        // Use config.collections as the authoritative source for sharded collections
        // Only truly sharded collections appear in config.collections
        boolean isSharded = shardedCollectionNamespaces.contains(ns);

        for (Map.Entry<String, Document> shardEntry : shardDocs.entrySet()) {
            String shardName = shardEntry.getKey();
            Document doc = shardEntry.getValue();
            Document storageStats = doc.get("storageStats", Document.class);

            if (storageStats == null) continue;

            long count = getLongValue(storageStats, "count");
            long size = getLongValue(storageStats, "size");
            long storageSize = getLongValue(storageStats, "storageSize");
            long freeStorageSize = getLongValue(storageStats, "freeStorageSize");
            long numOrphanDocs = getLongValue(storageStats, "numOrphanDocs");

            totalCount += count;
            totalSize += size;
            totalStorageSize += storageSize;
            totalFreeStorageSize += freeStorageSize;
            totalNumOrphanDocs += numOrphanDocs;

            // Take avgObjSize from first shard (or calculate weighted average)
            if (avgObjSize == 0) {
                avgObjSize = getDoubleValue(storageStats, "avgObjSize");
            }

            // Capped collection info
            if (storageStats.containsKey("capped")) {
                capped = storageStats.getBoolean("capped", false);
            }
            if (storageStats.containsKey("maxSize")) {
                maxSize = getLongValue(storageStats, "maxSize");
            }

            // Create shard stats for sharded collections
            if (isSharded && !"default".equals(shardName)) {
                ShardStats shardStats = new ShardStats();
                shardStats.setShardName(shardName);
                shardStats.setCount(count);
                shardStats.setSize(size);
                shardStats.setStorageSize(storageSize);
                stats.addShardStats(shardName, shardStats);
            }
        }

        stats.setCount(totalCount);
        stats.setSize(totalSize);
        stats.setStorageSize(totalStorageSize);
        stats.setFreeStorageSize(totalFreeStorageSize);
        stats.setNumOrphanDocs(totalNumOrphanDocs);
        stats.setAvgObjSize(avgObjSize);
        stats.setCapped(capped);
        stats.setMaxSize(maxSize);
        stats.setSharded(isSharded);
        stats.setShardCount(isSharded ? shardDocs.size() : 0);

        // Note: indexes and totalIndexSize not available from $_internalAllCollectionStats
        stats.setIndexes(0);
        stats.setTotalIndexSize(0L);

        return stats;
    }

    /**
     * Filter collection stats based on include/exclude patterns.
     */
    private List<CollectionStats> filterCollectionStats(List<CollectionStats> stats) {
        List<CollectionStats> filtered = new ArrayList<>(stats);

        // Apply include filter
        if (includeCollections != null && !includeCollections.trim().isEmpty()) {
            Set<String> includeSet = new HashSet<>();
            for (String c : includeCollections.split(",")) {
                includeSet.add(c.trim());
            }
            filtered.removeIf(s -> {
                String collName = s.getNamespace().substring(s.getNamespace().indexOf('.') + 1);
                return !includeSet.contains(collName);
            });
        }

        // Apply exclude filter
        if (excludeCollections != null && !excludeCollections.trim().isEmpty()) {
            Set<String> excludeSet = new HashSet<>();
            for (String c : excludeCollections.split(",")) {
                excludeSet.add(c.trim());
            }
            filtered.removeIf(s -> {
                String collName = s.getNamespace().substring(s.getNamespace().indexOf('.') + 1);
                return excludeSet.contains(collName);
            });
        }

        return filtered;
    }

    /**
     * Merge batch stats into existing stats map.
     */
    private void mergeStats(Map<String, List<CollectionStats>> existing,
                           Map<String, List<CollectionStats>> batch) {
        for (Map.Entry<String, List<CollectionStats>> entry : batch.entrySet()) {
            existing.computeIfAbsent(entry.getKey(), k -> new ArrayList<>())
                .addAll(entry.getValue());
        }
    }

    private Long getLongValue(Document doc, String key) {
        if (!doc.containsKey(key)) return 0L;
        Object value = doc.get(key);
        if (value instanceof Long) {
            return (Long) value;
        } else if (value instanceof Integer) {
            return ((Integer) value).longValue();
        } else if (value instanceof Number) {
            return ((Number) value).longValue();
        }
        return 0L;
    }

    private Double getDoubleValue(Document doc, String key) {
        if (!doc.containsKey(key)) return 0.0;
        Object value = doc.get(key);
        if (value instanceof Double) {
            return (Double) value;
        } else if (value instanceof Number) {
            return ((Number) value).doubleValue();
        }
        return 0.0;
    }
}