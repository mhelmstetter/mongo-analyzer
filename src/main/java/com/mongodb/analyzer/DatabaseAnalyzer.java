package com.mongodb.analyzer;

import com.mongodb.client.MongoCollection;
import com.mongodb.client.MongoDatabase;
import com.mongodb.client.MongoIterable;
import com.mongodb.client.MongoClient;
import com.mongodb.client.MongoClients;
import com.mongodb.ReadPreference;
import com.mongodb.ConnectionString;
import com.mongodb.MongoClientSettings;
import com.mongodb.ServerAddress;
import org.bson.Document;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.stream.Collectors;

public class DatabaseAnalyzer {
    
    private static final Logger logger = LoggerFactory.getLogger(DatabaseAnalyzer.class);
    private final MongoDatabase database;
    private final String connectionString;
    
    public DatabaseAnalyzer(MongoDatabase database) {
        this.database = database;
        this.connectionString = null;
    }
    
    public DatabaseAnalyzer(MongoDatabase database, String connectionString) {
        this.database = database;
        this.connectionString = connectionString;
    }
    
    public AnalysisResult analyze(String includeCollections, String excludeCollections, 
                                 boolean includeCollStats, boolean includeIndexStats) {
        
        AnalysisResult result = new AnalysisResult();
        result.setDatabaseName(database.getName());
        
        List<String> collectionNames = getFilteredCollections(includeCollections, excludeCollections);
        logger.debug("Gathering index stats for {} collections", collectionNames.size());
        
        for (String collectionName : collectionNames) {
            logger.debug("Processing collection: {}", collectionName);
            
            try {
                MongoCollection<Document> collection = database.getCollection(collectionName);
                
                if (includeCollStats) {
                    CollectionStats stats = getCollectionStats(collection);
                    result.addCollectionStats(stats);
                }
                
                if (includeIndexStats) {
                    List<IndexStats> indexStats = getIndexStatsFromAllMembers(collection);
                    result.addIndexStats(indexStats);
                }
                
            } catch (Exception e) {
                logger.warn("Failed to analyze collection {}: {}", collectionName, e.getMessage());
            }
        }
        
        return result;
    }
    
    private List<String> getFilteredCollections(String includeCollections, String excludeCollections) {
        MongoIterable<String> allCollections = database.listCollectionNames();
        List<String> collections = new ArrayList<>();
        
        for (String collection : allCollections) {
            if (!collection.startsWith("system.")) {
                collections.add(collection);
            }
        }
        
        // Apply include filter
        if (includeCollections != null && !includeCollections.trim().isEmpty()) {
            Set<String> includeSet = Arrays.stream(includeCollections.split(","))
                    .map(String::trim)
                    .collect(Collectors.toSet());
            collections = collections.stream()
                    .filter(includeSet::contains)
                    .collect(Collectors.toList());
        }
        
        // Apply exclude filter
        if (excludeCollections != null && !excludeCollections.trim().isEmpty()) {
            Set<String> excludeSet = Arrays.stream(excludeCollections.split(","))
                    .map(String::trim)
                    .collect(Collectors.toSet());
            collections = collections.stream()
                    .filter(name -> !excludeSet.contains(name))
                    .collect(Collectors.toList());
        }
        
        return collections;
    }
    
    private CollectionStats getCollectionStats(MongoCollection<Document> collection) {
        Document collStatsCommand = new Document("collStats", collection.getNamespace().getCollectionName());
        Document stats = database.runCommand(collStatsCommand);
        
        CollectionStats collStats = new CollectionStats();
        collStats.setNamespace(collection.getNamespace().getFullName());
        collStats.setCount(getLongValue(stats, "count"));
        collStats.setSize(getLongValue(stats, "size"));
        collStats.setStorageSize(getLongValue(stats, "storageSize"));
        collStats.setAvgObjSize(getDoubleValue(stats, "avgObjSize"));
        collStats.setIndexes(getIntValue(stats, "nindexes"));
        collStats.setTotalIndexSize(getLongValue(stats, "totalIndexSize"));
        
        // WiredTiger cache stats (present on direct mongod connections only)
        Document wt = stats.get("wiredTiger", Document.class);
        if (wt != null) {
            Document cache = wt.get("cache", Document.class);
            if (cache != null) {
                collStats.setBytesReadIntoCache(getLongValue(cache, "bytes read into cache"));
                collStats.setBytesWrittenFromCache(getLongValue(cache, "bytes written from cache"));
                collStats.setBytesCurrentlyInCache(getLongValue(cache, "bytes currently in the cache"));
                collStats.setPagesReadIntoCache(getLongValue(cache, "pages read into cache"));
                collStats.setPagesWrittenFromCache(getLongValue(cache, "pages written from cache"));
            }
        }

        // Handle sharded collections
        if (stats.containsKey("sharded") && stats.getBoolean("sharded")) {
            collStats.setSharded(true);
            if (stats.containsKey("shards")) {
                Document shards = stats.get("shards", Document.class);
                collStats.setShardCount(shards.size());

                // Extract per-shard statistics
                for (String shardName : shards.keySet()) {
                    Document shardDoc = shards.get(shardName, Document.class);
                    ShardStats shardStat = new ShardStats();
                    shardStat.setShardName(shardName);
                    shardStat.setCount(getLongValue(shardDoc, "count"));
                    shardStat.setSize(getLongValue(shardDoc, "size"));
                    shardStat.setStorageSize(getLongValue(shardDoc, "storageSize"));
                    collStats.addShardStats(shardName, shardStat);
                }
            }
        }
        
        return collStats;
    }

    public DatabaseStats getDatabaseStats() {
        Document dbStatsCommand = new Document("dbStats", 1);
        Document stats = database.runCommand(dbStatsCommand);

        DatabaseStats dbStats = new DatabaseStats();
        dbStats.setDatabaseName(database.getName());
        dbStats.setCollections(getLongValue(stats, "collections"));
        dbStats.setViews(getLongValue(stats, "views"));
        dbStats.setObjects(getLongValue(stats, "objects"));
        dbStats.setDataSize(getLongValue(stats, "dataSize"));
        dbStats.setStorageSize(getLongValue(stats, "storageSize"));
        dbStats.setIndexes(getLongValue(stats, "indexes"));
        dbStats.setIndexSize(getLongValue(stats, "indexSize"));
        dbStats.setAvgObjSize(getDoubleValue(stats, "avgObjSize"));

        // Parse per-shard stats from "raw" field (sharded clusters)
        if (stats.containsKey("raw")) {
            Document raw = stats.get("raw", Document.class);
            for (String shardKey : raw.keySet()) {
                Document shardDoc = raw.get(shardKey, Document.class);

                // Extract shard name from key like "shard-name/host1:port,host2:port"
                String shardName = shardKey.contains("/") ? shardKey.substring(0, shardKey.indexOf("/")) : shardKey;

                DatabaseStats.ShardDbStats shardStats = new DatabaseStats.ShardDbStats();
                shardStats.setShardName(shardName);
                shardStats.setObjects(getLongValue(shardDoc, "objects"));
                shardStats.setDataSize(getLongValue(shardDoc, "dataSize"));
                shardStats.setStorageSize(getLongValue(shardDoc, "storageSize"));
                shardStats.setIndexes(getLongValue(shardDoc, "indexes"));
                shardStats.setIndexSize(getLongValue(shardDoc, "indexSize"));

                dbStats.addShardStats(shardName, shardStats);
            }
        }

        return dbStats;
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
    
    private Integer getIntValue(Document doc, String key) {
        if (!doc.containsKey(key)) return 0;
        Object value = doc.get(key);
        if (value instanceof Integer) {
            return (Integer) value;
        } else if (value instanceof Long) {
            return ((Long) value).intValue();
        } else if (value instanceof Number) {
            return ((Number) value).intValue();
        }
        return 0;
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
    
    private List<IndexStats> getIndexStats(MongoCollection<Document> collection) {
        List<IndexStats> indexStatsList = new ArrayList<>();
        
        try {
            // Get index definitions from listIndexes
            Map<String, Document> indexDefinitions = new HashMap<>();
            for (Document indexDef : collection.listIndexes()) {
                String indexName = indexDef.getString("name");
                indexDefinitions.put(indexName, indexDef);
            }
            
            // Get index usage statistics from $indexStats
            List<Document> indexStatsResult = collection.aggregate(
                Arrays.asList(new Document("$indexStats", new Document()))
            ).into(new ArrayList<>());
            
            for (Document indexDoc : indexStatsResult) {
                IndexStats indexStats = new IndexStats();
                indexStats.setNamespace(collection.getNamespace().getFullName());
                
                String indexName = indexDoc.getString("name");
                indexStats.setIndexName(indexName);
                
                Document key = indexDoc.get("key", Document.class);
                indexStats.setIndexKey(key != null ? key.toJson() : "{}");
                
                Document accesses = indexDoc.get("accesses", Document.class);
                if (accesses != null) {
                    indexStats.setOps(getLongValue(accesses, "ops"));
                    indexStats.setSince(accesses.getDate("since"));
                }
                
                // Get additional metadata from index definition
                Document indexDef = indexDefinitions.get(indexName);
                if (indexDef != null) {
                    indexStats.setUnique(indexDef.getBoolean("unique", false));
                    indexStats.setSparse(indexDef.getBoolean("sparse", false));
                    indexStats.setPartial(indexDef.containsKey("partialFilterExpression"));
                    indexStats.setTtl(indexDef.containsKey("expireAfterSeconds"));
                    
                    if (indexDef.containsKey("expireAfterSeconds")) {
                        Object ttlValue = indexDef.get("expireAfterSeconds");
                        if (ttlValue instanceof Number) {
                            indexStats.setTtlSeconds(((Number) ttlValue).longValue());
                        }
                    }
                    
                    // Get index size from stats
                    try {
                        Document collStatsCmd = new Document("collStats", collection.getNamespace().getCollectionName())
                                .append("indexDetails", true);
                        Document collStats = database.runCommand(collStatsCmd);
                        
                        if (collStats.containsKey("indexSizes")) {
                            Document indexSizes = collStats.get("indexSizes", Document.class);
                            if (indexSizes.containsKey(indexName)) {
                                indexStats.setIndexSize(getLongValue(indexSizes, indexName));
                            }
                        }
                    } catch (Exception e) {
                        // Ignore - index size not critical
                    }
                }
                
                indexStatsList.add(indexStats);
            }
            
        } catch (Exception e) {
            logger.warn("Failed to get index stats for collection {}: {}", 
                       collection.getNamespace().getCollectionName(), e.getMessage());
        }
        
        return indexStatsList;
    }
    
    /**
     * Analyzes a database via a direct host connection (no replica set secondary logic).
     * Used when we've already connected directly to each individual mongod.
     */
    public AnalysisResult analyzeDirectConnection(String includeCollections, String excludeCollections,
                                                  boolean includeIndexStats) {
        AnalysisResult result = new AnalysisResult();
        result.setDatabaseName(database.getName());

        List<String> collectionNames = getFilteredCollections(includeCollections, excludeCollections);
        logger.debug("Direct host: gathering stats for {} collections in {}", collectionNames.size(), database.getName());

        for (String collectionName : collectionNames) {
            try {
                MongoCollection<Document> collection = database.getCollection(collectionName);
                CollectionStats collStats = getCollectionStats(collection);
                result.addCollectionStats(collStats);

                if (includeIndexStats) {
                    List<IndexStats> indexStatsList = getIndexStats(collection);
                    result.addIndexStats(indexStatsList);
                }
            } catch (Exception e) {
                logger.warn("Failed to analyze collection {}: {}", collectionName, e.getMessage());
            }
        }

        return result;
    }

    private List<IndexStats> getIndexStatsFromAllMembers(MongoCollection<Document> collection) {
        List<IndexStats> allIndexStats = new ArrayList<>();
        
        // Get index stats from primary (current connection)
        List<IndexStats> primaryStats = getIndexStats(collection);
        for (IndexStats stats : primaryStats) {
            stats.setReplicaSetMember("primary");
            stats.addMemberOps("primary", stats.getOps());
            allIndexStats.add(stats);
        }
        
        // If we have a connection string, try to get replica set members
        if (connectionString != null) {
            List<String> secondaryHosts = getReplicaSetSecondaries();
            
            for (String host : secondaryHosts) {
                try {
                    List<IndexStats> secondaryStats = getIndexStatsFromSecondary(host, collection);
                    mergeIndexStats(allIndexStats, secondaryStats, host);
                } catch (Exception e) {
                    logger.warn("Failed to get index stats from secondary {}: {}", host, e.getMessage());
                }
            }
        }
        
        return allIndexStats;
    }
    
    private List<String> getReplicaSetSecondaries() {
        List<String> secondaries = new ArrayList<>();
        
        try {
            // Get replica set status
            Document replSetStatus = database.runCommand(new Document("replSetGetStatus", 1));
            
            if (replSetStatus.containsKey("members")) {
                @SuppressWarnings("unchecked")
                List<Document> members = (List<Document>) replSetStatus.get("members");
                
                for (Document member : members) {
                    Integer state = member.getInteger("state", 0);
                    // State 2 = SECONDARY
                    if (state == 2) {
                        String host = member.getString("name");
                        if (host != null) {
                            secondaries.add(host);
                        }
                    }
                }
            }
        } catch (Exception e) {
            logger.debug("Could not get replica set status: {}", e.getMessage());
        }
        
        return secondaries;
    }
    
    private List<IndexStats> getIndexStatsFromSecondary(String host, MongoCollection<Document> collection) {
        List<IndexStats> secondaryStats = new ArrayList<>();
        
        try {
            // Create connection string for direct secondary connection
            String secondaryConnectionString = buildSecondaryConnectionString(host);
            
            try (MongoClient secondaryClient = MongoClients.create(secondaryConnectionString)) {
                MongoDatabase secondaryDb = secondaryClient.getDatabase(database.getName());
                MongoCollection<Document> secondaryCollection = secondaryDb.getCollection(collection.getNamespace().getCollectionName());
                
                // Get index stats from secondary using $indexStats
                List<Document> indexStatsResult = secondaryCollection.aggregate(
                    Arrays.asList(new Document("$indexStats", new Document()))
                ).into(new ArrayList<>());
                
                for (Document indexDoc : indexStatsResult) {
                    IndexStats indexStats = new IndexStats();
                    indexStats.setNamespace(collection.getNamespace().getFullName());
                    indexStats.setReplicaSetMember(host);
                    
                    String indexName = indexDoc.getString("name");
                    indexStats.setIndexName(indexName);
                    
                    Document key = indexDoc.get("key", Document.class);
                    indexStats.setIndexKey(key != null ? key.toJson() : "{}");
                    
                    Document accesses = indexDoc.get("accesses", Document.class);
                    if (accesses != null) {
                        Long ops = getLongValue(accesses, "ops");
                        indexStats.setOps(ops);
                        indexStats.addMemberOps(host, ops);
                        indexStats.setSince(accesses.getDate("since"));
                    }
                    
                    secondaryStats.add(indexStats);
                }
            }
        } catch (Exception e) {
            logger.warn("Failed to connect to secondary {}: {}", host, e.getMessage());
        }
        
        return secondaryStats;
    }
    
    private String buildSecondaryConnectionString(String host) {
        if (connectionString == null) {
            return "mongodb://" + host + "/?readPreference=secondary";
        }
        
        // Parse the original connection string to extract credentials and options
        ConnectionString originalCs = new ConnectionString(connectionString);
        
        StringBuilder sb = new StringBuilder();
        sb.append("mongodb://");
        
        // Add credentials if present
        if (originalCs.getCredential() != null) {
            sb.append(originalCs.getCredential().getUserName());
            if (originalCs.getCredential().getPassword() != null) {
                sb.append(":").append(new String(originalCs.getCredential().getPassword()));
            }
            sb.append("@");
        }
        
        sb.append(host);
        sb.append("/?readPreference=secondary");
        
        // Add SSL and other options if present in original connection string
        if (originalCs.getSslEnabled() != null && originalCs.getSslEnabled()) {
            sb.append("&ssl=true");
        }
        
        return sb.toString();
    }
    
    private void mergeIndexStats(List<IndexStats> allStats, List<IndexStats> secondaryStats, String memberHost) {
        for (IndexStats secondaryStat : secondaryStats) {
            // Find matching index in allStats
            IndexStats matchingIndex = null;
            for (IndexStats existingStat : allStats) {
                if (existingStat.getIndexName().equals(secondaryStat.getIndexName()) &&
                    existingStat.getNamespace().equals(secondaryStat.getNamespace())) {
                    matchingIndex = existingStat;
                    break;
                }
            }
            
            if (matchingIndex != null) {
                // Add secondary stats to existing index
                matchingIndex.addMemberOps(memberHost, secondaryStat.getOps());
            } else {
                // This shouldn't happen, but add the secondary stat as new
                allStats.add(secondaryStat);
            }
        }
    }
}