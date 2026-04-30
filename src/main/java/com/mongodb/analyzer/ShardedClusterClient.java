package com.mongodb.analyzer;

import com.mongodb.ConnectionString;
import com.mongodb.client.MongoClient;
import com.mongodb.client.MongoClients;
import com.mongodb.client.MongoDatabase;
import org.bson.Document;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.net.URLEncoder;
import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Comparator;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;

public class ShardedClusterClient {

    private static final Logger logger = LoggerFactory.getLogger(ShardedClusterClient.class);

    private final MongoClient mongosClient;
    private final String connectionString;

    public ShardedClusterClient(MongoClient mongosClient, String connectionString) {
        this.mongosClient = mongosClient;
        this.connectionString = connectionString;
    }

    /**
     * Returns true if the given client is connected to a mongos (sharded cluster router).
     * Uses the isdbgrid command, which only succeeds on a mongos.
     */
    public static boolean isMongos(MongoClient client) {
        try {
            Document result = client.getDatabase("admin")
                .runCommand(new Document("isdbgrid", 1));
            return result.getInteger("isdbgrid", 0) == 1;
        } catch (Exception e) {
            return false;
        }
    }

    /**
     * Reads config.shards to enumerate all shards, including their zone (tag) assignments.
     */
    public List<ShardInfo> getShards() {
        List<ShardInfo> shards = new ArrayList<>();
        for (Document doc : mongosClient.getDatabase("config").getCollection("shards").find()) {
            String id = doc.getString("_id");
            String host = doc.getString("host");
            if (id == null || host == null) continue;

            ShardInfo shard = new ShardInfo(id, host);
            // Tags field holds zone names assigned to this shard
            Object tagsObj = doc.get("tags");
            if (tagsObj instanceof List) {
                @SuppressWarnings("unchecked")
                List<String> tags = (List<String>) tagsObj;
                shard.setZones(new ArrayList<>(tags));
            }
            shards.add(shard);
        }
        return shards;
    }

    /**
     * Builds shardName -> zones map from the shard list for use in imbalance analysis.
     */
    public Map<String, List<String>> buildShardZonesMap(List<ShardInfo> shards) {
        Map<String, List<String>> map = new LinkedHashMap<>();
        for (ShardInfo s : shards) {
            map.put(s.getShardName(), s.getZones());
        }
        return map;
    }

    /**
     * Reads config.tags to get namespace -> zones mapping.
     * config.tags documents: { _id: { ns: "db.col", min: {}, max: {} }, tag: "zoneName" }
     */
    public Map<String, List<String>> getCollectionZoneRanges() {
        Map<String, List<String>> result = new LinkedHashMap<>();
        try {
            MongoDatabase configDb = mongosClient.getDatabase("config");
            for (Document doc : configDb.getCollection("tags").find()) {
                Document id = doc.get("_id", Document.class);
                String tag = doc.getString("tag");
                if (id == null || tag == null) continue;
                String ns = id.getString("ns");
                if (ns == null) continue;
                result.computeIfAbsent(ns, k -> new ArrayList<>()).add(tag);
            }
        } catch (Exception e) {
            logger.debug("Could not read config.tags: {}", e.getMessage());
        }
        return result;
    }

    /**
     * Reads config.chunks (version-agnostic) and returns chunk counts per namespace per shard.
     * For MongoDB 5.0+ chunks use uuid instead of ns; we join with config.collections.
     */
    public List<ChunkDistributionStats> getChunkDistribution() {
        List<ChunkDistributionStats> result = new ArrayList<>();
        try {
            MongoDatabase configDb = mongosClient.getDatabase("config");

            // First try simple group by ns+shard (works for pre-5.0 where ns field exists)
            List<Document> pipeline = Arrays.asList(
                new Document("$group", new Document("_id",
                    new Document("ns", "$ns").append("shard", "$shard"))
                    .append("count", new Document("$sum", 1L))
                    .append("jumbo", new Document("$sum", new Document("$cond",
                        Arrays.asList(new Document("$ifNull", Arrays.asList("$jumbo", false)), 1, 0)))))
            );

            List<Document> grouped = configDb.getCollection("chunks")
                .aggregate(pipeline).into(new ArrayList<>());

            // Check if ns was populated — if not, we need the uuid join path
            boolean hasNs = grouped.stream().anyMatch(d -> {
                Document id = d.get("_id", Document.class);
                return id != null && id.getString("ns") != null;
            });

            if (!hasNs && !grouped.isEmpty()) {
                grouped = getChunkDistributionViaUuid(configDb);
            }

            // Build ChunkDistributionStats map
            Map<String, ChunkDistributionStats> statsMap = new LinkedHashMap<>();
            for (Document doc : grouped) {
                Document id = doc.get("_id", Document.class);
                if (id == null) continue;
                String ns = id.getString("ns");
                String shard = id.getString("shard");
                if (ns == null || shard == null) continue;

                long count = getLongValue(doc, "count");
                long jumbo = getLongValue(doc, "jumbo");

                ChunkDistributionStats stats = statsMap.computeIfAbsent(ns, k -> {
                    ChunkDistributionStats s = new ChunkDistributionStats();
                    s.setNamespace(k);
                    return s;
                });
                stats.addShardChunks(shard, count);
                if (jumbo > 0) stats.setHasJumboChunks(true);
            }

            result.addAll(statsMap.values());
            // Sort by namespace
            result.sort(Comparator.comparing(ChunkDistributionStats::getNamespace));

        } catch (Exception e) {
            logger.warn("Could not read chunk distribution: {}", e.getMessage());
        }
        return result;
    }

    private List<Document> getChunkDistributionViaUuid(MongoDatabase configDb) {
        List<Document> pipeline = Arrays.asList(
            new Document("$group", new Document("_id",
                new Document("uuid", "$uuid").append("shard", "$shard"))
                .append("count", new Document("$sum", 1L))
                .append("jumbo", new Document("$sum", new Document("$cond",
                    Arrays.asList(new Document("$ifNull", Arrays.asList("$jumbo", false)), 1, 0))))),
            new Document("$lookup", new Document("from", "collections")
                .append("localField", "_id.uuid")
                .append("foreignField", "uuid")
                .append("as", "coll")),
            new Document("$unwind", "$coll"),
            new Document("$project", new Document("_id",
                new Document("ns", "$coll._id").append("shard", "$_id.shard"))
                .append("count", 1)
                .append("jumbo", 1))
        );
        return configDb.getCollection("chunks").aggregate(pipeline).into(new ArrayList<>());
    }

    /**
     * Reads WiredTiger cache statistics from serverStatus on a direct host connection.
     */
    public WiredTigerStats getWiredTigerStats(MongoClient directClient) {
        WiredTigerStats stats = new WiredTigerStats();
        try {
            Document serverStatus = directClient.getDatabase("admin")
                .runCommand(new Document("serverStatus", 1));
            Document wt = serverStatus.get("wiredTiger", Document.class);
            if (wt == null) return stats;
            Document cache = wt.get("cache", Document.class);
            if (cache == null) return stats;

            stats.setBytesReadIntoCache(getLongValue(cache, "bytes read into cache"));
            stats.setBytesWrittenFromCache(getLongValue(cache, "bytes written from cache"));
            stats.setBytesCurrentlyInCache(getLongValue(cache, "bytes currently in the cache"));
            stats.setMaxBytesConfigured(getLongValue(cache, "maximum bytes configured"));
            stats.setPagesReadIntoCache(getLongValue(cache, "pages read into cache"));
            stats.setPagesWrittenFromCache(getLongValue(cache, "pages written from cache"));
            stats.setUnmodifiedPagesEvicted(getLongValue(cache, "unmodified pages evicted"));
            stats.setModifiedPagesEvicted(getLongValue(cache, "modified pages evicted"));
        } catch (Exception e) {
            logger.debug("Could not read WiredTiger stats from host: {}", e.getMessage());
        }
        return stats;
    }

    private long getLongValue(Document doc, String key) {
        Object v = doc.get(key);
        if (v instanceof Long) return (Long) v;
        if (v instanceof Integer) return ((Integer) v).longValue();
        if (v instanceof Number) return ((Number) v).longValue();
        return 0L;
    }

    /**
     * Creates a direct MongoClient connection to a specific host, preserving credentials
     * and SSL settings from the original connection string.
     *
     * Uses directConnection=true so the driver connects to exactly this host without
     * replica set member discovery. readPreference=secondaryPreferred allows reads
     * from secondaries as well as primaries.
     */
    public MongoClient createDirectConnection(String host) {
        ConnectionString origCs = new ConnectionString(connectionString);
        StringBuilder uri = new StringBuilder("mongodb://");

        if (origCs.getCredential() != null) {
            String user = URLEncoder.encode(origCs.getCredential().getUserName(), StandardCharsets.UTF_8);
            uri.append(user);
            if (origCs.getCredential().getPassword() != null) {
                String pass = URLEncoder.encode(new String(origCs.getCredential().getPassword()), StandardCharsets.UTF_8);
                uri.append(":").append(pass);
            }
            uri.append("@");
        }

        uri.append(host);
        uri.append("/?directConnection=true&readPreference=secondaryPreferred");

        if (Boolean.TRUE.equals(origCs.getSslEnabled())) {
            uri.append("&ssl=true");
        }

        if (origCs.getCredential() != null && origCs.getCredential().getSource() != null) {
            uri.append("&authSource=").append(
                URLEncoder.encode(origCs.getCredential().getSource(), StandardCharsets.UTF_8));
        }

        logger.debug("Direct connection URI for {}: mongodb://...@{}/?directConnection=true", host, host);
        return MongoClients.create(uri.toString());
    }

    /**
     * Returns the role of the directly-connected host: PRIMARY, SECONDARY, or UNKNOWN.
     */
    public String getHostRole(MongoClient directClient) {
        try {
            Document result = directClient.getDatabase("admin")
                .runCommand(new Document("isMaster", 1));
            if (Boolean.TRUE.equals(result.getBoolean("ismaster"))) return "PRIMARY";
            if (Boolean.TRUE.equals(result.getBoolean("secondary"))) return "SECONDARY";
            return "UNKNOWN";
        } catch (Exception e) {
            logger.debug("Could not determine role for host: {}", e.getMessage());
            return "UNKNOWN";
        }
    }
}
