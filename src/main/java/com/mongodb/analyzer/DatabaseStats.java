package com.mongodb.analyzer;

import java.util.LinkedHashMap;
import java.util.Map;

public class DatabaseStats {
    private String databaseName;
    private long collections;
    private long views;
    private long objects;
    private long dataSize;
    private long storageSize;
    private long indexes;
    private long indexSize;
    private double avgObjSize;
    // Per-shard stats from db.stats().raw
    private Map<String, ShardDbStats> shardStats = new LinkedHashMap<>();

    public static class ShardDbStats {
        private String shardName;
        private long objects;
        private long dataSize;
        private long storageSize;
        private long indexes;
        private long indexSize;

        public String getShardName() { return shardName; }
        public void setShardName(String shardName) { this.shardName = shardName; }
        public long getObjects() { return objects; }
        public void setObjects(long objects) { this.objects = objects; }
        public long getDataSize() { return dataSize; }
        public void setDataSize(long dataSize) { this.dataSize = dataSize; }
        public long getStorageSize() { return storageSize; }
        public void setStorageSize(long storageSize) { this.storageSize = storageSize; }
        public long getIndexes() { return indexes; }
        public void setIndexes(long indexes) { this.indexes = indexes; }
        public long getIndexSize() { return indexSize; }
        public void setIndexSize(long indexSize) { this.indexSize = indexSize; }

        public String getDataSizeFormatted() { return formatBytes(dataSize); }
        public String getStorageSizeFormatted() { return formatBytes(storageSize); }
        public String getIndexSizeFormatted() { return formatBytes(indexSize); }

        private static String formatBytes(long bytes) {
            if (bytes < 1024) return bytes + " B";
            if (bytes < 1024 * 1024) return String.format("%.1f KB", bytes / 1024.0);
            if (bytes < 1024 * 1024 * 1024) return String.format("%.1f MB", bytes / (1024.0 * 1024));
            return String.format("%.1f GB", bytes / (1024.0 * 1024 * 1024));
        }
    }

    public String getDatabaseName() {
        return databaseName;
    }

    public void setDatabaseName(String databaseName) {
        this.databaseName = databaseName;
    }

    public long getCollections() {
        return collections;
    }

    public void setCollections(long collections) {
        this.collections = collections;
    }

    public long getViews() {
        return views;
    }

    public void setViews(long views) {
        this.views = views;
    }

    public long getObjects() {
        return objects;
    }

    public void setObjects(long objects) {
        this.objects = objects;
    }

    public long getDataSize() {
        return dataSize;
    }

    public void setDataSize(long dataSize) {
        this.dataSize = dataSize;
    }

    public long getStorageSize() {
        return storageSize;
    }

    public void setStorageSize(long storageSize) {
        this.storageSize = storageSize;
    }

    public long getIndexes() {
        return indexes;
    }

    public void setIndexes(long indexes) {
        this.indexes = indexes;
    }

    public long getIndexSize() {
        return indexSize;
    }

    public void setIndexSize(long indexSize) {
        this.indexSize = indexSize;
    }

    public double getAvgObjSize() {
        return avgObjSize;
    }

    public void setAvgObjSize(double avgObjSize) {
        this.avgObjSize = avgObjSize;
    }

    public String getDataSizeFormatted() {
        return formatBytes(dataSize);
    }

    public String getStorageSizeFormatted() {
        return formatBytes(storageSize);
    }

    public String getIndexSizeFormatted() {
        return formatBytes(indexSize);
    }

    public Map<String, ShardDbStats> getShardStats() {
        return shardStats;
    }

    public void addShardStats(String shardName, ShardDbStats stats) {
        this.shardStats.put(shardName, stats);
    }

    public boolean hasShardStats() {
        return !shardStats.isEmpty();
    }

    private String formatBytes(long bytes) {
        if (bytes < 1024) return bytes + " B";
        if (bytes < 1024 * 1024) return String.format("%.1f KB", bytes / 1024.0);
        if (bytes < 1024 * 1024 * 1024) return String.format("%.1f MB", bytes / (1024.0 * 1024));
        return String.format("%.1f GB", bytes / (1024.0 * 1024 * 1024));
    }
}
