package com.mongodb.analyzer;

import java.util.LinkedHashMap;
import java.util.Map;

public class CollectionStats {
    private String namespace;
    private Long count;
    private Long size;
    private Long storageSize;
    private Long freeStorageSize;
    private Double avgObjSize;
    private Integer indexes;
    private Long totalIndexSize;
    private Long numOrphanDocs;
    private boolean capped = false;
    private Long maxSize;  // For capped collections
    private boolean sharded = false;
    private Integer shardCount = 0;
    private Map<String, ShardStats> shardStats = new LinkedHashMap<>();

    // WiredTiger cache stats — populated from collStats.wiredTiger.cache on direct connections
    private long bytesReadIntoCache;
    private long bytesWrittenFromCache;
    private long bytesCurrentlyInCache;
    private long pagesReadIntoCache;
    private long pagesWrittenFromCache;

    // Per-index sizes, populated from $_internalAllCollectionStats or collStats with indexDetails
    private Map<String, Long> indexSizes = new LinkedHashMap<>();
    
    public String getNamespace() {
        return namespace;
    }
    
    public void setNamespace(String namespace) {
        this.namespace = namespace;
    }
    
    public Long getCount() {
        return count != null ? count : 0L;
    }
    
    public void setCount(Long count) {
        this.count = count;
    }
    
    public Long getSize() {
        return size != null ? size : 0L;
    }
    
    public void setSize(Long size) {
        this.size = size;
    }
    
    public Long getStorageSize() {
        return storageSize != null ? storageSize : 0L;
    }

    public void setStorageSize(Long storageSize) {
        this.storageSize = storageSize;
    }

    public Long getFreeStorageSize() {
        return freeStorageSize != null ? freeStorageSize : 0L;
    }

    public void setFreeStorageSize(Long freeStorageSize) {
        this.freeStorageSize = freeStorageSize;
    }

    public Double getAvgObjSize() {
        return avgObjSize != null ? avgObjSize : 0.0;
    }
    
    public void setAvgObjSize(Double avgObjSize) {
        this.avgObjSize = avgObjSize;
    }
    
    public Integer getIndexes() {
        return indexes != null ? indexes : 0;
    }
    
    public void setIndexes(Integer indexes) {
        this.indexes = indexes;
    }
    
    public Long getTotalIndexSize() {
        return totalIndexSize != null ? totalIndexSize : 0L;
    }
    
    public void setTotalIndexSize(Long totalIndexSize) {
        this.totalIndexSize = totalIndexSize;
    }

    public Long getNumOrphanDocs() {
        return numOrphanDocs != null ? numOrphanDocs : 0L;
    }

    public void setNumOrphanDocs(Long numOrphanDocs) {
        this.numOrphanDocs = numOrphanDocs;
    }

    public boolean isCapped() {
        return capped;
    }

    public void setCapped(boolean capped) {
        this.capped = capped;
    }

    public Long getMaxSize() {
        return maxSize != null ? maxSize : 0L;
    }

    public void setMaxSize(Long maxSize) {
        this.maxSize = maxSize;
    }

    public boolean isSharded() {
        return sharded;
    }
    
    public void setSharded(boolean sharded) {
        this.sharded = sharded;
    }
    
    public Integer getShardCount() {
        return shardCount != null ? shardCount : 0;
    }
    
    public void setShardCount(Integer shardCount) {
        this.shardCount = shardCount;
    }

    public Map<String, ShardStats> getShardStats() {
        return shardStats;
    }

    public void addShardStats(String shardName, ShardStats stats) {
        this.shardStats.put(shardName, stats);
    }

    public boolean hasShardStats() {
        return !shardStats.isEmpty();
    }

    public long getBytesReadIntoCache() { return bytesReadIntoCache; }
    public void setBytesReadIntoCache(long v) { this.bytesReadIntoCache = v; }

    public long getBytesWrittenFromCache() { return bytesWrittenFromCache; }
    public void setBytesWrittenFromCache(long v) { this.bytesWrittenFromCache = v; }

    public long getBytesCurrentlyInCache() { return bytesCurrentlyInCache; }
    public void setBytesCurrentlyInCache(long v) { this.bytesCurrentlyInCache = v; }

    public long getPagesReadIntoCache() { return pagesReadIntoCache; }
    public void setPagesReadIntoCache(long v) { this.pagesReadIntoCache = v; }

    public long getPagesWrittenFromCache() { return pagesWrittenFromCache; }
    public void setPagesWrittenFromCache(long v) { this.pagesWrittenFromCache = v; }

    public Map<String, Long> getIndexSizes() { return indexSizes; }
    public void setIndexSizes(Map<String, Long> indexSizes) { this.indexSizes = indexSizes; }

    public boolean hasWiredTigerStats() {
        return bytesReadIntoCache > 0 || bytesWrittenFromCache > 0;
    }

    public String getSizeFormatted() {
        return formatBytes(getSize());
    }
    
    public String getStorageSizeFormatted() {
        return formatBytes(getStorageSize());
    }
    
    public String getTotalIndexSizeFormatted() {
        return formatBytes(getTotalIndexSize());
    }
    
    private String formatBytes(long bytes) {
        if (bytes < 1024) return bytes + " B";
        if (bytes < 1024 * 1024) return String.format("%.1f KB", bytes / 1024.0);
        if (bytes < 1024 * 1024 * 1024) return String.format("%.1f MB", bytes / (1024.0 * 1024));
        return String.format("%.1f GB", bytes / (1024.0 * 1024 * 1024));
    }
}