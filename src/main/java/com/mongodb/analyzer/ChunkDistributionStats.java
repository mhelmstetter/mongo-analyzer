package com.mongodb.analyzer;

import java.util.LinkedHashMap;
import java.util.Map;

public class ChunkDistributionStats {
    private String namespace;
    private long totalChunks;
    private boolean hasJumboChunks;
    // shardName -> chunk count
    private Map<String, Long> chunksPerShard = new LinkedHashMap<>();

    public String getNamespace() { return namespace; }
    public void setNamespace(String namespace) { this.namespace = namespace; }

    public long getTotalChunks() { return totalChunks; }
    public void setTotalChunks(long totalChunks) { this.totalChunks = totalChunks; }

    public boolean isHasJumboChunks() { return hasJumboChunks; }
    public void setHasJumboChunks(boolean hasJumboChunks) { this.hasJumboChunks = hasJumboChunks; }

    public Map<String, Long> getChunksPerShard() { return chunksPerShard; }

    public void addShardChunks(String shard, long count) {
        chunksPerShard.put(shard, count);
        totalChunks += count;
    }

    // Returns the percentage of total chunks on the given shard (within this namespace).
    public double getShardPercent(String shard) {
        if (totalChunks <= 0) return 0;
        return chunksPerShard.getOrDefault(shard, 0L) * 100.0 / totalChunks;
    }

    public String getMaxShard() {
        return chunksPerShard.entrySet().stream()
            .max(Map.Entry.comparingByValue())
            .map(Map.Entry::getKey)
            .orElse(null);
    }

    public double getMaxShardPercent() {
        String max = getMaxShard();
        return max == null ? 0 : getShardPercent(max);
    }
}
