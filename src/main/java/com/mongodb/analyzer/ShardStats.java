package com.mongodb.analyzer;

public class ShardStats {
    private String shardName;
    private Long count;
    private Long size;
    private Long storageSize;

    public String getShardName() {
        return shardName;
    }

    public void setShardName(String shardName) {
        this.shardName = shardName;
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

    public String getSizeFormatted() {
        return formatBytes(getSize());
    }

    public String getStorageSizeFormatted() {
        return formatBytes(getStorageSize());
    }

    private String formatBytes(long bytes) {
        if (bytes < 1024) return bytes + " B";
        if (bytes < 1024 * 1024) return String.format("%.1f KB", bytes / 1024.0);
        if (bytes < 1024 * 1024 * 1024) return String.format("%.1f MB", bytes / (1024.0 * 1024));
        return String.format("%.1f GB", bytes / (1024.0 * 1024 * 1024));
    }
}
