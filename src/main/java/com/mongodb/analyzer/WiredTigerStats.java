package com.mongodb.analyzer;

public class WiredTigerStats {
    private long bytesReadIntoCache;
    private long bytesWrittenFromCache;
    private long bytesCurrentlyInCache;
    private long maxBytesConfigured;
    private long pagesReadIntoCache;
    private long pagesWrittenFromCache;
    private long unmodifiedPagesEvicted;
    private long modifiedPagesEvicted;

    public long getBytesReadIntoCache() { return bytesReadIntoCache; }
    public void setBytesReadIntoCache(long v) { this.bytesReadIntoCache = v; }

    public long getBytesWrittenFromCache() { return bytesWrittenFromCache; }
    public void setBytesWrittenFromCache(long v) { this.bytesWrittenFromCache = v; }

    public long getBytesCurrentlyInCache() { return bytesCurrentlyInCache; }
    public void setBytesCurrentlyInCache(long v) { this.bytesCurrentlyInCache = v; }

    public long getMaxBytesConfigured() { return maxBytesConfigured; }
    public void setMaxBytesConfigured(long v) { this.maxBytesConfigured = v; }

    public long getPagesReadIntoCache() { return pagesReadIntoCache; }
    public void setPagesReadIntoCache(long v) { this.pagesReadIntoCache = v; }

    public long getPagesWrittenFromCache() { return pagesWrittenFromCache; }
    public void setPagesWrittenFromCache(long v) { this.pagesWrittenFromCache = v; }

    public long getUnmodifiedPagesEvicted() { return unmodifiedPagesEvicted; }
    public void setUnmodifiedPagesEvicted(long v) { this.unmodifiedPagesEvicted = v; }

    public long getModifiedPagesEvicted() { return modifiedPagesEvicted; }
    public void setModifiedPagesEvicted(long v) { this.modifiedPagesEvicted = v; }

    public double getCacheUsedPercent() {
        if (maxBytesConfigured <= 0) return 0;
        return bytesCurrentlyInCache * 100.0 / maxBytesConfigured;
    }
}
