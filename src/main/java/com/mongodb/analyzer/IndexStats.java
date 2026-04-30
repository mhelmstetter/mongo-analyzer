package com.mongodb.analyzer;

import java.util.Date;
import java.util.HashMap;
import java.util.Map;

public class IndexStats {
    private String namespace;
    private String indexName;
    private String indexKey;
    private Long ops;
    private Date since;
    private boolean unique = false;
    private boolean sparse = false;
    private boolean partial = false;
    private boolean ttl = false;
    private Long ttlSeconds;
    private Long indexSize;
    private String replicaSetMember;
    private Map<String, Long> memberOpsMap = new HashMap<>();
    
    public String getNamespace() {
        return namespace;
    }
    
    public void setNamespace(String namespace) {
        this.namespace = namespace;
    }
    
    public String getIndexName() {
        return indexName;
    }
    
    public void setIndexName(String indexName) {
        this.indexName = indexName;
    }
    
    public String getIndexKey() {
        return indexKey;
    }
    
    public void setIndexKey(String indexKey) {
        this.indexKey = indexKey;
    }
    
    public Long getOps() {
        return ops != null ? ops : 0L;
    }
    
    public void setOps(Long ops) {
        this.ops = ops;
    }
    
    public Date getSince() {
        return since;
    }
    
    public void setSince(Date since) {
        this.since = since;
    }
    
    public boolean isUnique() {
        return unique;
    }
    
    public void setUnique(boolean unique) {
        this.unique = unique;
    }
    
    public boolean isSparse() {
        return sparse;
    }
    
    public void setSparse(boolean sparse) {
        this.sparse = sparse;
    }
    
    public boolean isPartial() {
        return partial;
    }
    
    public void setPartial(boolean partial) {
        this.partial = partial;
    }
    
    public boolean isTtl() {
        return ttl;
    }
    
    public void setTtl(boolean ttl) {
        this.ttl = ttl;
    }
    
    public Long getTtlSeconds() {
        return ttlSeconds;
    }
    
    public void setTtlSeconds(Long ttlSeconds) {
        this.ttlSeconds = ttlSeconds;
    }
    
    public Long getIndexSize() {
        return indexSize != null ? indexSize : 0L;
    }
    
    public void setIndexSize(Long indexSize) {
        this.indexSize = indexSize;
    }
    
    public String getIndexSizeFormatted() {
        return formatBytes(getIndexSize());
    }
    
    public String getIndexType() {
        StringBuilder type = new StringBuilder();
        if (unique) type.append("Unique ");
        if (sparse) type.append("Sparse ");
        if (partial) type.append("Partial ");
        if (ttl) type.append("TTL ");
        if (type.length() == 0) type.append("Standard ");
        return type.toString().trim();
    }
    
    public String getTtlDescription() {
        if (!ttl || ttlSeconds == null) return "N/A";
        if (ttlSeconds == 0) return "0 seconds";
        
        long seconds = ttlSeconds;
        if (seconds < 60) return seconds + " seconds";
        if (seconds < 3600) return (seconds / 60) + " minutes";
        if (seconds < 86400) return (seconds / 3600) + " hours";
        return (seconds / 86400) + " days";
    }
    
    private String formatBytes(long bytes) {
        if (bytes < 1024) return bytes + " B";
        if (bytes < 1024 * 1024) return String.format("%.1f KB", bytes / 1024.0);
        if (bytes < 1024 * 1024 * 1024) return String.format("%.1f MB", bytes / (1024.0 * 1024));
        return String.format("%.1f GB", bytes / (1024.0 * 1024 * 1024));
    }
    
    public String getReplicaSetMember() {
        return replicaSetMember;
    }
    
    public void setReplicaSetMember(String replicaSetMember) {
        this.replicaSetMember = replicaSetMember;
    }
    
    public Map<String, Long> getMemberOpsMap() {
        return memberOpsMap;
    }
    
    public void setMemberOpsMap(Map<String, Long> memberOpsMap) {
        this.memberOpsMap = memberOpsMap;
    }
    
    public void addMemberOps(String member, Long ops) {
        if (ops != null) {
            memberOpsMap.put(member, ops);
        }
    }
    
    public Long getTotalOpsAcrossMembers() {
        return memberOpsMap.values().stream()
                .mapToLong(Long::longValue)
                .sum();
    }
    
    public String getMemberOpsBreakdown() {
        if (memberOpsMap.isEmpty()) {
            return "No member data";
        }
        
        StringBuilder sb = new StringBuilder();
        for (Map.Entry<String, Long> entry : memberOpsMap.entrySet()) {
            if (sb.length() > 0) sb.append(", ");
            sb.append(entry.getKey()).append(": ").append(entry.getValue());
        }
        return sb.toString();
    }
}