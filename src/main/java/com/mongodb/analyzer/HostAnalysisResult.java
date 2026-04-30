package com.mongodb.analyzer;

import java.util.ArrayList;
import java.util.List;

public class HostAnalysisResult {
    private String shardName;
    private String hostName;
    private String role;
    private List<AnalysisResult> databaseResults = new ArrayList<>();

    public String getShardName() { return shardName; }
    public void setShardName(String shardName) { this.shardName = shardName; }

    public String getHostName() { return hostName; }
    public void setHostName(String hostName) { this.hostName = hostName; }

    public String getRole() { return role; }
    public void setRole(String role) { this.role = role; }

    public List<AnalysisResult> getDatabaseResults() { return databaseResults; }

    public void addDatabaseResult(AnalysisResult result) {
        databaseResults.add(result);
    }

    private WiredTigerStats wiredTigerStats;

    public WiredTigerStats getWiredTigerStats() { return wiredTigerStats; }
    public void setWiredTigerStats(WiredTigerStats wiredTigerStats) { this.wiredTigerStats = wiredTigerStats; }
}
