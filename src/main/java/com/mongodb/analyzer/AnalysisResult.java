package com.mongodb.analyzer;

import java.util.ArrayList;
import java.util.List;
import java.util.stream.Collectors;

public class AnalysisResult {
    private String databaseName;
    private String primaryShard;
    private DatabaseStats databaseStats;
    private List<CollectionStats> collectionStats = new ArrayList<>();
    private List<IndexStats> indexStats = new ArrayList<>();
    
    public String getDatabaseName() {
        return databaseName;
    }
    
    public void setDatabaseName(String databaseName) {
        this.databaseName = databaseName;
    }

    public String getPrimaryShard() {
        return primaryShard;
    }

    public void setPrimaryShard(String primaryShard) {
        this.primaryShard = primaryShard;
    }

    public DatabaseStats getDatabaseStats() {
        return databaseStats;
    }

    public void setDatabaseStats(DatabaseStats databaseStats) {
        this.databaseStats = databaseStats;
    }

    public List<CollectionStats> getCollectionStats() {
        return collectionStats;
    }
    
    public void setCollectionStats(List<CollectionStats> collectionStats) {
        this.collectionStats = collectionStats;
    }
    
    public void addCollectionStats(CollectionStats stats) {
        this.collectionStats.add(stats);
    }
    
    public List<IndexStats> getIndexStats() {
        return indexStats;
    }
    
    public void setIndexStats(List<IndexStats> indexStats) {
        this.indexStats = indexStats;
    }
    
    public void addIndexStats(List<IndexStats> stats) {
        this.indexStats.addAll(stats);
    }

    public List<CollectionStats> getShardedCollections() {
        return collectionStats.stream()
                .filter(c -> c.isSharded() && c.hasShardStats())
                .collect(Collectors.toList());
    }

    public long getShardedDataSize() {
        return collectionStats.stream()
                .filter(CollectionStats::isSharded)
                .mapToLong(CollectionStats::getSize)
                .sum();
    }

    public long getUnshardedDataSize() {
        return collectionStats.stream()
                .filter(c -> !c.isSharded())
                .mapToLong(CollectionStats::getSize)
                .sum();
    }

    public long getShardedStorageSize() {
        return collectionStats.stream()
                .filter(CollectionStats::isSharded)
                .mapToLong(CollectionStats::getStorageSize)
                .sum();
    }

    public long getUnshardedStorageSize() {
        return collectionStats.stream()
                .filter(c -> !c.isSharded())
                .mapToLong(CollectionStats::getStorageSize)
                .sum();
    }

    public long getShardedCollectionCount() {
        return collectionStats.stream()
                .filter(CollectionStats::isSharded)
                .count();
    }

    public long getUnshardedCollectionCount() {
        return collectionStats.stream()
                .filter(c -> !c.isSharded())
                .count();
    }
}