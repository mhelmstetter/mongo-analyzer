package com.mongodb.analyzer;

import java.util.List;

public interface OutputFormatter {
    void format(AnalysisResult result);
    void formatMultiple(List<AnalysisResult> results);
    void formatShardedCluster(ClusterAnalysisReport report);
}