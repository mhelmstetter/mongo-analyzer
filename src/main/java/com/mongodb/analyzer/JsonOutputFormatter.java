package com.mongodb.analyzer;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.SerializationFeature;

import java.util.List;

public class JsonOutputFormatter implements OutputFormatter {
    
    private final ObjectMapper objectMapper;
    
    public JsonOutputFormatter() {
        this.objectMapper = new ObjectMapper();
        this.objectMapper.enable(SerializationFeature.INDENT_OUTPUT);
    }
    
    @Override
    public void format(AnalysisResult result) {
        try {
            String json = objectMapper.writeValueAsString(result);
            System.out.println(json);
        } catch (Exception e) {
            System.err.println("Error formatting JSON output: " + e.getMessage());
        }
    }
    
    @Override
    public void formatMultiple(List<AnalysisResult> results) {
        try {
            String json = objectMapper.writeValueAsString(results);
            System.out.println(json);
        } catch (Exception e) {
            System.err.println("Error formatting JSON output: " + e.getMessage());
        }
    }

    @Override
    public void formatShardedCluster(ClusterAnalysisReport report) {
        try {
            String json = objectMapper.writeValueAsString(report);
            System.out.println(json);
        } catch (Exception e) {
            System.err.println("Error formatting JSON output: " + e.getMessage());
        }
    }
}