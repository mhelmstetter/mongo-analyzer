package com.mongodb.analyzer;

import java.io.FileWriter;
import java.io.IOException;
import java.io.PrintWriter;
import java.text.NumberFormat;
import java.time.LocalDateTime;
import java.time.format.DateTimeFormatter;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Locale;
import java.util.Map;
import java.util.Set;

public class HtmlOutputFormatter implements OutputFormatter {

    private static final NumberFormat NUMBER_FORMAT = NumberFormat.getNumberInstance(Locale.US);
    private int tableSeq = 0; // incremented for every table rendered to guarantee unique IDs
    
    @Override
    public void format(AnalysisResult result) {
        String fileName = "mongo-analysis-" + LocalDateTime.now().format(DateTimeFormatter.ofPattern("yyyyMMdd-HHmmss")) + ".html";
        
        try (PrintWriter writer = new PrintWriter(new FileWriter(fileName))) {
            writeHtmlReport(writer, result);
            System.out.println("HTML report generated: " + fileName);
        } catch (IOException e) {
            System.err.println("Error generating HTML report: " + e.getMessage());
        }
    }
    
    private void writeHtmlReport(PrintWriter writer, AnalysisResult result) {
        writeHtmlHeader(writer, result.getDatabaseName());
        writeNavigationHeader(writer, result);

        if (!result.getCollectionStats().isEmpty()) {
            Map<String, String> primaryShardMap = new HashMap<>();
            if (result.getPrimaryShard() != null) {
                primaryShardMap.put(result.getDatabaseName(), result.getPrimaryShard());
            }
            writeCollectionStatsTable(writer, result.getCollectionStats(), primaryShardMap);
        }

        if (!result.getShardedCollections().isEmpty()) {
            writeShardStatsTable(writer, result.getShardedCollections());
        }

        if (!result.getIndexStats().isEmpty()) {
            writeIndexStatsTable(writer, result.getIndexStats());
        }

        writeHtmlFooter(writer);
    }
    
    private void writeHtmlHeader(PrintWriter writer, String databaseName) {
        writer.println("<!DOCTYPE html>");
        writer.println("<html lang=\"en\">");
        writer.println("<head>");
        writer.println("    <meta charset=\"UTF-8\">");
        writer.println("    <meta name=\"viewport\" content=\"width=device-width, initial-scale=1.0\">");
        writer.println("    <title>MongoDB Analysis Report - " + escapeHtml(databaseName) + "</title>");
        writer.println("    <style>");
        writer.println("        body { font-family: 'Segoe UI', Tahoma, Geneva, Verdana, sans-serif; margin: 0; background-color: #f8f9fa; }");
        writer.println("        .container { max-width: 95%; margin: 0 auto; background-color: white; padding: 20px; border-radius: 8px; box-shadow: 0 2px 10px rgba(0,0,0,0.1); }");
        writer.println("        h1 { color: #001E2B; text-align: center; margin-bottom: 10px; }");
        writer.println("        h2 { color: #001E2B; margin-top: 40px; margin-bottom: 20px; border-bottom: 2px solid #00684A; padding-bottom: 5px; scroll-margin-top: 70px; }");
        writer.println("        .table-container { margin-bottom: 40px; overflow-x: auto; }");
        writer.println("        .controls { margin-bottom: 15px; }");
        writer.println("        .filter-input { padding: 8px; border: 1px solid #b8c4c2; border-radius: 4px; margin-right: 10px; width: 200px; }");
        writer.println("        .clear-btn { padding: 8px 12px; background-color: #5d6c74; color: white; border: none; border-radius: 4px; cursor: pointer; }");
        writer.println("        .clear-btn:hover { background-color: #21313C; }");
        writer.println("        table { width: 100%; border-collapse: collapse; margin-top: 10px; font-size: 14px; }");
        writer.println("        th, td { border: 1px solid #b8c4c2; padding: 8px; text-align: left; }");
        writer.println("        th { background-color: #00684A; color: white; font-weight: bold; cursor: pointer; user-select: none; position: relative; }");
        writer.println("        th:hover { background-color: #001E2B; }");
        writer.println("        th.sortable::after { content: ' ↕'; font-size: 12px; opacity: 0.5; }");
        writer.println("        th.sort-asc::after { content: ' ↑'; opacity: 1; }");
        writer.println("        th.sort-desc::after { content: ' ↓'; opacity: 1; }");
        writer.println("        tr:nth-child(even) { background-color: #f8f9fa; }");
        writer.println("        tr:hover { background-color: #E9FF99; }");
        writer.println("        .number { text-align: right; }");
        writer.println("        .highlight { background-color: #E9FF99 !important; }");
        writer.println("        .summary { background-color: #f0f7f4; padding: 15px; border-radius: 5px; margin-bottom: 20px; border-left: 4px solid #00684A; }");
        writer.println("        .summary h3 { margin-top: 0; color: #00684A; }");
        writer.println("        .summary-grid { display: grid; grid-template-columns: repeat(auto-fit, minmax(200px, 1fr)); gap: 10px; }");
        writer.println("        .summary-item { background-color: white; padding: 10px; border-radius: 4px; border-left: 4px solid #00684A; }");
        writer.println("        .summary-label { font-weight: bold; color: #21313C; }");
        writer.println("        .summary-value { font-size: 18px; color: #00684A; }");
        writer.println("        .nav-header { background-color: #001E2B; color: white; padding: 15px 0; margin: -20px -20px 20px -20px; position: sticky; top: 0; z-index: 1000; box-shadow: 0 2px 4px rgba(0,0,0,0.1); }");
        writer.println("        .nav-content { max-width: 95%; margin: 0 auto; padding: 0 20px; }");
        writer.println("        .nav-title { margin: 0 0 10px 0; font-size: 1.2em; font-weight: bold; }");
        writer.println("        .nav-links { display: flex; flex-wrap: wrap; gap: 10px; }");
        writer.println("        .nav-link { color: #ffffff; text-decoration: none; padding: 8px 12px; border-radius: 4px; background-color: #00684A; transition: background-color 0.2s; font-size: 0.9em; }");
        writer.println("        .nav-link:hover { background-color: #006CFA; color: white; }");
        writer.println("        .nav-link.active { background-color: #00ED64; color: #001E2B; }");
        writer.println("        .report-info { color: #b8c4c2; font-size: 0.85em; margin-top: 5px; }");
        writer.println("        .unused-index { background-color: #ffebee !important; }");
        writer.println("        .ttl-index { background-color: #fff3e0 !important; }");
        writer.println("        .sharded { background-color: #e3f2fd !important; }");
        writer.println("        .imbalanced { background-color: #ffebee !important; }");
        writer.println("        .distribution-bar { display: flex; align-items: center; }");
        writer.println("        .distribution-bar-bg { width: 80px; height: 12px; background: #e0e0e0; border-radius: 3px; margin-right: 8px; }");
        writer.println("        .distribution-bar-fill { height: 100%; border-radius: 3px; }");
        writer.println("        .distribution-bar-fill.balanced { background: #00684A; }");
        writer.println("        .distribution-bar-fill.warning { background: #ff9800; }");
        writer.println("        .distribution-bar-fill.critical { background: #f44336; }");
        writer.println("        .shard-group-start { border-top: 2px solid #b8c4c2; }");
        writer.println("        .single-shard { background-color: #fff3e0 !important; }");
        writer.println("        .stacked-bar { display: flex; width: 100px; height: 14px; background: #e0e0e0; border-radius: 3px; overflow: hidden; }");
        writer.println("        .stacked-bar-segment { height: 100%; }");
        writer.println("        .stacked-bar-segment.unsharded { background: #ff9800; }");
        writer.println("        .stacked-bar-segment.sharded { background: #2196f3; }");
    writer.println("    </style>");
        writer.println("</head>");
        writer.println("<body>");
        writer.println("    <div class=\"container\">");
    }
    
    private void writeNavigationHeader(PrintWriter writer, AnalysisResult result) {
        writer.println("        <div class=\"nav-header\">");
        writer.println("            <div class=\"nav-content\">");
        writer.println("                <div class=\"nav-title\">MongoDB Analysis Report - " + escapeHtml(result.getDatabaseName()) + "</div>");
        writer.println("                <div class=\"nav-links\">");
        
        if (!result.getCollectionStats().isEmpty()) {
            writer.println("                    <a href=\"#collection-stats\" class=\"nav-link\">Collection Statistics</a>");
        }

        if (!result.getShardedCollections().isEmpty()) {
            writer.println("                    <a href=\"#shard-stats\" class=\"nav-link\">Shard Distribution</a>");
        }

        if (!result.getIndexStats().isEmpty()) {
            writer.println("                    <a href=\"#index-stats\" class=\"nav-link\">Index Usage</a>");
        }

        writer.println("                </div>");
        writer.println("                <div class=\"report-info\" style=\"display: flex; justify-content: space-between;\">");
        writer.println("                    <span>Database: " + escapeHtml(result.getDatabaseName()) + "</span>");
        writer.println("                    <span>Generated on " + LocalDateTime.now().format(DateTimeFormatter.ofPattern("yyyy-MM-dd HH:mm:ss")) + "</span>");
        writer.println("                </div>");
        writer.println("            </div>");
        writer.println("        </div>");
    }
    
    private void writeCollectionStatsTable(PrintWriter writer, List<CollectionStats> stats, Map<String, String> primaryShardMap) {
        final String tId = "collStatsTable_" + (++tableSeq);
        final String fId = "collStatsFilter_" + tableSeq;
        writer.println("        <h2 id=\"collection-stats\">Collection Statistics</h2>");

        // Summary
        long totalDocs = stats.stream().mapToLong(CollectionStats::getCount).sum();
        long totalSize = stats.stream().mapToLong(CollectionStats::getSize).sum();
        long totalStorageSize = stats.stream().mapToLong(CollectionStats::getStorageSize).sum();
        long totalIndexSize = stats.stream().mapToLong(CollectionStats::getTotalIndexSize).sum();
        int totalIndexes = stats.stream().mapToInt(CollectionStats::getIndexes).sum();
        long shardedCount = stats.stream().filter(CollectionStats::isSharded).count();
        long unshardedCount = stats.size() - shardedCount;

        writer.println("        <div class=\"summary\">");
        writer.println("            <h3>Summary</h3>");
        writer.println("            <div class=\"summary-grid\">");
        writer.println("                <div class=\"summary-item\">");
        writer.println("                    <div class=\"summary-label\">Total Collections</div>");
        writer.println("                    <div class=\"summary-value\">" + stats.size() + "</div>");
        writer.println("                </div>");
        writer.println("                <div class=\"summary-item\">");
        writer.println("                    <div class=\"summary-label\">Total Documents</div>");
        writer.println("                    <div class=\"summary-value\">" + NUMBER_FORMAT.format(totalDocs) + "</div>");
        writer.println("                </div>");
        writer.println("                <div class=\"summary-item\">");
        writer.println("                    <div class=\"summary-label\">Total Data Size</div>");
        writer.println("                    <div class=\"summary-value\">" + formatBytes(totalSize) + "</div>");
        writer.println("                </div>");
        writer.println("                <div class=\"summary-item\">");
        writer.println("                    <div class=\"summary-label\">Total Storage Size</div>");
        writer.println("                    <div class=\"summary-value\">" + formatBytes(totalStorageSize) + "</div>");
        writer.println("                </div>");
        writer.println("                <div class=\"summary-item\">");
        writer.println("                    <div class=\"summary-label\">Total Index Size</div>");
        writer.println("                    <div class=\"summary-value\">" + formatBytes(totalIndexSize) + "</div>");
        writer.println("                </div>");
        writer.println("                <div class=\"summary-item\">");
        writer.println("                    <div class=\"summary-label\">Total Indexes</div>");
        writer.println("                    <div class=\"summary-value\">" + totalIndexes + "</div>");
        writer.println("                </div>");
        if (shardedCount > 0) {
            long shardedDataSize = stats.stream().filter(CollectionStats::isSharded).mapToLong(CollectionStats::getSize).sum();
            long unshardedDataSize = stats.stream().filter(c -> !c.isSharded()).mapToLong(CollectionStats::getSize).sum();
            writer.println("                <div class=\"summary-item\">");
            writer.println("                    <div class=\"summary-label\">Sharded (" + shardedCount + " collections)</div>");
            writer.println("                    <div class=\"summary-value\">" + formatBytes(shardedDataSize) + "</div>");
            writer.println("                </div>");
            writer.println("                <div class=\"summary-item\">");
            writer.println("                    <div class=\"summary-label\">Unsharded (" + unshardedCount + " collections)</div>");
            writer.println("                    <div class=\"summary-value\">" + formatBytes(unshardedDataSize) + "</div>");
            writer.println("                </div>");
        }
        writer.println("            </div>");
        writer.println("        </div>");
        
        writer.println("        <div class=\"table-container\">");
        writer.println("            <div class=\"controls\">");
        writer.println("                <input type=\"text\" id=\"" + fId + "\" class=\"filter-input\" placeholder=\"Filter by collection name...\">");
        writer.println("                <button class=\"clear-btn\" onclick=\"clearFilter('" + fId + "', '" + tId + "')\">Clear Filter</button>");
        writer.println("            </div>");
        boolean hasWt = stats.stream().anyMatch(CollectionStats::hasWiredTigerStats);
        writer.println("            <table id=\"" + tId + "\">");
        writer.println("                <thead>");
        writer.println("                    <tr>");
        writer.println("                        <th class=\"sortable\" onclick=\"sortTable('" + tId + "', 0, 'string')\">Collection</th>");
        writer.println("                        <th class=\"sortable\" onclick=\"sortTable('" + tId + "', 1, 'number')\">Documents</th>");
        writer.println("                        <th class=\"sortable\" onclick=\"sortTable('" + tId + "', 2, 'number')\">Data Size</th>");
        writer.println("                        <th class=\"sortable\" onclick=\"sortTable('" + tId + "', 3, 'number')\">Storage Size</th>");
        writer.println("                        <th class=\"sortable\" onclick=\"sortTable('" + tId + "', 4, 'number')\">Avg Doc Size</th>");
        writer.println("                        <th class=\"sortable\" onclick=\"sortTable('" + tId + "', 5, 'number')\">Indexes</th>");
        writer.println("                        <th class=\"sortable\" onclick=\"sortTable('" + tId + "', 6, 'number')\">Index Size</th>");
        writer.println("                        <th class=\"sortable\" onclick=\"sortTable('" + tId + "', 7, 'string')\">Sharded</th>");
        if (hasWt) {
            writer.println("                        <th class=\"sortable\" onclick=\"sortTable('" + tId + "', 8, 'number')\">Cache Reads</th>");
            writer.println("                        <th class=\"sortable\" onclick=\"sortTable('" + tId + "', 9, 'number')\">Cache Writes</th>");
            writer.println("                        <th class=\"sortable\" onclick=\"sortTable('" + tId + "', 10, 'number')\">Pages Read</th>");
            writer.println("                        <th class=\"sortable\" onclick=\"sortTable('" + tId + "', 11, 'number')\">Pages Written</th>");
        }
        writer.println("                    </tr>");
        writer.println("                </thead>");
        writer.println("                <tbody>");

        for (CollectionStats stat : stats) {
            String collectionName = extractCollectionName(stat.getNamespace());
            String rowClass = stat.isSharded() ? "sharded" : "";

            writer.println("                    <tr class=\"" + rowClass + "\">");
            writer.println("                        <td>" + escapeHtml(collectionName) + "</td>");
            writer.println("                        <td class=\"number\">" + NUMBER_FORMAT.format(stat.getCount()) + "</td>");
            writer.println("                        <td class=\"number\">" + stat.getSizeFormatted() + "</td>");
            writer.println("                        <td class=\"number\">" + stat.getStorageSizeFormatted() + "</td>");
            writer.println("                        <td class=\"number\">" + String.format("%.1f B", stat.getAvgObjSize()) + "</td>");
            writer.println("                        <td class=\"number\">" + stat.getIndexes() + "</td>");
            writer.println("                        <td class=\"number\">" + stat.getTotalIndexSizeFormatted() + "</td>");
            String shardedValue;
            if (stat.isSharded()) {
                shardedValue = "Yes (" + stat.getShardCount() + " shards)";
            } else {
                String dbName = stat.getNamespace().contains(".") ? stat.getNamespace().substring(0, stat.getNamespace().indexOf('.')) : "";
                String primaryShard = primaryShardMap != null ? primaryShardMap.get(dbName) : null;
                shardedValue = primaryShard != null ? "No (" + primaryShard + ")" : "No";
            }
            writer.println("                        <td>" + shardedValue + "</td>");
            if (hasWt) {
                writer.println("                        <td class=\"number\">" + formatBytes(stat.getBytesReadIntoCache()) + "</td>");
                writer.println("                        <td class=\"number\">" + formatBytes(stat.getBytesWrittenFromCache()) + "</td>");
                writer.println("                        <td class=\"number\">" + NUMBER_FORMAT.format(stat.getPagesReadIntoCache()) + "</td>");
                writer.println("                        <td class=\"number\">" + NUMBER_FORMAT.format(stat.getPagesWrittenFromCache()) + "</td>");
            }
            writer.println("                    </tr>");
        }

        writer.println("                </tbody>");
        writer.println("            </table>");
        writer.println("        </div>");
    }

    private void writeShardStatsTable(PrintWriter writer, List<CollectionStats> shardedCollections) {
        final String tId = "shardStatsTable_" + (++tableSeq);
        final String fId = "shardStatsFilter_" + tableSeq;
        writer.println("        <h2 id=\"shard-stats\">Shard Distribution</h2>");

        // Calculate summary stats - count unique shard names
        java.util.Set<String> uniqueShards = new java.util.HashSet<>();
        for (CollectionStats c : shardedCollections) {
            uniqueShards.addAll(c.getShardStats().keySet());
        }
        int totalShards = uniqueShards.size();
        long imbalancedCount = shardedCollections.stream()
                .filter(c -> {
                    if (c.getShardCount() <= 2) return false;
                    long totalDocs = c.getCount();
                    long totalSize = c.getSize();
                    for (ShardStats s : c.getShardStats().values()) {
                        double docsPct = totalDocs > 0 ? (s.getCount() * 100.0 / totalDocs) : 0;
                        double sizePct = totalSize > 0 ? (s.getSize() * 100.0 / totalSize) : 0;
                        if (docsPct > 50 || sizePct > 50) return true;
                    }
                    return false;
                })
                .count();

        // Summary section
        writer.println("        <div class=\"summary\">");
        writer.println("            <h3>Shard Distribution Summary</h3>");
        writer.println("            <div class=\"summary-grid\">");
        writer.println("                <div class=\"summary-item\">");
        writer.println("                    <div class=\"summary-label\">Sharded Collections</div>");
        writer.println("                    <div class=\"summary-value\">" + shardedCollections.size() + "</div>");
        writer.println("                </div>");
        writer.println("                <div class=\"summary-item\">");
        writer.println("                    <div class=\"summary-label\">Total Shards</div>");
        writer.println("                    <div class=\"summary-value\">" + totalShards + "</div>");
        writer.println("                </div>");
        writer.println("                <div class=\"summary-item\">");
        writer.println("                    <div class=\"summary-label\">Imbalanced Collections</div>");
        writer.println("                    <div class=\"summary-value\">" + imbalancedCount + "</div>");
        writer.println("                </div>");
        writer.println("            </div>");
        writer.println("        </div>");

        if (imbalancedCount > 0) {
            writer.println("        <div class=\"summary\">");
            writer.println("            <h3>Imbalance Warning</h3>");
            writer.println("            <p>Found " + imbalancedCount + " collection(s) with >50% of data on a single shard. Consider rebalancing for:");
            writer.println("            <ul>");
            writer.println("                <li>More even query distribution</li>");
            writer.println("                <li>Better storage utilization</li>");
            writer.println("                <li>Improved write scalability</li>");
            writer.println("            </ul>");
            writer.println("        </div>");
        }

        writer.println("        <div class=\"table-container\">");
        writer.println("            <div class=\"controls\">");
        writer.println("                <input type=\"text\" id=\"" + fId + "\" class=\"filter-input\" placeholder=\"Filter by collection or shard name...\">");
        writer.println("                <button class=\"clear-btn\" onclick=\"clearFilter('" + fId + "', '" + tId + "')\">Clear Filter</button>");
        writer.println("            </div>");
        writer.println("            <table id=\"" + tId + "\">");
        writer.println("                <thead>");
        writer.println("                    <tr>");
        writer.println("                        <th class=\"sortable\" onclick=\"sortTable('" + tId + "', 0, 'string')\">Namespace</th>");
        writer.println("                        <th class=\"sortable\" onclick=\"sortTable('" + tId + "', 1, 'string')\">Shard</th>");
        writer.println("                        <th class=\"sortable\" onclick=\"sortTable('" + tId + "', 2, 'number')\">Documents</th>");
        writer.println("                        <th class=\"sortable\" onclick=\"sortTable('" + tId + "', 3, 'number')\">Size</th>");
        writer.println("                        <th class=\"sortable\" onclick=\"sortTable('" + tId + "', 4, 'number')\">Storage</th>");
        writer.println("                        <th class=\"sortable\" onclick=\"sortTable('" + tId + "', 5, 'number')\">% Docs</th>");
        writer.println("                        <th>Size Distribution</th>");
        writer.println("                    </tr>");
        writer.println("                </thead>");
        writer.println("                <tbody>");

        for (CollectionStats collStats : shardedCollections) {
            String namespace = collStats.getNamespace();
            long totalDocs = collStats.getCount();
            long totalSize = collStats.getSize();

            boolean isFirstShard = true;
            for (Map.Entry<String, ShardStats> entry : collStats.getShardStats().entrySet()) {
                ShardStats shard = entry.getValue();

                // Calculate percentages
                double docsPct = totalDocs > 0 ? (shard.getCount() * 100.0 / totalDocs) : 0;
                double sizePct = totalSize > 0 ? (shard.getSize() * 100.0 / totalSize) : 0;

                // Determine if imbalanced (>50% for collections with >2 shards)
                boolean isImbalanced = collStats.getShardCount() > 2 && (docsPct > 50 || sizePct > 50);
                String rowClass = isFirstShard ? "shard-group-start" : "";
                if (isImbalanced) {
                    rowClass += (rowClass.isEmpty() ? "" : " ") + "imbalanced";
                }

                // Determine bar color
                String barClass = "balanced";
                if (sizePct > 60) {
                    barClass = "critical";
                } else if (sizePct > 40) {
                    barClass = "warning";
                }

                writer.println("                    <tr class=\"" + rowClass + "\">");
                writer.println("                        <td>" + (isFirstShard ? escapeHtml(namespace) : "") + "</td>");
                writer.println("                        <td>" + escapeHtml(shard.getShardName()) + "</td>");
                writer.println("                        <td class=\"number\">" + NUMBER_FORMAT.format(shard.getCount()) + "</td>");
                writer.println("                        <td class=\"number\">" + shard.getSizeFormatted() + "</td>");
                writer.println("                        <td class=\"number\">" + shard.getStorageSizeFormatted() + "</td>");
                writer.println("                        <td class=\"number\">" + String.format("%.1f%%", docsPct) + "</td>");
                writer.println("                        <td>");
                writer.println("                            <div class=\"distribution-bar\">");
                writer.println("                                <div class=\"distribution-bar-bg\">");
                writer.println("                                    <div class=\"distribution-bar-fill " + barClass + "\" style=\"width: " + Math.min(sizePct, 100) + "%;\"></div>");
                writer.println("                                </div>");
                writer.println("                                <span>" + String.format("%.1f%%", sizePct) + "</span>");
                writer.println("                            </div>");
                writer.println("                        </td>");
                writer.println("                    </tr>");

                isFirstShard = false;
            }
        }

        writer.println("                </tbody>");
        writer.println("            </table>");
        writer.println("        </div>");
    }

    private void writeIndexStatsTable(PrintWriter writer, List<IndexStats> stats) {
        final String tId = "indexStatsTable_" + (++tableSeq);
        final String fId = "indexStatsFilter_" + tableSeq;
        writer.println("        <h2 id=\"index-stats\">Index Usage Statistics (Replica Set Aggregated)</h2>");
        
        // Summary
        long totalOps = stats.stream().mapToLong(s -> s.getTotalOpsAcrossMembers() > 0 ? s.getTotalOpsAcrossMembers() : s.getOps()).sum();
        long totalIndexSize = stats.stream().mapToLong(IndexStats::getIndexSize).sum();
        long unusedIndexes = stats.stream().mapToLong(s -> (s.getTotalOpsAcrossMembers() == 0 && s.getOps() == 0) ? 1 : 0).sum();
        long uniqueIndexes = stats.stream().mapToLong(s -> s.isUnique() ? 1 : 0).sum();
        long ttlIndexes = stats.stream().mapToLong(s -> s.isTtl() ? 1 : 0).sum();
        
        writer.println("        <div class=\"summary\">");
        writer.println("            <h3>Index Usage Summary</h3>");
        writer.println("            <div class=\"summary-grid\">");
        writer.println("                <div class=\"summary-item\">");
        writer.println("                    <div class=\"summary-label\">Total Indexes</div>");
        writer.println("                    <div class=\"summary-value\">" + stats.size() + "</div>");
        writer.println("                </div>");
        writer.println("                <div class=\"summary-item\">");
        writer.println("                    <div class=\"summary-label\">Total Operations</div>");
        writer.println("                    <div class=\"summary-value\">" + NUMBER_FORMAT.format(totalOps) + "</div>");
        writer.println("                </div>");
        writer.println("                <div class=\"summary-item\">");
        writer.println("                    <div class=\"summary-label\">Unused Indexes</div>");
        writer.println("                    <div class=\"summary-value\">" + unusedIndexes + "</div>");
        writer.println("                </div>");
        writer.println("                <div class=\"summary-item\">");
        writer.println("                    <div class=\"summary-label\">Total Index Size</div>");
        writer.println("                    <div class=\"summary-value\">" + formatBytes(totalIndexSize) + "</div>");
        writer.println("                </div>");
        writer.println("                <div class=\"summary-item\">");
        writer.println("                    <div class=\"summary-label\">Unique Indexes</div>");
        writer.println("                    <div class=\"summary-value\">" + uniqueIndexes + "</div>");
        writer.println("                </div>");
        writer.println("                <div class=\"summary-item\">");
        writer.println("                    <div class=\"summary-label\">TTL Indexes</div>");
        writer.println("                    <div class=\"summary-value\">" + ttlIndexes + "</div>");
        writer.println("                </div>");
        writer.println("            </div>");
        writer.println("        </div>");
        
        if (unusedIndexes > 0) {
            writer.println("        <div class=\"summary\">");
            writer.println("            <h3>⚠️ Recommendations</h3>");
            writer.println("            <p>Found " + unusedIndexes + " unused indexes. Consider reviewing these for potential removal to:");
            writer.println("            <ul>");
            writer.println("                <li>Reduce storage overhead</li>");
            writer.println("                <li>Improve write performance</li>");
            writer.println("                <li>Simplify maintenance</li>");
            writer.println("            </ul>");
            writer.println("        </div>");
        }
        
        writer.println("        <div class=\"table-container\">");
        writer.println("            <div class=\"controls\">");
        writer.println("                <input type=\"text\" id=\"" + fId + "\" class=\"filter-input\" placeholder=\"Filter by collection or index name...\">");
        writer.println("                <button class=\"clear-btn\" onclick=\"clearFilter('" + fId + "', '" + tId + "')\">Clear Filter</button>");
        writer.println("            </div>");
        writer.println("            <table id=\"" + tId + "\">");
        writer.println("                <thead>");
        writer.println("                    <tr>");
        writer.println("                        <th class=\"sortable\" onclick=\"sortTable('" + tId + "', 0, 'string')\">Collection</th>");
        writer.println("                        <th class=\"sortable\" onclick=\"sortTable('" + tId + "', 1, 'string')\">Index Name</th>");
        writer.println("                        <th class=\"sortable\" onclick=\"sortTable('" + tId + "', 2, 'string')\">Index Key</th>");
        writer.println("                        <th class=\"sortable\" onclick=\"sortTable('" + tId + "', 3, 'string')\">Type</th>");
        writer.println("                        <th class=\"sortable\" onclick=\"sortTable('" + tId + "', 4, 'number')\">Size</th>");
        writer.println("                        <th class=\"sortable\" onclick=\"sortTable('" + tId + "', 5, 'number')\">Total Ops</th>");
        writer.println("                        <th class=\"sortable\" onclick=\"sortTable('" + tId + "', 6, 'string')\">Member Breakdown</th>");
        writer.println("                        <th class=\"sortable\" onclick=\"sortTable('" + tId + "', 7, 'string')\">TTL</th>");
        writer.println("                        <th class=\"sortable\" onclick=\"sortTable('" + tId + "', 8, 'string')\">Since</th>");
        writer.println("                    </tr>");
        writer.println("                </thead>");
        writer.println("                <tbody>");
        
        for (IndexStats stat : stats) {
            String collectionName = extractCollectionName(stat.getNamespace());
            String indexKey = formatIndexKey(stat.getIndexKey());
            Long indexTotalOps = stat.getTotalOpsAcrossMembers() > 0 ? stat.getTotalOpsAcrossMembers() : stat.getOps();
            String rowClass = (indexTotalOps == 0) ? "unused-index" : "";
            if (stat.isTtl()) {
                rowClass += (rowClass.isEmpty() ? "" : " ") + "ttl-index";
            }
            
            String memberBreakdown = stat.getMemberOpsBreakdown();
            
            writer.println("                    <tr class=\"" + rowClass + "\">");
            writer.println("                        <td>" + escapeHtml(collectionName) + "</td>");
            writer.println("                        <td>" + escapeHtml(stat.getIndexName()) + "</td>");
            writer.println("                        <td title=\"" + escapeHtml(stat.getIndexKey()) + "\">" + escapeHtml(indexKey) + "</td>");
            writer.println("                        <td>" + escapeHtml(stat.getIndexType()) + "</td>");
            writer.println("                        <td class=\"number\">" + stat.getIndexSizeFormatted() + "</td>");
            writer.println("                        <td class=\"number\">" + NUMBER_FORMAT.format(indexTotalOps) + "</td>");
            writer.println("                        <td title=\"" + escapeHtml(memberBreakdown) + "\">" + escapeHtml(memberBreakdown.length() > 50 ? memberBreakdown.substring(0, 47) + "..." : memberBreakdown) + "</td>");
            writer.println("                        <td>" + escapeHtml(stat.getTtlDescription()) + "</td>");
            writer.println("                        <td>" + (stat.getSince() != null ? stat.getSince().toString().substring(0, 19) : "N/A") + "</td>");
            writer.println("                    </tr>");
        }
        
        writer.println("                </tbody>");
        writer.println("            </table>");
        writer.println("        </div>");
    }
    
    private void writeHtmlFooter(PrintWriter writer) {
        writer.println("    </div>");
        writer.println("    <script>");
        writer.println("        let sortStates = {};");
        writer.println("");
        writer.println("        function sortTable(tableId, column, type) {");
        writer.println("            const table = document.getElementById(tableId);");
        writer.println("            const tbody = table.querySelector('tbody');");
        writer.println("            const headers = table.querySelectorAll('th');");
        writer.println("            const rows = Array.from(tbody.querySelectorAll('tr'));");
        writer.println("            ");
        writer.println("            const stateKey = tableId + '_' + column;");
        writer.println("            if (!sortStates[stateKey]) sortStates[stateKey] = 'none';");
        writer.println("            ");
        writer.println("            headers.forEach(h => h.classList.remove('sort-asc', 'sort-desc'));");
        writer.println("            ");
        writer.println("            let ascending = true;");
        writer.println("            if (sortStates[stateKey] === 'asc') {");
        writer.println("                ascending = false;");
        writer.println("                sortStates[stateKey] = 'desc';");
        writer.println("                headers[column].classList.add('sort-desc');");
        writer.println("            } else {");
        writer.println("                sortStates[stateKey] = 'asc';");
        writer.println("                headers[column].classList.add('sort-asc');");
        writer.println("            }");
        writer.println("            ");
        writer.println("            function parseSize(str) {");
        writer.println("                const val = str.replace(/[,%$]/g, '').trim();");
        writer.println("                const match = val.match(/^([\\d.]+)\\s*(B|KB|MB|GB|TB)?$/i);");
        writer.println("                if (!match) return parseFloat(val) || 0;");
        writer.println("                const num = parseFloat(match[1]);");
        writer.println("                const unit = (match[2] || 'B').toUpperCase();");
        writer.println("                const multipliers = { 'B': 1, 'KB': 1024, 'MB': 1024*1024, 'GB': 1024*1024*1024, 'TB': 1024*1024*1024*1024 };");
        writer.println("                return num * (multipliers[unit] || 1);");
        writer.println("            }");
        writer.println("            ");
        writer.println("            rows.sort((a, b) => {");
        writer.println("                let aVal = a.cells[column].textContent.trim();");
        writer.println("                let bVal = b.cells[column].textContent.trim();");
        writer.println("                ");
        writer.println("                if (type === 'number') {");
        writer.println("                    const aValue = parseSize(aVal);");
        writer.println("                    const bValue = parseSize(bVal);");
        writer.println("                    return ascending ? aValue - bValue : bValue - aValue;");
        writer.println("                } else {");
        writer.println("                    return ascending ? aVal.localeCompare(bVal) : bVal.localeCompare(aVal);");
        writer.println("                }");
        writer.println("            });");
        writer.println("            ");
        writer.println("            rows.forEach(row => tbody.appendChild(row));");
        writer.println("        }");
        writer.println("");
        writer.println("        function filterTable(inputId, tableId) {");
        writer.println("            const filter = document.getElementById(inputId).value.toLowerCase();");
        writer.println("            const table = document.getElementById(tableId);");
        writer.println("            const rows = table.querySelectorAll('tbody tr');");
        writer.println("            rows.forEach(row => {");
        writer.println("                const text = row.textContent.toLowerCase();");
        writer.println("                row.style.display = text.includes(filter) ? '' : 'none';");
        writer.println("                row.classList.toggle('highlight', filter && text.includes(filter));");
        writer.println("            });");
        writer.println("        }");
        writer.println("");
        writer.println("        function clearFilter(inputId, tableId) {");
        writer.println("            document.getElementById(inputId).value = '';");
        writer.println("            filterTable(inputId, tableId);");
        writer.println("        }");
        writer.println("");
        writer.println("        function filterByPrimaryShard() {");
        writer.println("            const shard = document.getElementById('shardFilter').value.toLowerCase();");
        writer.println("            const textFilter = document.getElementById('topUnshardedFilter').value.toLowerCase();");
        writer.println("            const table = document.getElementById('topUnshardedTable');");
        writer.println("            const rows = table.querySelectorAll('tbody tr');");
        writer.println("            rows.forEach(row => {");
        writer.println("                const rowShard = (row.dataset.shard || '').toLowerCase();");
        writer.println("                const rowText = row.textContent.toLowerCase();");
        writer.println("                const shardMatch = !shard || rowShard === shard;");
        writer.println("                const textMatch = !textFilter || rowText.includes(textFilter);");
        writer.println("                row.style.display = (shardMatch && textMatch) ? '' : 'none';");
        writer.println("            });");
        writer.println("        }");
        writer.println("        document.addEventListener('DOMContentLoaded', function() {");
        writer.println("            const filterInput = document.getElementById('topUnshardedFilter');");
        writer.println("            if (filterInput) { filterInput.addEventListener('input', filterByPrimaryShard); }");
        writer.println("        });");
        writer.println("");
        writer.println("        function sortGroupedTable(tableId, column, type) {");
        writer.println("            const table = document.getElementById(tableId);");
        writer.println("            const tbody = table.querySelector('tbody');");
        writer.println("            const headers = table.querySelectorAll('th');");
        writer.println("            const rows = Array.from(tbody.querySelectorAll('tr'));");
        writer.println("            ");
        writer.println("            // Group rows by database");
        writer.println("            const groups = [];");
        writer.println("            let currentGroup = [];");
        writer.println("            rows.forEach(row => {");
        writer.println("                if (row.classList.contains('shard-group-start') && currentGroup.length > 0) {");
        writer.println("                    groups.push(currentGroup);");
        writer.println("                    currentGroup = [];");
        writer.println("                }");
        writer.println("                currentGroup.push(row);");
        writer.println("            });");
        writer.println("            if (currentGroup.length > 0) groups.push(currentGroup);");
        writer.println("            ");
        writer.println("            const stateKey = tableId + '_' + column;");
        writer.println("            if (!sortStates[stateKey]) sortStates[stateKey] = 'none';");
        writer.println("            ");
        writer.println("            headers.forEach(h => h.classList.remove('sort-asc', 'sort-desc'));");
        writer.println("            ");
        writer.println("            let ascending = true;");
        writer.println("            if (sortStates[stateKey] === 'asc') {");
        writer.println("                ascending = false;");
        writer.println("                sortStates[stateKey] = 'desc';");
        writer.println("                headers[column].classList.add('sort-desc');");
        writer.println("            } else {");
        writer.println("                sortStates[stateKey] = 'asc';");
        writer.println("                headers[column].classList.add('sort-asc');");
        writer.println("            }");
        writer.println("            ");
        writer.println("            function parseSize(str) {");
        writer.println("                const val = str.replace(/[,%$]/g, '').trim();");
        writer.println("                const match = val.match(/^([\\\\d.]+)\\\\s*(B|KB|MB|GB|TB)?$/i);");
        writer.println("                if (!match) return parseFloat(val) || 0;");
        writer.println("                const num = parseFloat(match[1]);");
        writer.println("                const unit = (match[2] || 'B').toUpperCase();");
        writer.println("                const multipliers = { 'B': 1, 'KB': 1024, 'MB': 1024*1024, 'GB': 1024*1024*1024, 'TB': 1024*1024*1024*1024 };");
        writer.println("                return num * (multipliers[unit] || 1);");
        writer.println("            }");
        writer.println("            ");
        writer.println("            // Sort groups by first row's value in the specified column");
        writer.println("            groups.sort((a, b) => {");
        writer.println("                let aVal = a[0].cells[column].textContent.trim();");
        writer.println("                let bVal = b[0].cells[column].textContent.trim();");
        writer.println("                // For grouped tables, use data attribute for total if available");
        writer.println("                if (column === 1 && a[0].dataset.dbTotal) {");
        writer.println("                    aVal = a[0].dataset.dbTotal;");
        writer.println("                    bVal = b[0].dataset.dbTotal;");
        writer.println("                    return ascending ? parseFloat(aVal) - parseFloat(bVal) : parseFloat(bVal) - parseFloat(aVal);");
        writer.println("                }");
        writer.println("                if (type === 'number') {");
        writer.println("                    return ascending ? parseSize(aVal) - parseSize(bVal) : parseSize(bVal) - parseSize(aVal);");
        writer.println("                } else {");
        writer.println("                    return ascending ? aVal.localeCompare(bVal) : bVal.localeCompare(aVal);");
        writer.println("                }");
        writer.println("            });");
        writer.println("            ");
        writer.println("            // Flatten and re-append");
        writer.println("            groups.forEach(group => group.forEach(row => tbody.appendChild(row)));");
        writer.println("        }");
        writer.println("");
        writer.println("        function filterSingleShard() {");
        writer.println("            const checkbox = document.getElementById('showSingleShardOnly');");
        writer.println("            const table = document.getElementById('shardDbTable');");
        writer.println("            const rows = table.querySelectorAll('tbody tr');");
        writer.println("            rows.forEach(row => {");
        writer.println("                if (checkbox.checked) {");
        writer.println("                    row.style.display = row.dataset.shardCount === '1' ? '' : 'none';");
        writer.println("                } else {");
        writer.println("                    row.style.display = '';");
        writer.println("                }");
        writer.println("            });");
        writer.println("        }");
        writer.println("");
        writer.println("        document.addEventListener('DOMContentLoaded', function() {");
        writer.println("            const filterInputs = document.querySelectorAll('.filter-input');");
        writer.println("            filterInputs.forEach(input => {");
        writer.println("                const tableId = input.id.replace('Filter', 'Table');");
        writer.println("                input.addEventListener('input', () => filterTable(input.id, tableId));");
        writer.println("            });");
        writer.println("            ");
        writer.println("            document.querySelectorAll('.nav-link').forEach(link => {");
        writer.println("                link.addEventListener('click', function(e) {");
        writer.println("                    e.preventDefault();");
        writer.println("                    const targetId = this.getAttribute('href').substring(1);");
        writer.println("                    const targetElement = document.getElementById(targetId);");
        writer.println("                    if (targetElement) {");
        writer.println("                        targetElement.scrollIntoView({ behavior: 'smooth', block: 'start' });");
        writer.println("                        document.querySelectorAll('.nav-link').forEach(l => l.classList.remove('active'));");
        writer.println("                        this.classList.add('active');");
        writer.println("                    }");
        writer.println("                });");
        writer.println("            });");
        writer.println("        });");
        writer.println("    </script>");
        writer.println("</body>");
        writer.println("</html>");
    }
    
    private String extractCollectionName(String namespace) {
        if (namespace == null) return "unknown";
        int dotIndex = namespace.lastIndexOf('.');
        return dotIndex >= 0 ? namespace.substring(dotIndex + 1) : namespace;
    }
    
    private String formatIndexKey(String indexKey) {
        if (indexKey == null || indexKey.isEmpty()) return "{}";
        String simplified = indexKey
            .replaceAll("\\{\"", "")
            .replaceAll("\":", ":")
            .replaceAll("\"}", "")
            .replaceAll("\",\"", ", ");
        return simplified.length() > 60 ? simplified.substring(0, 57) + "..." : simplified;
    }
    
    private String escapeHtml(String text) {
        if (text == null) return "";
        return text.replace("&", "&amp;")
                   .replace("<", "&lt;")
                   .replace(">", "&gt;")
                   .replace("\"", "&quot;")
                   .replace("'", "&#x27;");
    }
    
    private String formatBytes(long bytes) {
        // Handle negative values
        String sign = "";
        if (bytes < 0) {
            sign = "-";
            bytes = Math.abs(bytes);
        }
        if (bytes < 1024) return sign + bytes + " B";
        if (bytes < 1024 * 1024) return sign + String.format("%.1f KB", bytes / 1024.0);
        if (bytes < 1024 * 1024 * 1024) return sign + String.format("%.1f MB", bytes / (1024.0 * 1024));
        return sign + String.format("%.1f GB", bytes / (1024.0 * 1024 * 1024));
    }
    
    @Override
    public void formatShardedCluster(ClusterAnalysisReport report) {
        String fileName = "mongo-analysis-sharded-" + LocalDateTime.now().format(DateTimeFormatter.ofPattern("yyyyMMdd-HHmmss")) + ".html";

        try (PrintWriter writer = new PrintWriter(new FileWriter(fileName))) {
            writeShardedClusterReport(writer, report);
            System.out.println("HTML report generated: " + fileName);
        } catch (IOException e) {
            System.err.println("Error generating HTML report: " + e.getMessage());
        }
    }

    private void writeShardedClusterReport(PrintWriter writer, ClusterAnalysisReport report) {
        writeHtmlHeader(writer, "Sharded Cluster - Per-Host Analysis");

        // Executive summary card
        writer.println("        <div class=\"summary\">");
        writer.println("            <h3>Cluster Executive Summary</h3>");
        writer.println("            <div class=\"summary-grid\">");
        writer.println("                <div class=\"summary-item\"><div class=\"summary-label\">Shards</div><div class=\"summary-value\">" + report.getTotalShards() + "</div></div>");
        writer.println("                <div class=\"summary-item\"><div class=\"summary-label\">Total Hosts</div><div class=\"summary-value\">" + report.getTotalHosts() + "</div></div>");
        writer.println("                <div class=\"summary-item\"><div class=\"summary-label\">Primaries</div><div class=\"summary-value\">" + report.getTotalPrimaryHosts() + "</div></div>");
        writer.println("                <div class=\"summary-item\"><div class=\"summary-label\">Secondaries</div><div class=\"summary-value\">" + (report.getTotalHosts() - report.getTotalPrimaryHosts()) + "</div></div>");
        writer.println("                <div class=\"summary-item\"><div class=\"summary-label\">Total Data</div><div class=\"summary-value\">" + formatBytes(report.getClusterTotalDataSize()) + "</div></div>");
        writer.println("                <div class=\"summary-item\"><div class=\"summary-label\">Total Storage</div><div class=\"summary-value\">" + formatBytes(report.getClusterTotalStorageSize()) + "</div></div>");
        writer.println("                <div class=\"summary-item\"><div class=\"summary-label\">Total Ops (all hosts)</div><div class=\"summary-value\">" + NUMBER_FORMAT.format(report.getClusterTotalOps()) + "</div></div>");
        writer.println("                <div class=\"summary-item\"><div class=\"summary-label\">Total Chunks</div><div class=\"summary-value\">" + NUMBER_FORMAT.format(report.getTotalChunks()) + "</div></div>");
        if (report.hasZones()) {
            writer.println("                <div class=\"summary-item\"><div class=\"summary-label\">Zones</div><div class=\"summary-value\">" + escapeHtml(String.join(", ", report.getZoneNames())) + "</div></div>");
        }
        writer.println("            </div>");
        // Issues
        long criticalIssues = report.getDataImbalances().stream().filter(e -> e.isCritical).count()
            + report.getChunkImbalances().stream().filter(e -> e.isCritical).count()
            + report.getActivityImbalances().stream().filter(e -> e.isCritical).count();
        long warnIssues = report.getDataImbalances().size() + report.getChunkImbalances().size()
            + report.getActivityImbalances().stream().filter(e -> e.isWarning).count() - criticalIssues;
        if (criticalIssues > 0) {
            writer.println("            <p style=\"color:red;font-weight:bold\">&#9888; " + criticalIssues + " critical issue(s) detected</p>");
        }
        if (warnIssues > 0) {
            writer.println("            <p style=\"color:#cc7700;font-weight:bold\">&#9888; " + warnIssues + " warning(s) detected</p>");
        }
        if (criticalIssues == 0 && warnIssues == 0) {
            writer.println("            <p style=\"color:#00684A;font-weight:bold\">&#10003; No significant imbalances detected</p>");
        }
        writer.println("        </div>");

        // WiredTiger Cache Stats
        writer.println("        <h2>WiredTiger Cache Statistics</h2>");
        writer.println("        <div class=\"table-container\">");
        writer.println("            <div class=\"controls\"><input type=\"text\" id=\"cacheFilter\" class=\"filter-input\" placeholder=\"Filter...\"><button class=\"clear-btn\" onclick=\"clearFilter('cacheFilter','cacheTable')\">Clear</button></div>");
        writer.println("            <table id=\"cacheTable\">");
        writer.println("                <thead><tr>");
        for (String h : new String[]{"Shard","Host","Role","Bytes Read into Cache","Bytes Written from Cache","Cache Used %","Cache Max"}) {
            writer.println("                    <th class=\"sortable\">" + escapeHtml(h) + "</th>");
        }
        writer.println("                </tr></thead><tbody>");
        for (HostAnalysisResult host : report.getHostResults()) {
            WiredTigerStats wt = host.getWiredTigerStats() != null ? host.getWiredTigerStats() : new WiredTigerStats();
            writer.println("                <tr>");
            writer.println("                    <td>" + escapeHtml(host.getShardName()) + "</td>");
            writer.println("                    <td>" + escapeHtml(host.getHostName()) + "</td>");
            writer.println("                    <td>" + escapeHtml(host.getRole()) + "</td>");
            writer.println("                    <td class=\"number\">" + formatBytes(wt.getBytesReadIntoCache()) + "</td>");
            writer.println("                    <td class=\"number\">" + formatBytes(wt.getBytesWrittenFromCache()) + "</td>");
            writer.println("                    <td class=\"number\">" + String.format("%.1f%%", wt.getCacheUsedPercent()) + "</td>");
            writer.println("                    <td class=\"number\">" + formatBytes(wt.getMaxBytesConfigured()) + "</td>");
            writer.println("                </tr>");
        }
        writer.println("                </tbody></table></div>");

        // Shard Totals
        writer.println("        <h2>Shard Totals</h2>");
        writer.println("        <div class=\"table-container\">");
        writer.println("            <table id=\"shardTotalsTable\">");
        writer.println("                <thead><tr>");
        for (String h : new String[]{"Shard","Zone(s)","Hosts","Data Size","Storage Size","Index Size","Documents","Total Ops","% Ops","Cache Reads","Cache Writes"}) {
            writer.println("                    <th class=\"sortable\">" + escapeHtml(h) + "</th>");
        }
        writer.println("                </tr></thead><tbody>");
        long clusterOps = report.getClusterTotalOps();
        for (ClusterAnalysisReport.ShardTotals st : report.getShardTotalsMap().values()) {
            writer.println("                <tr>");
            writer.println("                    <td>" + escapeHtml(st.shardName) + "</td>");
            writer.println("                    <td>" + escapeHtml(st.getZonesDisplay()) + "</td>");
            writer.println("                    <td class=\"number\">" + st.hostCount + "</td>");
            writer.println("                    <td class=\"number\">" + formatBytes(st.totalDataSize) + "</td>");
            writer.println("                    <td class=\"number\">" + formatBytes(st.totalStorageSize) + "</td>");
            writer.println("                    <td class=\"number\">" + formatBytes(st.totalIndexSize) + "</td>");
            writer.println("                    <td class=\"number\">" + NUMBER_FORMAT.format(st.totalDocuments) + "</td>");
            writer.println("                    <td class=\"number\">" + NUMBER_FORMAT.format(st.totalOps) + "</td>");
            writer.println("                    <td class=\"number\">" + String.format("%.1f%%", st.getPercentOfClusterOps(clusterOps)) + "</td>");
            writer.println("                    <td class=\"number\">" + formatBytes(st.totalBytesReadIntoCache) + "</td>");
            writer.println("                    <td class=\"number\">" + formatBytes(st.totalBytesWrittenFromCache) + "</td>");
            writer.println("                </tr>");
        }
        writer.println("                </tbody></table></div>");

        // Chunk Distribution
        if (!report.getChunkDistribution().isEmpty()) {
            writer.println("        <h2>Chunk Distribution</h2>");
            writer.println("        <div class=\"table-container\">");
            writer.println("            <div class=\"controls\"><input type=\"text\" id=\"chunkFilter\" class=\"filter-input\" placeholder=\"Filter...\"><button class=\"clear-btn\" onclick=\"clearFilter('chunkFilter','chunkTable')\">Clear</button></div>");
            writer.println("            <table id=\"chunkTable\">");
            writer.println("                <thead><tr>");
            for (String h : new String[]{"Namespace","Shard","Zone","Total Chunks","Shard Chunks","% Chunks","Jumbo","Status"}) {
                writer.println("                    <th class=\"sortable\">" + escapeHtml(h) + "</th>");
            }
            writer.println("                </tr></thead><tbody>");
            for (ChunkDistributionStats cds : report.getChunkDistribution()) {
                boolean first = true;
                for (Map.Entry<String, Long> e : cds.getChunksPerShard().entrySet()) {
                    double pct = cds.getShardPercent(e.getKey());
                    String rowStyle = pct >= 75 ? " style=\"background-color:#ffcccc\"" : pct >= 60 ? " style=\"background-color:#fff3cd\"" : "";
                    List<String> zones = report.getShardZones().getOrDefault(e.getKey(), java.util.Collections.emptyList());
                    String status = pct >= 75 ? "CRITICAL" : pct >= 60 ? "WARNING" : "OK";
                    writer.println("                <tr" + rowStyle + ">");
                    writer.println("                    <td>" + (first ? escapeHtml(cds.getNamespace()) : "") + "</td>");
                    writer.println("                    <td>" + escapeHtml(e.getKey()) + "</td>");
                    writer.println("                    <td>" + escapeHtml(zones.isEmpty() ? "-" : String.join(",", zones)) + "</td>");
                    writer.println("                    <td class=\"number\">" + (first ? cds.getTotalChunks() : "") + "</td>");
                    writer.println("                    <td class=\"number\">" + NUMBER_FORMAT.format(e.getValue()) + "</td>");
                    writer.println("                    <td class=\"number\">" + String.format("%.1f%%", pct) + "</td>");
                    writer.println("                    <td>" + (first && cds.isHasJumboChunks() ? "YES" : first ? "no" : "") + "</td>");
                    writer.println("                    <td>" + status + "</td>");
                    writer.println("                </tr>");
                    first = false;
                }
            }
            writer.println("                </tbody></table></div>");
        }

        // Namespace / Shard Distribution (full table, one row per namespace, sortable/filterable)
        {
            Map<String, Map<String, ClusterAnalysisReport.NamespaceShardEntry>> dist =
                report.getNamespaceDistribution();
            List<String> shardNames = new ArrayList<>(report.getShardTotalsMap().keySet());
            Set<String> imbalancedNs = new java.util.HashSet<>();
            for (ClusterAnalysisReport.DataImbalanceEntry e : report.getDataImbalances()) {
                imbalancedNs.add(e.namespace);
            }

            writer.println("        <h2>Namespace Shard Distribution (" + dist.size() + " namespaces)</h2>");
            writer.println("        <div class=\"table-container\">");
            writer.println("            <div class=\"controls\">");
            writer.println("                <input type=\"text\" id=\"nsDist Filter\" class=\"filter-input\" placeholder=\"Filter namespace...\">");
            writer.println("                <button class=\"clear-btn\" onclick=\"clearFilter('nsDistFilter','nsDistTable')\">Clear</button>");
            writer.println("                <label style=\"margin-left:20px\"><input type=\"checkbox\" id=\"nsImbalancedOnly\" onchange=\"filterNsTable()\"> Imbalanced only</label>");
            writer.println("            </div>");
            writer.println("            <table id=\"nsDistTable\">");
            writer.println("                <thead><tr>");
            writer.println("                    <th class=\"sortable\" onclick=\"sortTable('nsDistTable',0,'string')\">Namespace</th>");
            writer.println("                    <th class=\"sortable\" onclick=\"sortTable('nsDistTable',1,'string')\">Zone</th>");
            writer.println("                    <th class=\"sortable\" onclick=\"sortTable('nsDistTable',2,'number')\">Total Storage</th>");
            writer.println("                    <th class=\"sortable\" onclick=\"sortTable('nsDistTable',3,'number')\">Documents</th>");
            for (int si = 0; si < shardNames.size(); si++) {
                int col = 4 + si * 2;
                writer.println("                    <th class=\"sortable\" onclick=\"sortTable('nsDistTable'," + col + ",'number')\">"
                    + escapeHtml(shardNames.get(si)) + " Storage</th>");
                writer.println("                    <th class=\"sortable\" onclick=\"sortTable('nsDistTable'," + (col+1) + ",'number')\">"
                    + escapeHtml(shardNames.get(si)) + " %</th>");
            }
            writer.println("                    <th>Status</th>");
            writer.println("                </tr></thead><tbody>");

            for (Map.Entry<String, Map<String, ClusterAnalysisReport.NamespaceShardEntry>> entry : dist.entrySet()) {
                String ns = entry.getKey();
                Map<String, ClusterAnalysisReport.NamespaceShardEntry> shardData = entry.getValue();
                long totalStorage = shardData.values().stream().mapToLong(e -> e.storageSize).sum();
                long totalDocs = shardData.values().stream().mapToLong(e -> e.documentCount).sum();
                boolean isImb = imbalancedNs.contains(ns);
                List<String> nsZones = report.getCollectionZones().getOrDefault(ns, java.util.Collections.emptyList());

                String rowStyle = isImb ? " style=\"background-color:#fff3cd\"" : "";
                writer.println("                <tr" + rowStyle + " data-imbalanced=\"" + isImb + "\">");
                writer.println("                    <td>" + escapeHtml(ns) + "</td>");
                writer.println("                    <td>" + escapeHtml(nsZones.isEmpty() ? "-" : String.join(",", nsZones)) + "</td>");
                writer.println("                    <td class=\"number\">" + formatBytes(totalStorage) + "</td>");
                writer.println("                    <td class=\"number\">" + NUMBER_FORMAT.format(totalDocs) + "</td>");
                for (String shard : shardNames) {
                    ClusterAnalysisReport.NamespaceShardEntry e = shardData.get(shard);
                    if (e != null) {
                        double pct = e.getStoragePercent(totalStorage);
                        String cellStyle = pct >= 75 ? " style=\"color:red;font-weight:bold\"" : pct >= 60 ? " style=\"color:#cc7700;font-weight:bold\"" : "";
                        writer.println("                    <td class=\"number\">" + formatBytes(e.storageSize) + "</td>");
                        writer.println("                    <td class=\"number\"" + cellStyle + ">" + String.format("%.1f%%", pct) + "</td>");
                    } else {
                        writer.println("                    <td>—</td><td>—</td>");
                    }
                }
                writer.println("                    <td>" + (isImb ? "<span style=\"color:#cc7700;font-weight:bold\">IMBALANCED</span>" : "ok") + "</td>");
                writer.println("                </tr>");
            }
            writer.println("                </tbody></table></div>");
            writer.println("        <script>");
            writer.println("        function filterNsTable() {");
            writer.println("            var imbalancedOnly = document.getElementById('nsImbalancedOnly').checked;");
            writer.println("            var rows = document.querySelectorAll('#nsDistTable tbody tr');");
            writer.println("            rows.forEach(function(row) {");
            writer.println("                if (imbalancedOnly && row.getAttribute('data-imbalanced') !== 'true') {");
            writer.println("                    row.style.display = 'none';");
            writer.println("                } else {");
            writer.println("                    row.style.display = '';");
            writer.println("                }");
            writer.println("            });");
            writer.println("        }");
            writer.println("        </script>");
        }

        // Activity (Ops) by Shard
        writer.println("        <h2>Shard Activity (total ops across all members)</h2>");
        writer.println("        <div class=\"table-container\">");
        writer.println("            <table id=\"activityTable\">");
        writer.println("                <thead><tr>");
        for (String h : new String[]{"Shard","Zone(s)","Total Ops","% of Cluster","Status"}) {
            writer.println("                    <th class=\"sortable\">" + escapeHtml(h) + "</th>");
        }
        writer.println("                </tr></thead><tbody>");
        for (ClusterAnalysisReport.ActivityImbalanceEntry e : report.getActivityImbalances()) {
            String rowStyle = e.isCritical ? " style=\"background-color:#ffcccc\"" : e.isWarning ? " style=\"background-color:#fff3cd\"" : "";
            writer.println("                <tr" + rowStyle + ">");
            writer.println("                    <td>" + escapeHtml(e.shardName) + "</td>");
            writer.println("                    <td>" + escapeHtml(e.getZonesDisplay()) + "</td>");
            writer.println("                    <td class=\"number\">" + NUMBER_FORMAT.format(e.totalOps) + "</td>");
            writer.println("                    <td class=\"number\">" + String.format("%.1f%%", e.percentOfCluster) + "</td>");
            writer.println("                    <td>" + e.getSeverity() + "</td>");
            writer.println("                </tr>");
        }
        writer.println("                </tbody></table></div>");

        // Per-shard, per-host index detail (primaries only; collapsed by default)
        writer.println("        <h2>Per-Host Index Detail (Primaries)</h2>");
        writer.println("        <p style=\"color:#5d6c74;font-size:0.9em\">Collection storage is shown in the Namespace Distribution table above. Click a shard to expand index stats.</p>");
        String currentShard = null;
        for (HostAnalysisResult hostResult : report.getHostResults()) {
            if (!"PRIMARY".equals(hostResult.getRole())) continue;

            if (!hostResult.getShardName().equals(currentShard)) {
                if (currentShard != null) {
                    writer.println("        </details>");
                }
                currentShard = hostResult.getShardName();
                writer.println("        <details>");
                writer.println("            <summary><strong>Shard: " + escapeHtml(currentShard) + "</strong></summary>");
            }

            writer.println("        <h4 style=\"margin-left:1em\">" + escapeHtml(hostResult.getHostName()) + "</h4>");

            for (AnalysisResult dbResult : hostResult.getDatabaseResults()) {
                List<IndexStats> idxStats = dbResult.getIndexStats();
                if (idxStats.isEmpty()) continue;

                writer.println("        <h5 style=\"color:#5d6c74;margin-left:1em\">Database: " + escapeHtml(dbResult.getDatabaseName()) + "</h5>");
                writeIndexStatsTable(writer, idxStats);
            }
        }
        if (currentShard != null) {
            writer.println("        </details>");
        }

        writeHtmlFooter(writer);
    }

    @Override
    public void formatMultiple(List<AnalysisResult> results) {
        String fileName = "mongo-analysis-all-dbs-" + LocalDateTime.now().format(DateTimeFormatter.ofPattern("yyyyMMdd-HHmmss")) + ".html";
        
        try (PrintWriter writer = new PrintWriter(new FileWriter(fileName))) {
            writeHtmlReportMultiple(writer, results);
            System.out.println("HTML report generated: " + fileName);
        } catch (IOException e) {
            System.err.println("Error generating HTML report: " + e.getMessage());
        }
    }
    
    private void writeHtmlReportMultiple(PrintWriter writer, List<AnalysisResult> results) {
        writeHtmlHeader(writer, "All Databases");
        writeNavigationHeaderMultiple(writer, results);

        // Combine all stats and build primary shard map
        List<CollectionStats> allCollectionStats = new ArrayList<>();
        List<IndexStats> allIndexStats = new ArrayList<>();
        List<CollectionStats> allShardedCollections = new ArrayList<>();
        Map<String, String> primaryShardMap = new HashMap<>();

        for (AnalysisResult result : results) {
            allCollectionStats.addAll(result.getCollectionStats());
            allIndexStats.addAll(result.getIndexStats());
            allShardedCollections.addAll(result.getShardedCollections());
            if (result.getPrimaryShard() != null) {
                primaryShardMap.put(result.getDatabaseName(), result.getPrimaryShard());
            }
        }

        // Database summary table
        writeDatabaseSummaryTable(writer, results);

        if (!allCollectionStats.isEmpty()) {
            writeCollectionStatsTable(writer, allCollectionStats, primaryShardMap);
        }

        if (!allShardedCollections.isEmpty()) {
            writeShardStatsTable(writer, allShardedCollections);
        }

        // Shard summary table (if sharded cluster)
        boolean hasShardInfo = results.stream().anyMatch(r -> r.getPrimaryShard() != null);
        if (hasShardInfo) {
            writeShardSummaryTable(writer, results);
            writeImbalanceAnalysisSection(writer, results);
            writeShardDatabaseBreakdownTable(writer, results);
        }

        if (!allIndexStats.isEmpty()) {
            writeIndexStatsTable(writer, allIndexStats);
        }

        writeHtmlFooter(writer);
    }
    
    private void writeNavigationHeaderMultiple(PrintWriter writer, List<AnalysisResult> results) {
        writer.println("        <div class=\"nav-header\">");
        writer.println("            <div class=\"nav-content\">");
        writer.println("                <div class=\"nav-title\">MongoDB Multi-Database Analysis Report</div>");
        writer.println("                <div class=\"nav-links\">");
        
        writer.println("                    <a href=\"#database-summary\" class=\"nav-link\">Database Summary</a>");

        boolean hasCollStats = results.stream().anyMatch(r -> !r.getCollectionStats().isEmpty());
        boolean hasShardStats = results.stream().anyMatch(r -> !r.getShardedCollections().isEmpty());
        boolean hasShardInfo = results.stream().anyMatch(r -> r.getPrimaryShard() != null);
        boolean hasIndexStats = results.stream().anyMatch(r -> !r.getIndexStats().isEmpty());

        if (hasCollStats) {
            writer.println("                    <a href=\"#collection-stats\" class=\"nav-link\">Collection Statistics</a>");
        }

        if (hasShardStats) {
            writer.println("                    <a href=\"#shard-stats\" class=\"nav-link\">Shard Distribution</a>");
        }

        if (hasShardInfo) {
            writer.println("                    <a href=\"#shard-summary\" class=\"nav-link\">Shard Summary</a>");
            writer.println("                    <a href=\"#imbalance-analysis\" class=\"nav-link\">Imbalance Analysis</a>");
            writer.println("                    <a href=\"#shard-db-breakdown\" class=\"nav-link\">Per-Shard DB Breakdown</a>");
        }

        if (hasIndexStats) {
            writer.println("                    <a href=\"#index-stats\" class=\"nav-link\">Index Usage</a>");
        }

        writer.println("                </div>");
        writer.println("                <div class=\"report-info\" style=\"display: flex; justify-content: space-between;\">");
        writer.println("                    <span>Databases analyzed: " + results.size() + "</span>");
        writer.println("                    <span>Generated on " + LocalDateTime.now().format(DateTimeFormatter.ofPattern("yyyy-MM-dd HH:mm:ss")) + "</span>");
        writer.println("                </div>");
        writer.println("            </div>");
        writer.println("        </div>");
    }
    
    private void writeShardSummaryTable(PrintWriter writer, List<AnalysisResult> results) {
        writer.println("        <h2 id=\"shard-summary\">Shard Summary</h2>");

        // Aggregate per-shard totals from db.stats().raw
        // Array: [dataSize, storageSize, indexSize, unshardedDataSize, shardedDataSize]
        Map<String, long[]> shardTotals = new java.util.TreeMap<>();

        // First pass: get totals from db.stats().raw
        for (AnalysisResult result : results) {
            DatabaseStats dbStats = result.getDatabaseStats();
            if (dbStats != null && dbStats.hasShardStats()) {
                for (Map.Entry<String, DatabaseStats.ShardDbStats> entry : dbStats.getShardStats().entrySet()) {
                    String shardName = entry.getKey();
                    DatabaseStats.ShardDbStats stats = entry.getValue();
                    long[] totals = shardTotals.computeIfAbsent(shardName, k -> new long[5]);
                    totals[0] += stats.getDataSize();
                    totals[1] += stats.getStorageSize();
                    totals[2] += stats.getIndexSize();
                }
            }
        }

        // Second pass: calculate unsharded vs sharded data per shard from collection stats
        long totalUnshardedData = 0;
        long totalShardedData = 0;
        for (AnalysisResult result : results) {
            String primaryShard = result.getPrimaryShard();
            for (CollectionStats coll : result.getCollectionStats()) {
                if (coll.isSharded()) {
                    // Sharded collection - add per-shard data
                    if (coll.hasShardStats()) {
                        for (Map.Entry<String, ShardStats> shardEntry : coll.getShardStats().entrySet()) {
                            String shardName = shardEntry.getKey();
                            long shardDataSize = shardEntry.getValue().getSize();
                            long[] totals = shardTotals.get(shardName);
                            if (totals != null) {
                                totals[4] += shardDataSize;
                            }
                            totalShardedData += shardDataSize;
                        }
                    }
                } else {
                    // Unsharded collection - all data on primary shard
                    long collDataSize = coll.getSize();
                    if (primaryShard != null) {
                        long[] totals = shardTotals.get(primaryShard);
                        if (totals != null) {
                            totals[3] += collDataSize;
                        }
                    }
                    totalUnshardedData += collDataSize;
                }
            }
        }

        if (shardTotals.isEmpty()) {
            writer.println("        <p>No shard information available.</p>");
            return;
        }

        // Calculate totals for summary
        long totalDataSize = 0, totalStorageSize = 0, totalIndexSize = 0;
        for (long[] totals : shardTotals.values()) {
            totalDataSize += totals[0];
            totalStorageSize += totals[1];
            totalIndexSize += totals[2];
        }

        writer.println("        <div class=\"summary\">");
        writer.println("            <h3>Cluster Totals</h3>");
        writer.println("            <div class=\"summary-grid\">");
        writer.println("                <div class=\"summary-item\">");
        writer.println("                    <div class=\"summary-label\">Total Shards</div>");
        writer.println("                    <div class=\"summary-value\">" + shardTotals.size() + "</div>");
        writer.println("                </div>");
        writer.println("                <div class=\"summary-item\">");
        writer.println("                    <div class=\"summary-label\">Total Data Size</div>");
        writer.println("                    <div class=\"summary-value\">" + formatBytes(totalDataSize) + "</div>");
        writer.println("                </div>");
        writer.println("                <div class=\"summary-item\">");
        writer.println("                    <div class=\"summary-label\">Unsharded Data</div>");
        writer.println("                    <div class=\"summary-value\">" + formatBytes(totalUnshardedData) + "</div>");
        writer.println("                </div>");
        writer.println("                <div class=\"summary-item\">");
        writer.println("                    <div class=\"summary-label\">Sharded Data</div>");
        writer.println("                    <div class=\"summary-value\">" + formatBytes(totalShardedData) + "</div>");
        writer.println("                </div>");
        writer.println("                <div class=\"summary-item\">");
        writer.println("                    <div class=\"summary-label\">Total Storage Size</div>");
        writer.println("                    <div class=\"summary-value\">" + formatBytes(totalStorageSize) + "</div>");
        writer.println("                </div>");
        writer.println("                <div class=\"summary-item\">");
        writer.println("                    <div class=\"summary-label\">Total Index Size</div>");
        writer.println("                    <div class=\"summary-value\">" + formatBytes(totalIndexSize) + "</div>");
        writer.println("                </div>");
        writer.println("            </div>");
        writer.println("        </div>");

        writer.println("        <div class=\"table-container\">");
        writer.println("            <table id=\"shardSummaryTable\">");
        writer.println("                <thead>");
        writer.println("                    <tr>");
        writer.println("                        <th class=\"sortable\" onclick=\"sortTable('shardSummaryTable', 0, 'string')\">Shard</th>");
        writer.println("                        <th class=\"sortable\" onclick=\"sortTable('shardSummaryTable', 1, 'number')\">Data Size</th>");
        writer.println("                        <th class=\"sortable\" onclick=\"sortTable('shardSummaryTable', 2, 'number')\">Unsharded</th>");
        writer.println("                        <th class=\"sortable\" onclick=\"sortTable('shardSummaryTable', 3, 'number')\">Sharded</th>");
        writer.println("                        <th class=\"sortable\" onclick=\"sortTable('shardSummaryTable', 4, 'number')\">Storage Size</th>");
        writer.println("                        <th class=\"sortable\" onclick=\"sortTable('shardSummaryTable', 5, 'number')\">Index Size</th>");
        writer.println("                        <th class=\"sortable\" onclick=\"sortTable('shardSummaryTable', 6, 'number')\">Total Size</th>");
        writer.println("                    </tr>");
        writer.println("                </thead>");
        writer.println("                <tbody>");

        for (Map.Entry<String, long[]> entry : shardTotals.entrySet()) {
            long[] totals = entry.getValue();
            long totalSize = totals[1] + totals[2]; // storage + index

            writer.println("                    <tr>");
            writer.println("                        <td>" + escapeHtml(entry.getKey()) + "</td>");
            writer.println("                        <td class=\"number\">" + formatBytes(totals[0]) + "</td>");
            writer.println("                        <td class=\"number\">" + formatBytes(totals[3]) + "</td>");
            writer.println("                        <td class=\"number\">" + formatBytes(totals[4]) + "</td>");
            writer.println("                        <td class=\"number\">" + formatBytes(totals[1]) + "</td>");
            writer.println("                        <td class=\"number\">" + formatBytes(totals[2]) + "</td>");
            writer.println("                        <td class=\"number\">" + formatBytes(totalSize) + "</td>");
            writer.println("                    </tr>");
        }

        writer.println("                </tbody>");
        writer.println("            </table>");
        writer.println("        </div>");
    }

    private void writeDatabaseSummaryTable(PrintWriter writer, List<AnalysisResult> results) {
        writer.println("        <h2 id=\"database-summary\">Database Summary</h2>");

        // Check if any database has primary shard info
        boolean hasShardInfo = results.stream().anyMatch(r -> r.getPrimaryShard() != null);

        // Overall summary using db.stats()
        long totalCollections = results.stream()
                .filter(r -> r.getDatabaseStats() != null)
                .mapToLong(r -> r.getDatabaseStats().getCollections()).sum();
        long totalDocs = results.stream()
                .filter(r -> r.getDatabaseStats() != null)
                .mapToLong(r -> r.getDatabaseStats().getObjects()).sum();
        long totalSize = results.stream()
                .filter(r -> r.getDatabaseStats() != null)
                .mapToLong(r -> r.getDatabaseStats().getDataSize()).sum();
        long totalStorageSize = results.stream()
                .filter(r -> r.getDatabaseStats() != null)
                .mapToLong(r -> r.getDatabaseStats().getStorageSize()).sum();
        long totalIndexSize = results.stream()
                .filter(r -> r.getDatabaseStats() != null)
                .mapToLong(r -> r.getDatabaseStats().getIndexSize()).sum();

        writer.println("        <div class=\"summary\">");
        writer.println("            <h3>Overall Summary</h3>");
        writer.println("            <div class=\"summary-grid\">");
        writer.println("                <div class=\"summary-item\">");
        writer.println("                    <div class=\"summary-label\">Total Databases</div>");
        writer.println("                    <div class=\"summary-value\">" + results.size() + "</div>");
        writer.println("                </div>");
        writer.println("                <div class=\"summary-item\">");
        writer.println("                    <div class=\"summary-label\">Total Collections</div>");
        writer.println("                    <div class=\"summary-value\">" + totalCollections + "</div>");
        writer.println("                </div>");
        writer.println("                <div class=\"summary-item\">");
        writer.println("                    <div class=\"summary-label\">Total Documents</div>");
        writer.println("                    <div class=\"summary-value\">" + NUMBER_FORMAT.format(totalDocs) + "</div>");
        writer.println("                </div>");
        writer.println("                <div class=\"summary-item\">");
        writer.println("                    <div class=\"summary-label\">Total Data Size</div>");
        writer.println("                    <div class=\"summary-value\">" + formatBytes(totalSize) + "</div>");
        writer.println("                </div>");
        writer.println("                <div class=\"summary-item\">");
        writer.println("                    <div class=\"summary-label\">Total Storage Size</div>");
        writer.println("                    <div class=\"summary-value\">" + formatBytes(totalStorageSize) + "</div>");
        writer.println("                </div>");
        writer.println("                <div class=\"summary-item\">");
        writer.println("                    <div class=\"summary-label\">Total Index Size</div>");
        writer.println("                    <div class=\"summary-value\">" + formatBytes(totalIndexSize) + "</div>");
        writer.println("                </div>");
        writer.println("            </div>");
        writer.println("        </div>");

        writer.println("        <div class=\"table-container\">");
        writer.println("            <div class=\"controls\">");
        writer.println("                <input type=\"text\" id=\"dbSummaryFilter\" class=\"filter-input\" placeholder=\"Filter by database name...\">");
        writer.println("                <button class=\"clear-btn\" onclick=\"clearFilter('dbSummaryFilter', 'dbSummaryTable')\">Clear Filter</button>");
        writer.println("            </div>");
        writer.println("            <table id=\"dbSummaryTable\">");
        writer.println("                <thead>");
        writer.println("                    <tr>");
        writer.println("                        <th class=\"sortable\" onclick=\"sortTable('dbSummaryTable', 0, 'string')\">Database</th>");
        if (hasShardInfo) {
            writer.println("                        <th class=\"sortable\" onclick=\"sortTable('dbSummaryTable', 1, 'string')\">Primary Shard</th>");
            writer.println("                        <th class=\"sortable\" onclick=\"sortTable('dbSummaryTable', 2, 'number')\">Collections</th>");
            writer.println("                        <th class=\"sortable\" onclick=\"sortTable('dbSummaryTable', 3, 'number')\">Data Size</th>");
            writer.println("                        <th class=\"sortable\" onclick=\"sortTable('dbSummaryTable', 4, 'number')\">Storage Size</th>");
            writer.println("                        <th class=\"sortable\" onclick=\"sortTable('dbSummaryTable', 5, 'number')\">Index Size</th>");
            writer.println("                        <th class=\"sortable\" onclick=\"sortTable('dbSummaryTable', 6, 'number')\">Indexes</th>");
        } else {
            writer.println("                        <th class=\"sortable\" onclick=\"sortTable('dbSummaryTable', 1, 'number')\">Collections</th>");
            writer.println("                        <th class=\"sortable\" onclick=\"sortTable('dbSummaryTable', 2, 'number')\">Documents</th>");
            writer.println("                        <th class=\"sortable\" onclick=\"sortTable('dbSummaryTable', 3, 'number')\">Data Size</th>");
            writer.println("                        <th class=\"sortable\" onclick=\"sortTable('dbSummaryTable', 4, 'number')\">Storage Size</th>");
            writer.println("                        <th class=\"sortable\" onclick=\"sortTable('dbSummaryTable', 5, 'number')\">Index Size</th>");
            writer.println("                        <th class=\"sortable\" onclick=\"sortTable('dbSummaryTable', 6, 'number')\">Indexes</th>");
        }
        writer.println("                    </tr>");
        writer.println("                </thead>");
        writer.println("                <tbody>");

        for (AnalysisResult result : results) {
            DatabaseStats dbStats = result.getDatabaseStats();

            writer.println("                    <tr>");
            writer.println("                        <td>" + escapeHtml(result.getDatabaseName()) + "</td>");
            if (dbStats != null) {
                if (hasShardInfo) {
                    String primaryShard = result.getPrimaryShard() != null ? result.getPrimaryShard() : "-";
                    writer.println("                        <td>" + escapeHtml(primaryShard) + "</td>");
                }
                writer.println("                        <td class=\"number\">" + dbStats.getCollections() + "</td>");
                if (!hasShardInfo) {
                    writer.println("                        <td class=\"number\">" + NUMBER_FORMAT.format(dbStats.getObjects()) + "</td>");
                }
                writer.println("                        <td class=\"number\">" + formatBytes(dbStats.getDataSize()) + "</td>");
                writer.println("                        <td class=\"number\">" + formatBytes(dbStats.getStorageSize()) + "</td>");
                writer.println("                        <td class=\"number\">" + formatBytes(dbStats.getIndexSize()) + "</td>");
                writer.println("                        <td class=\"number\">" + dbStats.getIndexes() + "</td>");
            } else {
                // Fallback to collection stats
                List<CollectionStats> stats = result.getCollectionStats();
                if (hasShardInfo) {
                    String primaryShard = result.getPrimaryShard() != null ? result.getPrimaryShard() : "-";
                    writer.println("                        <td>" + escapeHtml(primaryShard) + "</td>");
                }
                writer.println("                        <td class=\"number\">" + stats.size() + "</td>");
                if (!hasShardInfo) {
                    long docs = stats.stream().mapToLong(CollectionStats::getCount).sum();
                    writer.println("                        <td class=\"number\">" + NUMBER_FORMAT.format(docs) + "</td>");
                }
                long dataSize = stats.stream().mapToLong(CollectionStats::getSize).sum();
                long storageSize = stats.stream().mapToLong(CollectionStats::getStorageSize).sum();
                long indexSize = stats.stream().mapToLong(CollectionStats::getTotalIndexSize).sum();
                int indexes = stats.stream().mapToInt(CollectionStats::getIndexes).sum();
                writer.println("                        <td class=\"number\">" + formatBytes(dataSize) + "</td>");
                writer.println("                        <td class=\"number\">" + formatBytes(storageSize) + "</td>");
                writer.println("                        <td class=\"number\">" + formatBytes(indexSize) + "</td>");
                writer.println("                        <td class=\"number\">" + indexes + "</td>");
            }
            writer.println("                    </tr>");
        }

        writer.println("                </tbody>");
        writer.println("            </table>");
        writer.println("        </div>");
    }

    private void writeShardDatabaseBreakdownTable(PrintWriter writer, List<AnalysisResult> results) {
        writer.println("        <h2 id=\"shard-db-breakdown\">Per-Shard Database Breakdown</h2>");

        // Collect all shard names
        java.util.Set<String> allShards = new java.util.TreeSet<>();
        for (AnalysisResult result : results) {
            DatabaseStats dbStats = result.getDatabaseStats();
            if (dbStats != null && dbStats.hasShardStats()) {
                allShards.addAll(dbStats.getShardStats().keySet());
            }
        }

        if (allShards.isEmpty()) {
            writer.println("        <p>No per-shard breakdown available.</p>");
            return;
        }

        // Build database summaries for sorting and display
        List<DatabaseShardSummary> dbSummaries = new ArrayList<>();
        for (AnalysisResult result : results) {
            DatabaseStats dbStats = result.getDatabaseStats();
            if (dbStats != null && dbStats.hasShardStats()) {
                DatabaseShardSummary summary = new DatabaseShardSummary();
                summary.databaseName = result.getDatabaseName();
                summary.primaryShard = result.getPrimaryShard();
                summary.shardStats = new ArrayList<>(dbStats.getShardStats().entrySet());

                // Calculate totals
                for (DatabaseStats.ShardDbStats s : dbStats.getShardStats().values()) {
                    summary.totalDataSize += s.getDataSize();
                    summary.totalStorageSize += s.getStorageSize();
                    summary.totalIndexSize += s.getIndexSize();
                }
                summary.totalSize = summary.totalDataSize + summary.totalIndexSize;
                summary.shardCount = dbStats.getShardStats().size();

                dbSummaries.add(summary);
            }
        }

        // Sort by total size descending
        dbSummaries.sort((a, b) -> Long.compare(b.totalSize, a.totalSize));

        writer.println("        <div class=\"summary\">");
        writer.println("            <h3>Database Distribution Analysis</h3>");
        writer.println("            <p>Databases sorted by total size. Databases on single shard are highlighted in <span style=\"background:#fff3e0;padding:2px 6px;\">orange</span>.</p>");
        writer.println("        </div>");

        writer.println("        <div class=\"table-container\">");
        writer.println("            <div class=\"controls\">");
        writer.println("                <input type=\"text\" id=\"shardDbFilter\" class=\"filter-input\" placeholder=\"Filter by database or shard...\">");
        writer.println("                <button class=\"clear-btn\" onclick=\"clearFilter('shardDbFilter', 'shardDbTable')\">Clear Filter</button>");
        writer.println("                <label style=\"margin-left:15px;\"><input type=\"checkbox\" id=\"showSingleShardOnly\" onchange=\"filterSingleShard()\"> Show single-shard databases only</label>");
        writer.println("            </div>");
        writer.println("            <table id=\"shardDbTable\">");
        writer.println("                <thead>");
        writer.println("                    <tr>");
        writer.println("                        <th class=\"sortable\" onclick=\"sortGroupedTable('shardDbTable', 0, 'string')\">Database</th>");
        writer.println("                        <th class=\"sortable\" onclick=\"sortGroupedTable('shardDbTable', 1, 'number')\">DB Total Size</th>");
        writer.println("                        <th>Shard</th>");
        writer.println("                        <th class=\"sortable\" onclick=\"sortGroupedTable('shardDbTable', 3, 'number')\">Data Size</th>");
        writer.println("                        <th class=\"sortable\" onclick=\"sortGroupedTable('shardDbTable', 4, 'number')\">Storage Size</th>");
        writer.println("                        <th class=\"sortable\" onclick=\"sortGroupedTable('shardDbTable', 5, 'number')\">Index Size</th>");
        writer.println("                        <th>% of DB</th>");
        writer.println("                        <th>Distribution</th>");
        writer.println("                    </tr>");
        writer.println("                </thead>");
        writer.println("                <tbody>");

        for (DatabaseShardSummary summary : dbSummaries) {
            boolean isSingleShard = summary.shardCount == 1;
            boolean isFirst = true;

            for (Map.Entry<String, DatabaseStats.ShardDbStats> entry : summary.shardStats) {
                DatabaseStats.ShardDbStats shardStats = entry.getValue();
                long shardTotal = shardStats.getStorageSize() + shardStats.getIndexSize();
                double pctOfDb = summary.totalSize > 0 ? (shardTotal * 100.0 / summary.totalSize) : 0;

                String rowClass = isFirst ? "shard-group-start" : "";
                if (isSingleShard) {
                    rowClass += " single-shard";
                }

                // Determine bar color based on percentage and shard count
                String barClass = "balanced";
                if (summary.shardCount > 1) {
                    if (pctOfDb > 60) barClass = "critical";
                    else if (pctOfDb > 40) barClass = "warning";
                }

                writer.println("                    <tr class=\"" + rowClass.trim() + "\" data-db=\"" + escapeHtml(summary.databaseName) + "\" data-db-total=\"" + summary.totalSize + "\" data-shard-count=\"" + summary.shardCount + "\">");
                writer.println("                        <td>" + (isFirst ? escapeHtml(summary.databaseName) : "") + "</td>");
                writer.println("                        <td class=\"number\">" + (isFirst ? formatBytes(summary.totalSize) : "") + "</td>");
                writer.println("                        <td>" + escapeHtml(entry.getKey()) + (entry.getKey().equals(summary.primaryShard) ? " ★" : "") + "</td>");
                writer.println("                        <td class=\"number\">" + formatBytes(shardStats.getDataSize()) + "</td>");
                writer.println("                        <td class=\"number\">" + formatBytes(shardStats.getStorageSize()) + "</td>");
                writer.println("                        <td class=\"number\">" + formatBytes(shardStats.getIndexSize()) + "</td>");
                writer.println("                        <td class=\"number\">" + String.format("%.1f%%", pctOfDb) + "</td>");
                writer.println("                        <td>");
                writer.println("                            <div class=\"distribution-bar\">");
                writer.println("                                <div class=\"distribution-bar-bg\">");
                writer.println("                                    <div class=\"distribution-bar-fill " + barClass + "\" style=\"width: " + Math.min(pctOfDb, 100) + "%;\"></div>");
                writer.println("                                </div>");
                writer.println("                            </div>");
                writer.println("                        </td>");
                writer.println("                    </tr>");
                isFirst = false;
            }
        }

        writer.println("                </tbody>");
        writer.println("            </table>");
        writer.println("        </div>");
    }

    private void writeImbalanceAnalysisSection(PrintWriter writer, List<AnalysisResult> results) {
        writer.println("        <h2 id=\"imbalance-analysis\">Shard Imbalance Analysis</h2>");

        // Calculate per-shard totals split by sharded vs unsharded COLLECTIONS
        // A database is "unsharded" if it has NO sharded collections
        Map<String, long[]> shardTotals = new java.util.TreeMap<>(); // [unshardedSize, shardedSize]
        Map<String, List<String>> unshardedDbsByShard = new java.util.TreeMap<>();

        long totalUnshardedSize = 0;
        long totalShardedSize = 0;
        int unshardedDbCount = 0;
        int unshardedCollCount = 0;
        int shardedCollCount = 0;

        for (AnalysisResult result : results) {
            String primaryShard = result.getPrimaryShard();
            List<CollectionStats> collections = result.getCollectionStats();

            // Check if this database has ANY sharded collections
            boolean hasShardedCollections = collections.stream().anyMatch(CollectionStats::isSharded);

            // Calculate unsharded vs sharded collection sizes
            long dbUnshardedSize = 0;
            long dbShardedSize = 0;

            for (CollectionStats coll : collections) {
                long collSize = coll.getSize() + coll.getTotalIndexSize();

                if (coll.isSharded()) {
                    // Sharded collection - data is distributed across shards
                    // Add to each shard's sharded total based on shard stats
                    shardedCollCount++;
                    if (coll.hasShardStats()) {
                        for (Map.Entry<String, ShardStats> shardEntry : coll.getShardStats().entrySet()) {
                            String shardName = shardEntry.getKey();
                            ShardStats ss = shardEntry.getValue();
                            long shardCollSize = ss.getSize();
                            long[] totals = shardTotals.computeIfAbsent(shardName, k -> new long[2]);
                            totals[1] += shardCollSize;
                            dbShardedSize += shardCollSize;
                        }
                    } else {
                        // No per-shard breakdown, add to sharded total
                        dbShardedSize += collSize;
                    }
                } else {
                    // Unsharded collection - lives on primary shard
                    unshardedCollCount++;
                    dbUnshardedSize += collSize;
                    if (primaryShard != null) {
                        long[] totals = shardTotals.computeIfAbsent(primaryShard, k -> new long[2]);
                        totals[0] += collSize;
                    }
                }
            }

            totalUnshardedSize += dbUnshardedSize;
            totalShardedSize += dbShardedSize;

            // Track databases with NO sharded collections
            if (!hasShardedCollections && primaryShard != null && dbUnshardedSize > 0) {
                unshardedDbCount++;
                unshardedDbsByShard.computeIfAbsent(primaryShard, k -> new ArrayList<>())
                    .add(result.getDatabaseName() + " (" + formatBytes(dbUnshardedSize) + ")");
            }
        }

        if (shardTotals.isEmpty()) {
            writer.println("        <p>No shard data available for imbalance analysis.</p>");
            return;
        }

        // Calculate average and identify imbalance
        long totalSize = totalUnshardedSize + totalShardedSize;
        long avgPerShard = totalSize / shardTotals.size();

        writer.println("        <div class=\"summary\">");
        writer.println("            <h3>Imbalance Overview</h3>");
        writer.println("            <p style=\"margin-bottom:15px;color:#666;\">Unsharded collections reside only on their database's primary shard, potentially causing imbalance.</p>");
        writer.println("            <div class=\"summary-grid\">");
        writer.println("                <div class=\"summary-item\">");
        writer.println("                    <div class=\"summary-label\">Unsharded Collections</div>");
        writer.println("                    <div class=\"summary-value\">" + NUMBER_FORMAT.format(unshardedCollCount) + "</div>");
        writer.println("                </div>");
        writer.println("                <div class=\"summary-item\">");
        writer.println("                    <div class=\"summary-label\">Sharded Collections</div>");
        writer.println("                    <div class=\"summary-value\">" + NUMBER_FORMAT.format(shardedCollCount) + "</div>");
        writer.println("                </div>");
        writer.println("                <div class=\"summary-item\">");
        writer.println("                    <div class=\"summary-label\">Unsharded Data (on primary shards)</div>");
        writer.println("                    <div class=\"summary-value\">" + formatBytes(totalUnshardedSize) + "</div>");
        writer.println("                </div>");
        writer.println("                <div class=\"summary-item\">");
        writer.println("                    <div class=\"summary-label\">Sharded Data (distributed)</div>");
        writer.println("                    <div class=\"summary-value\">" + formatBytes(totalShardedSize) + "</div>");
        writer.println("                </div>");
        writer.println("                <div class=\"summary-item\">");
        writer.println("                    <div class=\"summary-label\">DBs with NO Sharded Collections</div>");
        writer.println("                    <div class=\"summary-value\">" + unshardedDbCount + "</div>");
        writer.println("                </div>");
        writer.println("                <div class=\"summary-item\">");
        writer.println("                    <div class=\"summary-label\">Avg Size per Shard</div>");
        writer.println("                    <div class=\"summary-value\">" + formatBytes(avgPerShard) + "</div>");
        writer.println("                </div>");
        writer.println("            </div>");
        writer.println("        </div>");

        // Shard breakdown table showing unsharded vs sharded contribution
        writer.println("        <h3>Per-Shard Size Breakdown (Unsharded vs Sharded Collections)</h3>");
        writer.println("        <div class=\"table-container\">");
        writer.println("            <table id=\"imbalanceTable\">");
        writer.println("                <thead>");
        writer.println("                    <tr>");
        writer.println("                        <th class=\"sortable\" onclick=\"sortTable('imbalanceTable', 0, 'string')\">Shard</th>");
        writer.println("                        <th class=\"sortable\" onclick=\"sortTable('imbalanceTable', 1, 'number')\">Total Size</th>");
        writer.println("                        <th class=\"sortable\" onclick=\"sortTable('imbalanceTable', 2, 'number')\">Unsharded Data</th>");
        writer.println("                        <th class=\"sortable\" onclick=\"sortTable('imbalanceTable', 3, 'number')\">Sharded Data</th>");
        writer.println("                        <th class=\"sortable\" onclick=\"sortTable('imbalanceTable', 4, 'number')\">Delta from Avg</th>");
        writer.println("                        <th>Composition</th>");
        writer.println("                    </tr>");
        writer.println("                </thead>");
        writer.println("                <tbody>");

        for (Map.Entry<String, long[]> entry : shardTotals.entrySet()) {
            String shardName = entry.getKey();
            long unshardedSize = entry.getValue()[0];
            long shardedSize = entry.getValue()[1];
            long shardTotal = unshardedSize + shardedSize;
            long delta = shardTotal - avgPerShard;

            double unshardedPct = shardTotal > 0 ? (unshardedSize * 100.0 / shardTotal) : 0;
            double shardedPct = shardTotal > 0 ? (shardedSize * 100.0 / shardTotal) : 0;

            String deltaClass = delta > 0 ? "style=\"color:#d32f2f;\"" : "style=\"color:#388e3c;\"";
            String deltaSign = delta > 0 ? "+" : "";

            writer.println("                    <tr>");
            writer.println("                        <td>" + escapeHtml(shardName) + "</td>");
            writer.println("                        <td class=\"number\">" + formatBytes(shardTotal) + "</td>");
            writer.println("                        <td class=\"number\">" + formatBytes(unshardedSize) + "</td>");
            writer.println("                        <td class=\"number\">" + formatBytes(shardedSize) + "</td>");
            writer.println("                        <td class=\"number\" " + deltaClass + ">" + deltaSign + formatBytes(delta) + "</td>");
            writer.println("                        <td>");
            writer.println("                            <div class=\"stacked-bar\">");
            writer.println("                                <div class=\"stacked-bar-segment unsharded\" style=\"width:" + unshardedPct + "%;\" title=\"Single-shard: " + String.format("%.1f%%", unshardedPct) + "\"></div>");
            writer.println("                                <div class=\"stacked-bar-segment sharded\" style=\"width:" + shardedPct + "%;\" title=\"Multi-shard: " + String.format("%.1f%%", shardedPct) + "\"></div>");
            writer.println("                            </div>");
            writer.println("                        </td>");
            writer.println("                    </tr>");
        }

        writer.println("                </tbody>");
        writer.println("            </table>");
        writer.println("            <div style=\"margin-top:10px;font-size:12px;\">");
        writer.println("                <span style=\"display:inline-block;width:12px;height:12px;background:#ff9800;margin-right:4px;\"></span> Single-shard database data");
        writer.println("                <span style=\"display:inline-block;width:12px;height:12px;background:#2196f3;margin-left:15px;margin-right:4px;\"></span> Multi-shard (distributed) data");
        writer.println("            </div>");
        writer.println("        </div>");

        // Top databases with unsharded data by size
        writer.println("        <h3>Top Databases with Unsharded Data</h3>");
        writer.println("        <p>These databases have unsharded collections on their primary shard. Consider sharding large collections for better distribution.</p>");

        // Collect unique primary shards for filter dropdown
        java.util.Set<String> primaryShards = new java.util.TreeSet<>();

        // Collect and sort databases by unsharded data size
        List<Object[]> dbsWithUnshardedData = new ArrayList<>();
        for (AnalysisResult result : results) {
            String primaryShard = result.getPrimaryShard();
            if (primaryShard == null) continue;
            primaryShards.add(primaryShard);

            List<CollectionStats> collections = result.getCollectionStats();
            long unshardedSize = collections.stream()
                .filter(c -> !c.isSharded())
                .mapToLong(c -> c.getSize() + c.getTotalIndexSize())
                .sum();
            long shardedSize = collections.stream()
                .filter(CollectionStats::isSharded)
                .mapToLong(c -> c.getSize() + c.getTotalIndexSize())
                .sum();
            int unshardedCollections = (int) collections.stream().filter(c -> !c.isSharded()).count();
            int shardedCollections = (int) collections.stream().filter(CollectionStats::isSharded).count();

            if (unshardedSize > 0) {
                dbsWithUnshardedData.add(new Object[]{
                    result.getDatabaseName(),
                    primaryShard,
                    unshardedSize,
                    shardedSize,
                    unshardedCollections,
                    shardedCollections
                });
            }
        }
        dbsWithUnshardedData.sort((a, b) -> Long.compare((Long)b[2], (Long)a[2]));

        writer.println("        <div class=\"table-container\">");
        writer.println("            <div class=\"controls\">");
        writer.println("                <label>Filter by Primary Shard: ");
        writer.println("                    <select id=\"shardFilter\" onchange=\"filterByPrimaryShard()\">");
        writer.println("                        <option value=\"\">All Shards</option>");
        for (String shard : primaryShards) {
            writer.println("                        <option value=\"" + escapeHtml(shard) + "\">" + escapeHtml(shard) + "</option>");
        }
        writer.println("                    </select>");
        writer.println("                </label>");
        writer.println("                <input type=\"text\" id=\"topUnshardedFilter\" class=\"filter-input\" placeholder=\"Filter by database...\" style=\"margin-left:15px;\">");
        writer.println("            </div>");
        writer.println("            <table id=\"topUnshardedTable\">");
        writer.println("                <thead>");
        writer.println("                    <tr>");
        writer.println("                        <th class=\"sortable\" onclick=\"sortTable('topUnshardedTable', 0, 'string')\">Database</th>");
        writer.println("                        <th class=\"sortable\" onclick=\"sortTable('topUnshardedTable', 1, 'string')\">Primary Shard</th>");
        writer.println("                        <th class=\"sortable\" onclick=\"sortTable('topUnshardedTable', 2, 'number')\">Unsharded Data</th>");
        writer.println("                        <th class=\"sortable\" onclick=\"sortTable('topUnshardedTable', 3, 'number')\">Sharded Data</th>");
        writer.println("                        <th class=\"sortable\" onclick=\"sortTable('topUnshardedTable', 4, 'number')\">Unsharded Colls</th>");
        writer.println("                        <th class=\"sortable\" onclick=\"sortTable('topUnshardedTable', 5, 'number')\">Sharded Colls</th>");
        writer.println("                    </tr>");
        writer.println("                </thead>");
        writer.println("                <tbody>");

        for (Object[] db : dbsWithUnshardedData) {
            String shard = (String)db[1];
            writer.println("                    <tr data-shard=\"" + escapeHtml(shard) + "\">");
            writer.println("                        <td>" + escapeHtml((String)db[0]) + "</td>");
            writer.println("                        <td>" + escapeHtml(shard) + "</td>");
            writer.println("                        <td class=\"number\">" + formatBytes((Long)db[2]) + "</td>");
            writer.println("                        <td class=\"number\">" + formatBytes((Long)db[3]) + "</td>");
            writer.println("                        <td class=\"number\">" + db[4] + "</td>");
            writer.println("                        <td class=\"number\">" + db[5] + "</td>");
            writer.println("                    </tr>");
        }

        writer.println("                </tbody>");
        writer.println("            </table>");
        writer.println("        </div>");
    }

    // Helper class for database shard summary
    private static class DatabaseShardSummary {
        String databaseName;
        String primaryShard;
        List<Map.Entry<String, DatabaseStats.ShardDbStats>> shardStats;
        long totalDataSize;
        long totalStorageSize;
        long totalIndexSize;
        long totalSize;
        int shardCount;
    }
}