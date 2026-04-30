# mongo-analyzer

Java CLI that deep-analyzes MongoDB deployments by connecting directly to every mongod in a sharded cluster or replica set. Collects per-collection WiredTiger cache stats, index usage, storage metrics, zone-aware shard imbalance detection, and chunk distribution analysis. Outputs to console table, JSON, or interactive HTML.

## Features

### Sharded Cluster Analysis
- Detects mongos automatically via `isdbgrid`
- Reads all shards from `config.shards` and direct-connects to every mongod (primaries and secondaries)
- Collects `collStats`, `$indexStats`, and `serverStatus` per host
- Builds a per-namespace × per-shard storage distribution matrix
- **Zone-aware imbalance detection**: groups shards by zone before comparing — only shards in the same zone are compared against each other
- **Data imbalance** (WARNING ≥60%, CRITICAL ≥75% of namespace storage on one shard)
- **Chunk imbalance** from `config.chunks` (version-agnostic: pre-5.0 `ns` field and 5.0+ UUID join)
- **Activity imbalance** from `$indexStats` ops aggregated across all replica set members per shard
- Executive summary with cluster-wide totals

### WiredTiger Cache Stats
- Per-collection `bytes read into cache` and `bytes written from cache` from `collStats.wiredTiger.cache`
- Per-host server-level cache stats from `serverStatus.wiredTiger.cache`
- Cache utilization percentage per host

### Index Analysis
- Combines `listIndexes`, `$indexStats`, and `collStats.indexDetails`
- Per-index size, type (unique, sparse, partial, TTL), and usage count
- TTL expiration settings in human-readable form
- Index ops aggregated across all replica set members (primary + secondaries)
- Color-coded unused indexes in HTML output

### Output Formats
- **Table**: console-friendly FlipTables output, namespace distribution capped at 100 rows (imbalanced first)
- **HTML**: interactive report — sortable/filterable tables, "imbalanced only" toggle, color-coded severity cells
- **JSON**: full structured output for programmatic use

### Standalone / Replica Set
- Falls back to standard `$_internalAllCollectionStats` analysis when connected to a non-mongos endpoint
- Collection and index stats, database-level stats, primary shard mapping

## Requirements

- Java 11+
- Maven 3.6+
- MongoDB 3.6+ (for `$indexStats`)
- MongoDB 4.4+ for `$_internalAllCollectionStats` (standalone/replica set path)

## Building

```bash
mvn clean package
```

Produces a shaded JAR at `target/mongo-analyzer-1.0-SNAPSHOT.jar`.

## Usage

```bash
java -jar target/mongo-analyzer-1.0-SNAPSHOT.jar [OPTIONS]
```

### Options

| Option | Default | Description |
|--------|---------|-------------|
| `-c, --connection` | `mongodb://localhost:27017` | MongoDB connection string |
| `-d, --database` | *(all)* | Restrict analysis to a single database |
| `-o, --output` | `table` | Output format: `table`, `json`, `html` |
| `--include-collections` | | Comma-separated collection whitelist |
| `--exclude-collections` | | Comma-separated collection blacklist |
| `--exclude-databases` | | Comma-separated database blacklist |
| `--stats-only` | | Skip index statistics |
| `--index-only` | | Skip collection statistics |

### Examples

```bash
# Analyze a local standalone or replica set (all databases, table output)
java -jar target/mongo-analyzer-1.0-SNAPSHOT.jar

# Connect to a sharded cluster via mongos and generate an HTML report
java -jar target/mongo-analyzer-1.0-SNAPSHOT.jar \
  -c "mongodb://mongos-host:27017" -o html

# Atlas cluster — full HTML report for all databases
java -jar target/mongo-analyzer-1.0-SNAPSHOT.jar \
  -c "mongodb+srv://user:pass@cluster.mongodb.net/" -o html

# Analyze one database with table output
java -jar target/mongo-analyzer-1.0-SNAPSHOT.jar -d mydb

# JSON output for all databases, excluding system DBs
java -jar target/mongo-analyzer-1.0-SNAPSHOT.jar \
  -o json --exclude-databases "admin,config,local"

# Only collection stats for specific collections
java -jar target/mongo-analyzer-1.0-SNAPSHOT.jar \
  -d mydb --include-collections "orders,users" --stats-only
```

### Installation (optional)

```bash
./install.sh
```

Installs a `mongo-analyzer` wrapper script to `~/.local/bin` pointing at the built JAR.

## Architecture

| File | Responsibility |
|------|---------------|
| `MongoAnalyzer.java` | CLI entry point, topology detection, orchestration |
| `ShardedClusterClient.java` | Shard enumeration, direct connections, chunk/zone metadata |
| `DatabaseAnalyzer.java` | Per-database stats collection (collStats, indexStats) |
| `ClusterAnalysisReport.java` | Aggregates all host results; computes imbalance metrics |
| `CollectionStats.java` | Collection-level data model including WT cache fields |
| `IndexStats.java` | Index-level data model with per-member op counts |
| `WiredTigerStats.java` | Server-level WiredTiger cache stats |
| `HostAnalysisResult.java` | Per-host container (shard name, role, DB results, WT stats) |
| `ChunkDistributionStats.java` | Per-namespace chunk counts per shard |
| `TableOutputFormatter.java` | Console table rendering |
| `HtmlOutputFormatter.java` | Interactive HTML report generation |
| `JsonOutputFormatter.java` | JSON serialization |

## Sharded Cluster Report Structure

When connected to a mongos the tool produces multiple analysis sections:

1. **Executive Summary** — shard count, host count, cluster-wide data/index/ops totals
2. **WiredTiger Cache (per host)** — bytes read/written, cache utilization %
3. **Shard Totals** — storage, index size, document count, ops, and zone assignment per shard
4. **Chunk Distribution** — chunk count per shard per namespace; flags imbalanced namespaces
5. **Namespace / Shard Distribution** — one row per namespace with per-shard storage size and %, sorted by imbalance severity then total size; HTML includes "imbalanced only" filter
6. **Activity (Ops) by Shard** — index operation counts aggregated across all hosts per shard
7. **Per-Host Detail** — collection stats and index stats for each individual mongod
