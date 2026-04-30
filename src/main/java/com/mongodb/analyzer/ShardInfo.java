package com.mongodb.analyzer;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.stream.Collectors;

public class ShardInfo {
    private final String shardName;
    private final String hostString;
    private List<String> zones = new ArrayList<>();

    public ShardInfo(String shardName, String hostString) {
        this.shardName = shardName;
        this.hostString = hostString;
    }

    public String getShardName() {
        return shardName;
    }

    public String getHostString() {
        return hostString;
    }

    public List<String> getZones() { return zones; }
    public void setZones(List<String> zones) { this.zones = zones; }

    // Parses "rsName/host1:port,host2:port" or plain "host:port,host2:port"
    public List<String> getHosts() {
        String hosts = hostString.contains("/")
            ? hostString.substring(hostString.indexOf('/') + 1)
            : hostString;
        return Arrays.stream(hosts.split(","))
            .map(String::trim)
            .filter(h -> !h.isEmpty())
            .collect(Collectors.toList());
    }

    @Override
    public String toString() {
        return shardName + " (" + hostString + ")";
    }
}
