package com.octo.poc.storm;

import backtype.storm.Config;
import backtype.storm.topology.TopologyBuilder;
import backtype.storm.tuple.Fields;

import com.octo.poc.storm.bolt.ExtractHashtagsBolt;
import com.octo.poc.storm.bolt.ExtractMessageBolt;
import com.octo.poc.storm.bolt.IntermediateRankingsBolt;
import com.octo.poc.storm.bolt.RollingCountBolt;
import com.octo.poc.storm.bolt.TotalRankingsBolt;
import com.octo.poc.storm.spout.TwitterSampleSpout;
import com.octo.poc.storm.util.StormRunner;

/**
 * This topology does a continuous computation of the top N words that the topology has seen in terms of cardinality.
 * The top N computation is done in a completely scalable way, and a similar approach could be used to compute things
 * like trending topics or trending images on Twitter.
 */
public class RollingTopWords {

    private static final int DEFAULT_RUNTIME_IN_SECONDS = 600;
    private static final int TOP_N = 10;

    private final TopologyBuilder builder;
    private final String topologyName;
    private final Config topologyConfig;
    private final int runtimeInSeconds;
    private final String username;
    private final String password;

    public RollingTopWords(String _username, String _password) throws InterruptedException {
        builder = new TopologyBuilder();
        topologyName = "slidingWindowCounts";
        topologyConfig = createTopologyConfiguration();
        runtimeInSeconds = DEFAULT_RUNTIME_IN_SECONDS;
        username = _username;
        password = _password;

        wireTopology();
    }

    private static Config createTopologyConfiguration() {
        Config conf = new Config();
        conf.setDebug(true);
        return conf;
    }

    private void wireTopology() throws InterruptedException {
        String twitterSpoutId = "twitterSpout";
        String messageExtractorId = "messageExtractor";
        String hashtagsExtractorId = "hashtagsExtractor";
        String counterId = "counter";
        String intermediateRankerId = "intermediateRanker";
        String totalRankerId = "totalRanker";
        builder.setSpout(twitterSpoutId, new TwitterSampleSpout(username, password));
        builder.setBolt(messageExtractorId, new ExtractMessageBolt(), 4).shuffleGrouping(twitterSpoutId);
        builder.setBolt(hashtagsExtractorId, new ExtractHashtagsBolt(), 4).shuffleGrouping(messageExtractorId);
        builder.setBolt(counterId, new RollingCountBolt(300, 1), 4).fieldsGrouping(hashtagsExtractorId, new Fields("hashtag"));
        builder.setBolt(intermediateRankerId, new IntermediateRankingsBolt(TOP_N), 4).fieldsGrouping(counterId,
            new Fields("obj"));
        builder.setBolt(totalRankerId, new TotalRankingsBolt(TOP_N)).globalGrouping(intermediateRankerId);
    }

    public void run() throws InterruptedException {
        StormRunner.runTopologyLocally(builder.createTopology(), topologyName, topologyConfig, runtimeInSeconds);
    }

    public static void main(String[] args) throws Exception {
        String username = args[0];
        String password = args[1];
        new RollingTopWords(username, password).run();
    }
}
