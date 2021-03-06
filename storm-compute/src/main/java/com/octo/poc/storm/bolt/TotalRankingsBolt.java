package com.octo.poc.storm.bolt;

import java.util.Map;

import org.apache.log4j.Logger;
import org.codehaus.jackson.map.ObjectMapper;
import org.zeromq.ZMQ;

import backtype.storm.task.TopologyContext;
import backtype.storm.topology.BasicOutputCollector;
import backtype.storm.topology.OutputFieldsDeclarer;
import backtype.storm.tuple.Tuple;

import com.octo.poc.storm.tools.Rankings;

/**
 * This bolt merges incoming {@link Rankings}.
 * 
 * It can be used to merge intermediate rankings generated by {@link IntermediateRankingsBolt} into a final,
 * consolidated ranking. To do so, configure this bolt with a globalGrouping on {@link IntermediateRankingsBolt}.
 * 
 */
public final class TotalRankingsBolt extends AbstractRankerBolt {

    private static final long serialVersionUID = -8447525895532302198L;
    private static final Logger LOG = Logger.getLogger(TotalRankingsBolt.class);
	private ZMQ.Socket sender;
	private ZMQ.Context context;

    public TotalRankingsBolt() {
        super();
    }

    public TotalRankingsBolt(int topN) {
        super(topN);
    }

    public TotalRankingsBolt(int topN, int emitFrequencyInSeconds) {
        super(topN, emitFrequencyInSeconds);
    }
    
    @Override
    public void prepare(Map conf, TopologyContext ctx) {
    	context = ZMQ.context(1);
    	sender = context.socket(ZMQ.PUSH);
    	sender.connect("tcp://127.0.0.1:4777");
    }

    @Override
    void updateRankingsWithTuple(Tuple tuple) {
        Rankings rankingsToBeMerged = (Rankings) tuple.getValue(0);
        super.getRankings().updateWith(rankingsToBeMerged);
    }

    @Override
    Logger getLogger() {
        return LOG;
    }

    @Override
    public void declareOutputFields(OutputFieldsDeclarer ofd) {
    }
    
    @Override
    public void emitRankings(BasicOutputCollector collector) {
    	ObjectMapper objectMapper = new ObjectMapper();
    	String json = "";
        try {
        	json = objectMapper.writeValueAsString(getRankings());
        }
        catch(Exception e) {
            e.printStackTrace();
        }
    	sender.send(json.getBytes(), 0);
        getLogger().info("!!!Rankings!!! " + getRankings());
    }
}
