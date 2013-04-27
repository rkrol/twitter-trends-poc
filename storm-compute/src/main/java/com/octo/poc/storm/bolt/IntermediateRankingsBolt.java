package com.octo.poc.storm.bolt;

import org.apache.log4j.Logger;

import backtype.storm.topology.BasicOutputCollector;
import backtype.storm.tuple.Tuple;
import backtype.storm.tuple.Values;

import com.google.common.collect.ImmutableList;
import com.octo.poc.storm.tools.Rankable;
import com.octo.poc.storm.tools.RankableObjectWithFields;
import com.octo.poc.storm.tools.Rankings;

/**
 * This bolt ranks incoming objects by their count.
 * 
 * It assumes the input tuples to adhere to the following format: (object, object_count, additionalField1,
 * additionalField2, ..., additionalFieldN).
 * 
 */
public final class IntermediateRankingsBolt extends AbstractRankerBolt {

    private static final long serialVersionUID = -1369800530256637409L;
    private static final Logger LOG = Logger.getLogger(IntermediateRankingsBolt.class);

    public IntermediateRankingsBolt() {
        super();
    }

    public IntermediateRankingsBolt(int topN) {
        super(topN);
    }

    public IntermediateRankingsBolt(int topN, int emitFrequencyInSeconds) {
        super(topN, emitFrequencyInSeconds);
    }

    @Override
    void updateRankingsWithTuple(Tuple tuple) {
        Rankable rankable = RankableObjectWithFields.from(tuple);
        super.getRankings().updateWith(rankable);
    }

    @Override
    Logger getLogger() {
        return LOG;
    }
    
    @Override
    public void emitRankings(BasicOutputCollector collector) {
    	collector.emit(new Values(getClonedRankings()));
    	
    }
    
    private Rankings getClonedRankings() {
    	Rankings rankings = getRankings();
    	Rankings clonedRankings = new Rankings(rankings.maxSize(), rankings.getRankings());
    	//for (Rankable rankable : rankings.getRankings()) {
    	//	Rankable clonedRankable = new RankableObjectWithFields(rankable.getObject(), rankable.getCount(), ImmutableList.copyOf(rankable.getFields()));
    	//	clonedRankings.getRankings().add(clonedRankable);
		//}
    	
    	return clonedRankings;
    }
}
