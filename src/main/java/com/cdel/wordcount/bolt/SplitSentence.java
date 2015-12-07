package com.cdel.wordcount.bolt;

import java.util.Map;

import backtype.storm.task.OutputCollector;
import backtype.storm.task.TopologyContext;
import backtype.storm.topology.IRichBolt;
import backtype.storm.topology.OutputFieldsDeclarer;
import backtype.storm.tuple.Fields;
import backtype.storm.tuple.Tuple;
import backtype.storm.tuple.Values;

@SuppressWarnings("serial")
public class SplitSentence implements IRichBolt {

	OutputCollector _collector;

	public void declareOutputFields(OutputFieldsDeclarer declarer) {
		declarer.declare(new Fields("word"));
	}

	public Map<String, Object> getComponentConfiguration() {
		return null;
	}

	@SuppressWarnings("rawtypes")
	public void prepare(Map map, TopologyContext topologycontext, OutputCollector collector) {
		_collector = collector;
	}

	public void execute(Tuple tuple) {
		String[] words = tuple.getString(0).split(" ");
		for (String word : words) {
			_collector.emit(tuple, new Values(word));
		}
		_collector.ack(tuple);
	}

	public void cleanup() {
	}

}
