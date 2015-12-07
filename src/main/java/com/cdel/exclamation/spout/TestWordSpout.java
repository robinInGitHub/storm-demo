package com.cdel.exclamation.spout;

import backtype.storm.spout.SpoutOutputCollector;
import backtype.storm.task.TopologyContext;
import backtype.storm.topology.OutputFieldsDeclarer;
import backtype.storm.topology.base.BaseRichSpout;
import backtype.storm.tuple.Fields;
import backtype.storm.tuple.Values;
import backtype.storm.utils.Utils;

import java.util.Map;
import java.util.Random;

/**
 * Spout负责发射新的tuple到这个topology里面来。
 * TestWordSpout从["nathan", "mike", "jackson", "golda", "bertels"]里面随机选择一个单词发射出来。
 *
 * Spout：在一个topology中产生源数据流的组件。通常情况下spout会从外部数据源中读取数据，然后转换为topology内部的源数据。
 * Spout是一个主动的角色，其接口中有个nextTuple()函数，storm框架会不停地调用此函数，用户只要在其中生成源数据即可。
 *
 */
@SuppressWarnings("serial")
public class TestWordSpout extends BaseRichSpout {
	SpoutOutputCollector _collector;
	Random _rand;

	@SuppressWarnings("rawtypes")
	public void open(Map map, TopologyContext topologycontext, SpoutOutputCollector spoutoutputcollector) {
		_collector = spoutoutputcollector;
	    _rand = new Random();
	}

	public void nextTuple() {
		Utils.sleep(100);
	    final String[] words =new String[] {"nathan", "mike", "jackson", "golda", "bertels"};
	    final Random rand =new Random();
	    final String word = words[rand.nextInt(words.length)];
	    _collector.emit(new Values(word));
	}

	public void declareOutputFields(OutputFieldsDeclarer declarer) {
		declarer.declare(new Fields("word"));
	}

	@Override
	public void ack(Object id) {
	}

	@Override
	public void fail(Object id) {
	}

}