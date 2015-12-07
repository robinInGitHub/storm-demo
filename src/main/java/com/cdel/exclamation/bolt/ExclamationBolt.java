package com.cdel.exclamation.bolt;

import java.util.Map;
import backtype.storm.task.OutputCollector;
import backtype.storm.task.TopologyContext;
import backtype.storm.topology.IRichBolt;
import backtype.storm.topology.OutputFieldsDeclarer;
import backtype.storm.tuple.Fields;
import backtype.storm.tuple.Tuple;
import backtype.storm.tuple.Values;

/**
 * Bolt：在一个topology中接受数据然后执行处理的组件。Bolt可以执行过滤、函数操作、合并、写数据库等任何操作。
 * Bolt是一个被动的角色，其接口中有个execute(Tuple input)函数,在接受到消息后会调用此函数，用户可以在其中执行自己想要的操作。
 *
 */
@SuppressWarnings("serial")
public class ExclamationBolt implements IRichBolt {

	OutputCollector _collector;

	@SuppressWarnings("rawtypes")
	//prepare方法提供给bolt一个Outputcollector用来发射tuple。Bolt可以在任何时候发射tuple — 在prepare, execute或者cleanup方法里面, 或者甚至在另一个线程里面异步发射。这里prepare方法只是简单地把OutputCollector作为一个类字段保存下来给后面execute方法使用。
	public void prepare(Map conf, TopologyContext context, OutputCollector collector) {
		_collector = collector;
	}

	//execute方法从bolt的一个输入接收tuple(一个bolt可能有多个输入源). ExclamationBolt获取tuple的第一个字段，加上”!!!”之后再发射出去。如果一个bolt有多个输入源，你可以通过调用Tuple#getSourceComponent方法来知道它是来自哪个输入源的。
	public void execute(Tuple tuple) {
		_collector.emit(tuple, new Values(tuple.getString(0) + "!!!"));
		_collector.ack(tuple);
	}

	public void cleanup() {
	}

	//定义一个叫做”word”的字段的tuple。
	public void declareOutputFields(OutputFieldsDeclarer declarer) {
		declarer.declare(new Fields("word"));
	}

	public Map<String, Object> getComponentConfiguration() {
		// TODO Auto-generated method stub
		return null;
	}

}
