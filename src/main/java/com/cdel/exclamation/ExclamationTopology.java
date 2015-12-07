package com.cdel.exclamation;

import backtype.storm.Config;
import backtype.storm.LocalCluster;
import backtype.storm.StormSubmitter;
import backtype.storm.topology.TopologyBuilder;
import com.cdel.exclamation.bolt.ExclamationBolt;
import com.cdel.exclamation.spout.TestWordSpout;

/**
 * This topology demonstrates Storm's stream groupings and multilang
 * capabilities.
 */
public class ExclamationTopology {

	public static void main(String[] args) throws Exception {
		//这个Topology包含一个Spout和两个Bolt。
		TopologyBuilder builder = new TopologyBuilder();

		//使用setSpout和setBolt来定义Topology里面的节点。这些方法接收我们指定的一个id， 一个包含处理逻辑的对象(spout或者bolt), 以及你所需要的并行度。
		//这个包含处理的对象如果是spout那么要实现IRichSpout的接口， 如果是bolt，那么就要实现IRichBolt接口.
		//最后一个指定并行度的参数是可选的。它表示集群里面需要多少个thread来一起执行这个节点。如果你忽略它那么storm会分配一个线程来执行这个节点。
		builder.setSpout("spout", new TestWordSpout(), 5);
		//setBolt方法返回一个InputDeclarer对象， 这个对象是用来定义Bolt的输入。
		//第一个Bolt声明它要读取spout所发射的所有的tuple — 使用shuffle grouping。
		//shuffle grouping表示所有的tuple会被随机的分发给bolt的所有task。
		builder.setBolt("add", new ExclamationBolt(), 8).shuffleGrouping("spout");
		//第二个bolt声明它读取第一个bolt所发射的tuple。
		//如果你想第二个bolt读取spout和第一个bolt所发射的所有的tuple， 那么你应该这样定义第二个bolt:
		//builder.setBolt(3,new ExclamationBolt(),5).shuffleGrouping(1).shuffleGrouping(2);
		builder.setBolt("add2", new ExclamationBolt(), 12).shuffleGrouping("add");

		Config conf = new Config();
		conf.setDebug(true);

		if (args != null && args.length > 0) {
			//定义你希望集群分配多少个工作进程给你来执行这个topology. topology里面的每个组件会被需要线程来执行。每个组件到底用多少个线程是通过setBolt和setSpout来指定的。这些线程都运行在工作进程里面. 每一个工作进程包含一些节点的一些工作线程。比如， 如果你指定300个线程，60个进程， 那么每个工作进程里面要执行6个线程， 而这6个线程可能属于不同的组件(Spout, Bolt)。你可以通过调整每个组件的并行度以及这些线程所在的进程数量来调整topology的性能。
			conf.setNumWorkers(3);

			StormSubmitter.submitTopologyWithProgressBar(args[0], conf, builder.createTopology());
		} else {
			conf.setMaxTaskParallelism(3);
			//定义一个LocalCluster对象来定义一个进程内的集群
			LocalCluster cluster = new LocalCluster();
			//提交topology给这个虚拟的集群
			//三个参数：要运行的topology的名字，一个配置对象conf以及要运行的topology本身
			cluster.submitTopology("word-count", conf, builder.createTopology());

			Thread.sleep(10000);
			//topology的名字是用来唯一区别一个topology的，可以用这个名字来杀死这个topology的。你必须显式的杀掉一个topology， 否则它会一直运行。
			cluster.shutdown();
		}
	}

}
