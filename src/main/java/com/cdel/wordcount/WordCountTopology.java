package com.cdel.wordcount;

import backtype.storm.Config;
import backtype.storm.LocalCluster;
import backtype.storm.StormSubmitter;
import backtype.storm.topology.TopologyBuilder;
import backtype.storm.tuple.Fields;
import com.cdel.wordcount.bolt.SplitSentence;
import com.cdel.wordcount.bolt.WordCount;
import com.cdel.wordcount.spout.RandomSentenceSpout;

//http://www.aboutyun.com/thread-7394-1-1.html
//http://www.csdn.net/article/2012-12-24/2813117-storm-realtime-big-data-analysis
public class WordCountTopology {

  public static void main(String[] args) throws Exception {

    TopologyBuilder builder = new TopologyBuilder();

    builder.setSpout("spout", new RandomSentenceSpout(), 5);

    /**
     * Storm的Grouping即消息的Partition机制。当一个Tuple被发送时，如何确定将它发送个某个（些）Task来处理？？
	 ShuffleGrouping：随机选择一个Task来发送。
	 FiledGrouping：根据Tuple中Fields来做一致性hash，相同hash值的Tuple被发送到相同的Task。
	 AllGrouping：广播发送，将每一个Tuple发送到所有的Task。
	 GlobalGrouping：所有的Tuple会被发送到某个Bolt中的id最小的那个Task。
	 NoneGrouping：不关心Tuple发送给哪个Task来处理，等价于ShuffleGrouping。
	 DirectGrouping：直接将Tuple发送到指定的Task来处理。
     */
    builder.setBolt("split", new SplitSentence(), 8).shuffleGrouping("spout");
    builder.setBolt("count", new WordCount(), 12).fieldsGrouping("split", new Fields("word"));

    Config conf = new Config();
    conf.setDebug(true);


    if (args != null && args.length > 0) {
      conf.setNumWorkers(3);

      StormSubmitter.submitTopologyWithProgressBar(args[0], conf, builder.createTopology());
    }
    else {
      conf.setMaxTaskParallelism(3);

      LocalCluster cluster = new LocalCluster();
      cluster.submitTopology("word-count", conf, builder.createTopology());

      Thread.sleep(10000);

      cluster.shutdown();
    }
  }
}
