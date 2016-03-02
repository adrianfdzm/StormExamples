package es.afm.storm.examples.redis;

import org.apache.storm.redis.bolt.RedisStoreBolt;
import org.apache.storm.redis.common.config.JedisPoolConfig;

import es.afm.storm.examples.bolts.NormalizeBolt;
import es.afm.storm.examples.bolts.SentenceSplitBolt;
import es.afm.storm.examples.bolts.WordCountBolt;
import es.afm.storm.examples.spouts.FileReaderSpout;
import backtype.storm.Config;
import backtype.storm.LocalCluster;
import backtype.storm.topology.TopologyBuilder;
import backtype.storm.tuple.Fields;

public class RedisWordCount {

	private void run() {
		Config conf = new Config();
		conf.setNumWorkers(2);

		JedisPoolConfig jedisConf = new JedisPoolConfig("localhost", 6379, 0,
				null, 0);
		TopologyBuilder builder = new TopologyBuilder();
		builder.setSpout("file", new FileReaderSpout());
		builder.setBolt("normalize", new NormalizeBolt()).shuffleGrouping(
				"file");
		builder.setBolt("split", new SentenceSplitBolt()).shuffleGrouping(
				"normalize");
		builder.setBolt("wordcount", new WordCountBolt()).fieldsGrouping(
				"split", new Fields("word"));
		builder.setBolt("wordcount-store",
				new RedisStoreBolt(jedisConf, new RedisWordCountMapper()))
				.fieldsGrouping("wordcount", new Fields("word"));

		LocalCluster cluster = new LocalCluster();
		cluster.submitTopology("builder", conf, builder.createTopology());
	}

	public static void main(String[] args) {
		new RedisWordCount().run();
	}

}
