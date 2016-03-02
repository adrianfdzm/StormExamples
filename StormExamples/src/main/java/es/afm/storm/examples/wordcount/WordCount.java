package es.afm.storm.examples.wordcount;

import es.afm.storm.examples.bolts.NormalizeBolt;
import es.afm.storm.examples.bolts.SentenceSplitBolt;
import es.afm.storm.examples.bolts.WordCountBolt;
import es.afm.storm.examples.spouts.FileReaderSpout;
import backtype.storm.Config;
import backtype.storm.LocalCluster;
import backtype.storm.topology.TopologyBuilder;
import backtype.storm.tuple.Fields;

public class WordCount {

	private void run() throws InterruptedException {
		TopologyBuilder builder = new TopologyBuilder();
		builder.setSpout("file", new FileReaderSpout());
		builder.setBolt("normalize", new NormalizeBolt()).shuffleGrouping(
				"file");
		builder.setBolt("split", new SentenceSplitBolt()).shuffleGrouping(
				"normalize");
		builder.setBolt("wordcount", new WordCountBolt()).fieldsGrouping(
				"split", new Fields("word"));
		
		Config conf = new Config();
		conf.setDebug(true);
		conf.setNumWorkers(2);
		
		LocalCluster cluster = new LocalCluster();
		cluster.submitTopology("Wordcount", conf, builder.createTopology());
		Thread.sleep(10000);
		cluster.killTopology("Wordcount");
		cluster.shutdown();
	}

	public static void main(String[] args) throws InterruptedException {
		new WordCount().run();
	}
}
