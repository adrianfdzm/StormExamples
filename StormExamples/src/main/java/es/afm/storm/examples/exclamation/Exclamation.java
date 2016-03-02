package es.afm.storm.examples.exclamation;

import es.afm.storm.examples.bolts.ExclamationBolt;
import es.afm.storm.examples.spouts.TestWordSpout;
import backtype.storm.Config;
import backtype.storm.LocalCluster;
import backtype.storm.topology.TopologyBuilder;

public class Exclamation {

	private void run() throws InterruptedException {
		TopologyBuilder builder = new TopologyBuilder();
		builder.setSpout("words", new TestWordSpout());
		builder.setBolt("exclaim", new ExclamationBolt()).shuffleGrouping("words");
		builder.setBolt("exclaim2", new ExclamationBolt()).shuffleGrouping("exclaim");
		
		Config conf = new Config();
		conf.setDebug(true);
		conf.setNumWorkers(2);
		
		LocalCluster cluster = new LocalCluster();
		cluster.submitTopology("Exclamation", conf, builder.createTopology());
		Thread.sleep(10000);
		cluster.killTopology("Exclamation");
		cluster.shutdown();
	}
	
	public static void main(String[] args) throws InterruptedException {
		new Exclamation().run();
	}
}
