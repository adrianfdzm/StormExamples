package es.afm.storm.examples.apachelog;

import es.afm.storm.examples.spouts.ApacheLogSpout;
import backtype.storm.Config;
import backtype.storm.LocalCluster;
import backtype.storm.topology.TopologyBuilder;

public class ApacheLog {

	private void run() throws InterruptedException {
		Config conf = new Config();
		conf.setNumWorkers(2);

		TopologyBuilder builder = new TopologyBuilder();

		builder.setSpout("apache", new ApacheLogSpout());
		builder.setBolt("filter", new FilterResponseCodeBolt())
				.shuffleGrouping("apache");
		builder.setBolt("mean-obj-size", new AvgObjSizeBolt()).shuffleGrouping(
				"filter", FilterResponseCodeBolt.STREAM_200);
		builder.setBolt("response-code-count", new ResponseCodeCountBolt())
				.shuffleGrouping("filter", FilterResponseCodeBolt.STREAM_ERROR);

		LocalCluster cluster = new LocalCluster();
		cluster.submitTopology("apache", conf, builder.createTopology());

		Thread.sleep(10000);
		cluster.killTopology("apache");
		cluster.shutdown();
	}

	public static void main(String[] args) throws InterruptedException {
		new ApacheLog().run();
	}

}
