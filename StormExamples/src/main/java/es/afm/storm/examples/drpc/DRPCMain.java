package es.afm.storm.examples.drpc;

import es.afm.storm.examples.bolts.ExclamationBoltDRPC;
import backtype.storm.Config;
import backtype.storm.LocalCluster;
import backtype.storm.LocalDRPC;
import backtype.storm.drpc.LinearDRPCTopologyBuilder;

public class DRPCMain {

	@SuppressWarnings("deprecation")
	private void run() {
		LinearDRPCTopologyBuilder builder = new LinearDRPCTopologyBuilder("exclamation-drpc");
		Config conf = new Config();
		conf.setNumWorkers(2);
		
		builder.addBolt(new ExclamationBoltDRPC());
		
		LocalDRPC drpc = new LocalDRPC();
		LocalCluster cluster = new LocalCluster();
		
		cluster.submitTopology("exclamation-drpc", conf, builder.createLocalTopology(drpc));
		System.out.println("Results for hola: " + drpc.execute("exclamation-drpc", "hola"));
		cluster.shutdown();
		drpc.shutdown();
		
	}
	public static void main(String[] args) {
		new DRPCMain().run();
	}
}
