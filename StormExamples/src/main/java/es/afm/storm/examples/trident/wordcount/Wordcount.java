package es.afm.storm.examples.trident.wordcount;

import es.afm.storm.examples.trident.aggregators.SumCombinerAggregator;
import es.afm.storm.examples.trident.filters.Print;
import backtype.storm.Config;
import backtype.storm.LocalCluster;
import backtype.storm.tuple.Fields;
import backtype.storm.tuple.Values;
import backtype.storm.utils.Utils;
import storm.trident.TridentTopology;
import storm.trident.testing.FixedBatchSpout;
import storm.trident.testing.MemoryMapState;
import storm.trident.testing.Split;
import storm.trident.topology.TridentTopologyBuilder;

public class Wordcount {
	
	private void run() {
		TridentTopology topology = new TridentTopology();
		// new FixedBatchSpout(fields, maxBatchSize, outputs)
		@SuppressWarnings("unchecked")
		FixedBatchSpout spout = new FixedBatchSpout(new Fields("sentence"), 4,
				new Values("Hola mundo"), new Values(
						"Trident carece de buena documentacion"), new Values(
						"Hay muchos implementaciones de Spouts"), new Values(
						"Para casi todos los origenes de datos"), new Values(
						"Adios mundo"));
		spout.setCycle(true);
		topology.newStream("wordcount", spout)
				.each(new Fields("sentence"), new Split(), new Fields("word"))
				.groupBy(new Fields("word"))
				.persistentAggregate(new MemoryMapState.Factory(),
						new Fields("word"), new SumCombinerAggregator(),
						new Fields("count")).newValuesStream()
				.each(new Fields("word", "count"), new Print());

		Config conf = new Config();

		LocalCluster cluster = new LocalCluster();
		cluster.submitTopology("wordcount", conf, topology.build());
		Utils.sleep(10000);
		cluster.killTopology("wordcount");
	}

	public static void main(String[] args) {
		new Wordcount().run();
	}
}
