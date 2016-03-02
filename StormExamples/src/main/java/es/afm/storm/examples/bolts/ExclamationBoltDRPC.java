package es.afm.storm.examples.bolts;

import java.util.Map;

import backtype.storm.task.TopologyContext;
import backtype.storm.topology.BasicOutputCollector;
import backtype.storm.topology.OutputFieldsDeclarer;
import backtype.storm.tuple.Fields;
import backtype.storm.tuple.Tuple;
import backtype.storm.tuple.Values;

public class ExclamationBoltDRPC implements backtype.storm.topology.IBasicBolt {

	private static final long serialVersionUID = 1L;

	public void declareOutputFields(OutputFieldsDeclarer declarer) {
		declarer.declare(new Fields("id", "result"));
	}

	public Map<String, Object> getComponentConfiguration() {
		return null;
	}

	public void cleanup() {}

	public void execute(Tuple tuple, BasicOutputCollector collector) {
		String word = tuple.getString(1);
		collector.emit(new Values(tuple.getValue(0), word + "!!!"));
	}

	public void prepare(Map conf, TopologyContext context) {}

}
