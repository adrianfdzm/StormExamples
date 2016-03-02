package es.afm.storm.examples.bolts;

import java.util.Map;

import backtype.storm.task.OutputCollector;
import backtype.storm.task.TopologyContext;
import backtype.storm.topology.IRichBolt;
import backtype.storm.topology.OutputFieldsDeclarer;
import backtype.storm.tuple.Fields;
import backtype.storm.tuple.Tuple;
import backtype.storm.tuple.Values;

public class ExclamationBolt implements IRichBolt {
	private static final long serialVersionUID = 1L;
	private OutputCollector _collector;

	public void cleanup() { }

	public void execute(Tuple tuple) {
		_collector.emit(new Values(tuple.getValueByField("word") + "!!!"));
		_collector.ack(tuple);
	}

	public void prepare(Map conf, TopologyContext context, OutputCollector collector) {
		_collector = collector;

	}

	public void declareOutputFields(OutputFieldsDeclarer declarer) {
		declarer.declare(new Fields("word"));
	}

	public Map<String, Object> getComponentConfiguration() {
		return null;
	}

}
