package es.afm.storm.examples.apachelog;

import java.util.Map;

import backtype.storm.task.TopologyContext;
import backtype.storm.topology.BasicOutputCollector;
import backtype.storm.topology.IBasicBolt;
import backtype.storm.topology.OutputFieldsDeclarer;
import backtype.storm.tuple.Fields;
import backtype.storm.tuple.Tuple;
import backtype.storm.tuple.Values;

public class AvgObjSizeBolt implements IBasicBolt {

	private static final long serialVersionUID = 1L;
	private long sum = 0;
	private long count = 0;

	@Override
	public void declareOutputFields(OutputFieldsDeclarer declarer) {
		declarer.declare(new Fields("avg-size"));
	}

	@Override
	public Map<String, Object> getComponentConfiguration() {
		return null;
	}

	@Override
	public void cleanup() {
	}

	@Override
	public void execute(Tuple tuple, BasicOutputCollector collector) {
		try {
			sum += Long.parseLong(tuple.getStringByField("obj_size"));
			count++;
			collector.emit(new Values(sum / (double) count));
			System.err.println("Avg object size " + sum / (double) count);
		} catch (Exception ignored) {
		}
	}

	@Override
	public void prepare(Map conf, TopologyContext context) {
	}

}
