package es.afm.storm.examples.apachelog;

import java.util.HashMap;
import java.util.Map;

import backtype.storm.task.TopologyContext;
import backtype.storm.topology.BasicOutputCollector;
import backtype.storm.topology.IBasicBolt;
import backtype.storm.topology.OutputFieldsDeclarer;
import backtype.storm.tuple.Fields;
import backtype.storm.tuple.Tuple;
import backtype.storm.tuple.Values;

public class ResponseCodeCountBolt implements IBasicBolt {

	private static final long serialVersionUID = 1L;
	private HashMap<String, Long> count = new HashMap<String, Long>();

	@Override
	public void declareOutputFields(OutputFieldsDeclarer declarer) {
		declarer.declare(new Fields("code", "count"));
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
			String code = tuple.getStringByField("response_code");
			if (count.containsKey(code)) {
				long valor = count.get(code) + 1;
				count.put(code, valor);
				collector.emit(new Values(code, valor));
				System.err.println(code + ", " + valor);
			} else {
				count.put(code, 1L);
				collector.emit(new Values(code, 1L));
				System.err.println(code + ", " + 1L);
			}
		} catch (Exception ignored) {
		}
	}

	@Override
	public void prepare(Map conf, TopologyContext context) {
	}

}
