package es.afm.storm.examples.apachelog;

import java.util.Arrays;
import java.util.Map;

import backtype.storm.task.TopologyContext;
import backtype.storm.topology.BasicOutputCollector;
import backtype.storm.topology.IBasicBolt;
import backtype.storm.topology.OutputFieldsDeclarer;
import backtype.storm.tuple.Fields;
import backtype.storm.tuple.Tuple;

public class FilterResponseCodeBolt implements IBasicBolt {
	private static final long serialVersionUID = 1L;
	public static final String[] fieldNames = { "from", "date", "http_method",
			"requested_url", "protocol", "response_code", "obj_size" };
	public static final String STREAM_200 = "STREAM200";
	public static final String STREAM_ERROR = "STREAMERROR";

	@Override
	public void declareOutputFields(OutputFieldsDeclarer declarer) {
		declarer.declareStream(STREAM_200,
				new Fields(Arrays.asList(fieldNames)));
		declarer.declareStream(STREAM_ERROR,
				new Fields(Arrays.asList(fieldNames)));
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
		if (tuple.getStringByField("response_code").compareTo("200") == 0) {
			collector.emit(STREAM_200, tuple.getValues());
		} else {
			collector.emit(STREAM_ERROR, tuple.getValues());
		}
	}

	@Override
	public void prepare(Map conf, TopologyContext context) {
	}
}
