package es.afm.storm.examples.bolts;

import java.util.Map;

import backtype.storm.task.TopologyContext;
import backtype.storm.topology.BasicOutputCollector;
import backtype.storm.topology.IBasicBolt;
import backtype.storm.topology.OutputFieldsDeclarer;
import backtype.storm.tuple.Fields;
import backtype.storm.tuple.Tuple;
import backtype.storm.tuple.Values;

public class NormalizeBolt implements IBasicBolt {
	private static final long serialVersionUID = 1L;
	private static final String WHITESPACE = " ";
	private static final String NO_WORD_REGEX = "\\W";

	public void declareOutputFields(OutputFieldsDeclarer declarer) {
		declarer.declare(new Fields("normalizedSentence"));
	}

	public Map<String, Object> getComponentConfiguration() {
		return null;
	}

	public void cleanup() {}

	public void execute(Tuple tuple, BasicOutputCollector collector) {
		String sentence = tuple.getStringByField("sentence");
		sentence = sentence.toLowerCase().replaceAll(NO_WORD_REGEX, WHITESPACE);
		collector.emit(new Values(sentence));
	}

	public void prepare(Map conf, TopologyContext context) {}

}
