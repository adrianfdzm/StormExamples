package es.afm.storm.examples.bolts;

import java.util.Arrays;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;

import backtype.storm.task.TopologyContext;
import backtype.storm.topology.BasicOutputCollector;
import backtype.storm.topology.IBasicBolt;
import backtype.storm.topology.OutputFieldsDeclarer;
import backtype.storm.tuple.Fields;
import backtype.storm.tuple.Tuple;
import backtype.storm.tuple.Values;

public class SentenceSplitBolt implements IBasicBolt {
	private static final long serialVersionUID = 1L;
	private static final String WHITESPACES_REGEX = "\\s+";
	private static final Set<String> COMMON_WORDS = new HashSet<String>(
			Arrays.asList(new String[] { "en", "que", "de", "a", "la", "el",
					"las", "los", "y", "o", "por", "una", "uno", "del", "este",
					"esta" }));

	public void declareOutputFields(OutputFieldsDeclarer declarer) {
		declarer.declare(new Fields("word"));
	}

	public Map<String, Object> getComponentConfiguration() {
		return null;
	}

	public void cleanup() {
	}

	public void execute(Tuple tuple, BasicOutputCollector collector) {
		String sentence = tuple.getStringByField("normalizedSentence");
		for (String word : sentence.split(WHITESPACES_REGEX)) {
			if (!COMMON_WORDS.contains(word)) {
				collector.emit(new Values(word));
			}
		}
	}

	public void prepare(Map conf, TopologyContext contex) {
	}

}
