package es.afm.storm.examples.bolts;

import java.util.HashMap;
import java.util.Map;

import backtype.storm.task.TopologyContext;
import backtype.storm.topology.BasicOutputCollector;
import backtype.storm.topology.IBasicBolt;
import backtype.storm.topology.OutputFieldsDeclarer;
import backtype.storm.tuple.Fields;
import backtype.storm.tuple.Tuple;
import backtype.storm.tuple.Values;

public class WordCountBolt implements IBasicBolt {
	private static final long serialVersionUID = 1L;
	private HashMap<String, Long> wordcounts;
	
	public void declareOutputFields(OutputFieldsDeclarer declarer) {
		declarer.declare(new Fields("wordcount"));
	}

	public Map<String, Object> getComponentConfiguration() {
		return null;
	}

	public void cleanup() { }

	public void execute(Tuple tuple, BasicOutputCollector collector) {
		String word = tuple.getStringByField("word");
		if(wordcounts.containsKey(word)) {
			long value = wordcounts.get(word) + 1;
			wordcounts.put(word, value);
		}else {
			wordcounts.put(word, 1L);
		}
		collector.emit(new Values(word + ", " + wordcounts.get(word)));
	}

	public void prepare(Map conf, TopologyContext context) {
		wordcounts = new HashMap<String, Long>();
	}

}
