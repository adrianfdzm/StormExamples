package es.afm.storm.examples.spouts;

import java.util.Map;
import java.util.Random;

import backtype.storm.spout.SpoutOutputCollector;
import backtype.storm.task.TopologyContext;
import backtype.storm.topology.IRichSpout;
import backtype.storm.topology.OutputFieldsDeclarer;
import backtype.storm.tuple.Fields;
import backtype.storm.tuple.Values;

public class TestWordSpout implements IRichSpout {
	private static final long serialVersionUID = 1L;
	private SpoutOutputCollector _collector;
	private final static String[] WORDS = { "Hola", "Mundo", "Storm" };
	private Random _rnd;

	public TestWordSpout() {
		_rnd = new Random(System.currentTimeMillis());
	}

	public void ack(Object arg0) {
	}

	public void activate() {
	}

	public void close() {
	}

	public void deactivate() {
	}

	public void fail(Object arg0) {
	}

	public void nextTuple() {
		try {
			Thread.sleep(100);
			_collector.emit(new Values(WORDS[_rnd.nextInt(WORDS.length)]));
		} catch (InterruptedException ignored) {
		}
	}

	public void open(Map conf, TopologyContext context,
			SpoutOutputCollector collector) {
		_collector = collector;
	}

	public void declareOutputFields(OutputFieldsDeclarer declarer) {
		declarer.declare(new Fields("word"));
	}

	public Map<String, Object> getComponentConfiguration() {
		return null;
	}
}
