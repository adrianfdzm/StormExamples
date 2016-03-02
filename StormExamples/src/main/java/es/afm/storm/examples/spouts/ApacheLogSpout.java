package es.afm.storm.examples.spouts;

import java.io.BufferedReader;
import java.io.FileInputStream;
import java.io.IOException;
import java.io.InputStreamReader;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.Map;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

import backtype.storm.spout.SpoutOutputCollector;
import backtype.storm.task.TopologyContext;
import backtype.storm.topology.IRichSpout;
import backtype.storm.topology.OutputFieldsDeclarer;
import backtype.storm.tuple.Fields;
import backtype.storm.tuple.Values;
import backtype.storm.utils.Utils;

public class ApacheLogSpout implements IRichSpout {
	private static final long serialVersionUID = 1L;
	private static final Pattern regEx = Pattern
			.compile("^(\\S+) \\S+ \\S+ \\[([^\\]]+)\\] \"([A-Z]+) ([^\"\\s]*) ([^\"]*)\" (\\d+) (\\d+)$");
	public final static String[] fieldNames = { "from", "date", "http_method",
			"requested_url", "protocol", "response_code", "obj_size" };

	private BufferedReader _br;
	private SpoutOutputCollector _collector;

	@Override
	public void ack(Object arg0) {
	}

	@Override
	public void activate() {
	}

	@Override
	public void close() {
		try {
			if (_br != null)
				_br.close();
		} catch (IOException ignored) {
		}
	}

	@Override
	public void deactivate() {
	}

	@Override
	public void fail(Object arg0) {
	}

	@Override
	public void nextTuple() {
		try {
			Thread.sleep(100);
			String line = _br.readLine();
			if (line != null) {
				Matcher matcher = regEx.matcher(line);
				if (matcher.find()) {
					List<Object> values = new ArrayList<Object>();
					for (int i = 0; i < fieldNames.length; i++) {
						values.add(matcher.group(i + 1));
					}
					_collector.emit(values);
				}
			}
		} catch (Exception ignored) {
			ignored.printStackTrace();
		}
	}

	@Override
	public void open(Map conf, TopologyContext context,
			SpoutOutputCollector collector) {
		_collector = collector;
		try {
			_br = new BufferedReader(new InputStreamReader(new FileInputStream(
					"src/main/resources/apache-log.log")));
		} catch (Exception ignored) {
			ignored.printStackTrace();
		}
	}

	@Override
	public void declareOutputFields(OutputFieldsDeclarer declarer) {
		declarer.declare(new Fields(Arrays.asList(fieldNames)));
	}

	@Override
	public Map<String, Object> getComponentConfiguration() {
		return null;
	}

}
