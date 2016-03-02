package es.afm.storm.examples.spouts;

import java.io.BufferedReader;
import java.io.FileInputStream;
import java.io.IOException;
import java.io.InputStreamReader;
import java.util.Map;

import backtype.storm.spout.SpoutOutputCollector;
import backtype.storm.task.TopologyContext;
import backtype.storm.topology.IRichSpout;
import backtype.storm.topology.OutputFieldsDeclarer;
import backtype.storm.tuple.Fields;
import backtype.storm.tuple.Values;

public class FileReaderSpout implements IRichSpout {
	private static final long serialVersionUID = 1L;
	private SpoutOutputCollector _collector;
	private BufferedReader _br;

	public void ack(Object arg0) {
		// TODO Auto-generated method stub
	}

	public void activate() {
		// TODO Auto-generated method stub
	}

	public void close() {
		try {
			if(_br != null){
				_br.close();
			}
		} catch(IOException ignored) {} 
	}

	public void deactivate() {
		// TODO Auto-generated method stub	
	}

	public void fail(Object arg0) {
		// TODO Auto-generated method stub
	}

	public void nextTuple() {
		try {
		Thread.sleep(100);
		String line = _br.readLine();
		if (line != null){
			_collector.emit(new Values(line));
		}
		}catch(IOException ignored) {} 
		catch (InterruptedException ignored) {}
	}

	public void open(Map conf, TopologyContext context, SpoutOutputCollector collector) {
		_collector = collector;
		try {
		_br = new BufferedReader(new InputStreamReader(new FileInputStream("src/main/resources/pg2000.txt")));
		}catch (IOException e) {
			e.printStackTrace();
		}
	}

	public void declareOutputFields(OutputFieldsDeclarer declarer) {
		declarer.declare(new Fields("sentence"));
	}

	public Map<String, Object> getComponentConfiguration() {
		return null;
	}

}
