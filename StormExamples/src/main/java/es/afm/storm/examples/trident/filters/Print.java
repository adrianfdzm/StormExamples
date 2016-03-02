package es.afm.storm.examples.trident.filters;

import java.util.Map;

import storm.trident.operation.Filter;
import storm.trident.operation.TridentOperationContext;
import storm.trident.tuple.TridentTuple;

public class Print implements Filter {
	private static final long serialVersionUID = 1L;

	public void cleanup() {
	}

	public void prepare(Map conf, TridentOperationContext context) {
	}

	public boolean isKeep(TridentTuple tuple) {
		System.err.println(tuple.toString());
		return true;
	}
}
