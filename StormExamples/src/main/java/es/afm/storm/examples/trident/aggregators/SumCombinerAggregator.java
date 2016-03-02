package es.afm.storm.examples.trident.aggregators;

import storm.trident.operation.CombinerAggregator;
import storm.trident.tuple.TridentTuple;

public class SumCombinerAggregator implements CombinerAggregator<Long> {

	private static final long serialVersionUID = 1L;

	public Long combine(Long arg0, Long arg1) {
		return arg0 + arg1;
	}

	public Long init(TridentTuple tuple) {
		return 1L;
	}

	public Long zero() {
		return 0L;
	}
}
