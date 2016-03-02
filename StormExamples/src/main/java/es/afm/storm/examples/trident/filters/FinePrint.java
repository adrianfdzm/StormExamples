package es.afm.storm.examples.trident.filters;

import java.util.Map;

import storm.trident.operation.BaseFilter;
import storm.trident.operation.TridentOperationContext;
import storm.trident.tuple.TridentTuple;

public class FinePrint extends BaseFilter {
	private static final long serialVersionUID = 1L;
	private int partitionIndex;
    private int numPartitions;
    private final String name;

    public FinePrint(){
        name = "";
    }
    public FinePrint(String name){
        this.name = name;
    }

    @Override
    public void prepare(Map conf, TridentOperationContext context) {
        this.partitionIndex = context.getPartitionIndex();
        this.numPartitions = context.numPartitions();
    }


    public boolean isKeep(TridentTuple tuple) {
        System.err.println(String.format("%s::Partition %s/%s -> %s",
                name, partitionIndex, numPartitions - 1, tuple.toString()));
        return true;
    }
}
