package es.afm.storm.examples.redis;

import org.apache.storm.redis.common.mapper.RedisDataTypeDescription;
import org.apache.storm.redis.common.mapper.RedisStoreMapper;

import backtype.storm.tuple.ITuple;

public class RedisWordCountMapper implements RedisStoreMapper {
	private static final long serialVersionUID = 1L;
	private RedisDataTypeDescription description;
    private final String hashKey = "wordCount";

    public RedisWordCountMapper() {
        description = new RedisDataTypeDescription(
            RedisDataTypeDescription.RedisDataType.SORTED_SET, hashKey);
    }

	public String getKeyFromTuple(ITuple tuple) {
		return tuple.getStringByField("word");
	}

	public String getValueFromTuple(ITuple tuple) {
		return tuple.getLongByField("count").toString();
	}

	public RedisDataTypeDescription getDataTypeDescription() {
		return description;
	}

}
