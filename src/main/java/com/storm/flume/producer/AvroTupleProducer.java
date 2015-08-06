

package com.storm.flume.producer;

import java.io.Serializable;
import org.apache.flume.Event;
import backtype.storm.tuple.Values;
import backtype.storm.topology.OutputFieldsDeclarer;



public interface AvroTupleProducer extends Serializable{
	Values toTuple(Event event) throws Exception;
	void declareOutputFields(OutputFieldsDeclarer declarer);
}
