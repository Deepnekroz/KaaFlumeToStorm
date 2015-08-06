

package com.storm.flume.producer;

import java.io.Serializable;

import org.apache.flume.Event;

import backtype.storm.tuple.Tuple;



public interface AvroFlumeEventProducer extends Serializable {
	Event toEvent(Tuple input) throws Exception;
}
