package com.storm.flume.bolt;

/*
 * Copyright 2014-2015 CyberVision, Inc.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *      http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

import java.util.Map;
import java.util.Properties;
import org.apache.flume.Event;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import com.storm.flume.producer.AvroFlumeEventProducer;
import backtype.storm.task.OutputCollector;
import backtype.storm.task.TopologyContext;
import backtype.storm.topology.IRichBolt;
import backtype.storm.topology.OutputFieldsDeclarer;
import backtype.storm.tuple.Tuple;

@SuppressWarnings("serial")
public class AvroSinkBolt implements IRichBolt {

    private static final Logger LOG = LoggerFactory.getLogger(AvroSinkBolt.class);
    public static final String DEFAULT_FLUME_PROPERTY_PREFIX = "flume-avro-forward";
    
    private Properties sinkProperties;
    private AvroFlumeEventProducer producer;
    private OutputCollector collector;
    private String flumePropertyPrefix = DEFAULT_FLUME_PROPERTY_PREFIX;

    public String getFlumePropertyPrefix() {
		return flumePropertyPrefix;
	}

	public void setFlumePropertyPrefix(String flumePropertPrefix) {
		this.flumePropertyPrefix = flumePropertPrefix;
	}

	public AvroFlumeEventProducer getProducer() {
        return producer;
    }

    public void setProducer(AvroFlumeEventProducer producer) {
        this.producer = producer;
    }

    @SuppressWarnings("rawtypes")
    public void prepare(Map config, TopologyContext context, OutputCollector collector) {
    	
        this.collector = collector;
        sinkProperties = new Properties();
        LOG.info("Looking for flume properties");
		for (Object key : config.keySet()) {
			if (key.toString().startsWith(this.getFlumePropertyPrefix())) {
				LOG.info("Found:Key:" + key.toString() + ":" + (String) config.get(key));
				sinkProperties.put(
							key.toString().replace(this.getFlumePropertyPrefix() + ".",
									""), (String) config.get(key));
			}
		}

    }

    public void execute(Tuple input) {

        try {
            Event event = this.producer.toEvent(input);
            LOG.info("Event Created: " + event.toString() + ":MSG:" + new String(event.getBody()));

            //Example of failed Tuple
            if("wrong text".equals(new String(event.getBody())))
                throw new ClassCastException();

            //All seems to be nice, notify spout about it
            this.collector.ack(input);

        } catch (Exception e) {
            LOG.warn("Failing tuple: " + input);
            LOG.warn("Exception: ", e);
            //Notify spout about fail
            this.collector.fail(input);
        }
    }

    @Override
    public void cleanup() {
    }

    @Override
    public void declareOutputFields(OutputFieldsDeclarer declarer) {
    }

    @Override
    public Map<String, Object> getComponentConfiguration() {
        return null;
    }




}
