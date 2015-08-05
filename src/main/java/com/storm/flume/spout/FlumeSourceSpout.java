/*
 * Copyright 2014-2015 CyberVision, Inc.
 *
 *  Licensed under the Apache License, Version 2.0 (the "License");
 *  you may not use this file except in compliance with the License.
 *  You may obtain a copy of the License at
 *
 *       http://www.apache.org/licenses/LICENSE-2.0
 *
 *  Unless required by applicable law or agreed to in writing, software
 *  distributed under the License is distributed on an "AS IS" BASIS,
 *  WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 *  See the License for the specific language governing permissions and
 *  limitations under the License.
 *
 *
 */

package com.storm.flume.spout;

import java.util.*;
import java.util.concurrent.ConcurrentHashMap;

import org.apache.flume.Channel;
import org.apache.flume.ChannelException;
import org.apache.flume.Event;
import org.apache.flume.FlumeException;
import org.apache.flume.SourceRunner;
import org.apache.flume.Transaction;
import org.apache.flume.instrumentation.SinkCounter;
import org.apache.flume.node.MaterializedConfiguration;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import backtype.storm.spout.SpoutOutputCollector;
import backtype.storm.task.TopologyContext;
import backtype.storm.topology.IRichSpout;
import backtype.storm.topology.OutputFieldsDeclarer;
import backtype.storm.tuple.Values;
import backtype.storm.utils.Utils;

import com.google.common.collect.Lists;
import com.google.common.collect.Maps;
import com.storm.flume.common.Constants;
import com.storm.flume.common.MaterializedConfigurationProvider;
import com.storm.flume.common.StormEmbeddedAgentConfiguration;
import com.storm.flume.producer.AvroTupleProducer;



@SuppressWarnings("serial")
public class FlumeSourceSpout implements IRichSpout {

	private static final Logger LOG = LoggerFactory
			.getLogger(FlumeSourceSpout.class);
	public static final String DEFAULT_FLUME_PROPERTY_PREFIX = "flume-agent";
	public static final String FLUME_AGENT_NAME = "flume-agent";
	public static final String FLUME_BATCH_SIZE = "batch-size";
	public static final int DEFAULT_BATCH_SIZE = 100;

	private Channel channel;
	private SourceRunner sourceRunner;
	private int batchSize = DEFAULT_BATCH_SIZE;
	private SinkCounter sinkCounter;
	private MaterializedConfigurationProvider configurationProvider;
	private SpoutOutputCollector outputCollector;

	//TODO Are we need it?
	private ConcurrentHashMap<String, Event> pendingMessages;
	
    private String flumePropertyPrefix = DEFAULT_FLUME_PROPERTY_PREFIX;

    public String getFlumePropertyPrefix() {
		return flumePropertyPrefix;
	}

	public void setFlumePropertyPrefix(String flumePropertPrefix) {
		this.flumePropertyPrefix = flumePropertPrefix;
	}

	public SpoutOutputCollector getOutputCollector() {
		return outputCollector;
	}

	public ConcurrentHashMap<String, Event> getPendingMessages() {
		return pendingMessages;
	}

	private AvroTupleProducer avroTupleProducer;

	public AvroTupleProducer getAvroTupleProducer() {
		return avroTupleProducer;
	}

	public void setAvroTupleProducer(AvroTupleProducer avroTupleProducer) {
		this.avroTupleProducer = avroTupleProducer;
	}

	@SuppressWarnings("rawtypes")
	public void open(Map config, TopologyContext context,
			SpoutOutputCollector collector) {
		this.configurationProvider = new MaterializedConfigurationProvider();
		
		Map<String, String> flumeAgentProps = Maps.newHashMap();
		for (Object key : config.keySet()) {
			LOG.debug("StormFlumeSourceSpout configuration:" + key.toString() + ":" + config.get(key));
			if (key.toString().startsWith(getFlumePropertyPrefix())) {
				// Since there will not be any sink component configured for
				// storm. batch size needs to be ignored
				if (key.toString().contains(FLUME_BATCH_SIZE)) {
					String batchSizeStr = (String) config.get(key);
					try {
						this.batchSize = Integer.parseInt(batchSizeStr);
					} catch (Exception e) {
						// tolerate this error and default it to the default
						// batch size
						this.batchSize = DEFAULT_BATCH_SIZE;
					}
				} else {
					flumeAgentProps.put(
							key.toString().replace(getFlumePropertyPrefix() + ".",
									""), (String) config.get(key));
				}
			}
		}

		flumeAgentProps = StormEmbeddedAgentConfiguration.configure(
				FLUME_AGENT_NAME, flumeAgentProps);
		MaterializedConfiguration conf = configurationProvider.get(
				getFlumePropertyPrefix(), flumeAgentProps);

		Map<String, Channel> channels = conf.getChannels();
		if (channels.size() != 1) {
			throw new FlumeException("Expected one channel and got "
					+ channels.size());
		}
		Map<String, SourceRunner> sources = conf.getSourceRunners();
		if (sources.size() != 1) {
			throw new FlumeException("Expected one source and got "
					+ sources.size());
		}

		this.sourceRunner = sources.values().iterator().next();
		this.channel = channels.values().iterator().next();

		if (sinkCounter == null) {
			sinkCounter = new SinkCounter(FlumeSourceSpout.class.getName());
		}

		if (null == this.getAvroTupleProducer()) {
			throw new IllegalStateException("Tuple Producer has not been set.");
		}

		this.outputCollector = collector;
		this.pendingMessages = new ConcurrentHashMap<String, Event>();

		try {
			this.start();
		} catch (Exception e) {
			LOG.warn("Error Starting Flume Source/channel", e);
		}
	}

	/*
	 * Starts all the flume components like source and channels.
	 */
	private void start() {
		if (null == this.sourceRunner || null == this.channel) {
			throw new FlumeException(
					"Source/Channel is null. Cannot start flume components");
		}
		this.sourceRunner.start();
		this.channel.start();
		this.sinkCounter.start();
	}

	/*
	 * Stops all the flume components like source and channels
	 */
	private void stop() {
		if (null == this.sourceRunner || null == this.channel) {
			return;
		}
		this.sourceRunner.stop();
		this.channel.stop();
		this.sinkCounter.stop();
	}

	/*
	 * On close, Flume components needs to be stopped
	 * 
	 * @see backtype.storm.spout.ISpout#close()
	 */
	public void close() {
		try {
			this.stop();
		} catch (Exception e) {
			LOG.warn("Error closing Avro RPC server.", e);
		}
	}

	/*
	 * Since FlumeSource, Channel is not deactivated, we dont need to activate
	 * FlumeSource, Channel
	 * 
	 * @see backtype.storm.spout.ISpout#activate()
	 */
    @Override
	public void activate() {
		// TODO Auto-generated method stub
	}

	/*
	 * On Deactivation, we dont need to stop the FlumeSource, Channel
	 * 
	 * @see backtype.storm.spout.ISpout#deactivate()
	 */
    @Override
	public void deactivate() {
		// TODO Auto-generated method stub
	}
    @Override
	public void nextTuple() {

		//Transaction Begins
		Transaction transaction = channel.getTransaction();

		int size = 0;
		try {
			transaction.begin();
			List<Event> batch = Lists.newLinkedList();
			//Get all the messages upto the batch size into memory
			for (int i = 0; i < this.batchSize; i++) {
				Event event = channel.take();
				if (event == null) {
					break;
				}
				batch.add(event);
			}

			//Update the counters if the batch is empty or underFlow
			size = batch.size();
			if (size == 0) {
				sinkCounter.incrementBatchEmptyCount();
			} else {
				if (size < this.batchSize) {
					sinkCounter.incrementBatchUnderflowCount();
				} else {
					sinkCounter.incrementBatchCompleteCount();
				}
				sinkCounter.addToEventDrainAttemptCount(size);
			}

			// Emit all the the events in the topology before committing the
			// transaction
			for (Event event : batch) {

				Values vals = this.getAvroTupleProducer().toTuple(event);

				this.outputCollector.emit(vals, event);
				this.pendingMessages.put(
						event.getHeaders().get(Constants.MESSAGE_ID), event);
				LOG.debug("NextTuple:"
						+ event.getHeaders().get(Constants.MESSAGE_ID));
			}
			//Everything went fine. Commit the transaction 
			transaction.commit();
			sinkCounter.addToEventDrainSuccessCount(size);

		} catch (Throwable t) {
			//On error, roll back the whole batch
			transaction.rollback();
			if (t instanceof Error) {
				throw (Error) t;
			} else if (t instanceof ChannelException) {
				LOG.error(
						"Unable to get event from" + " channel "
								+ channel.getName() + ". Exception follows.", t);
			} else {
				LOG.error("Failed to emit events", t);
			}
		} finally {
			transaction.close();
		}

		//if we come across empty batch, sleep for some time as the load is not that high anyways
		if (size == 0) {
			Utils.sleep(100);
		}
	}

	/*
	 * When a message is succeeded remove from the pending list
	 * 
	 * @see backtype.storm.spout.ISpout#ack(java.lang.Object)
	 */
    @Override
	public void ack(Object msgId) {
		this.pendingMessages.remove(msgId);

		System.out.println("IN>> [" + Thread.currentThread().getId() + "] message " +
				msgId + " processed successfully");
	}


	@Override
	public void fail(Object msgId) {
		//TODO implement retry
        Event event = (Event)msgId;
        System.out.println("FAILED Event data: "+new String(event.getBody()));
        /*
		Event m = this.pendingMessages.get(msgId);
		channel.put();
		if(++m.failCount > MAX_RETRY_COUNT) {
			throw new IllegalStateException("Too many message processing errors");
		}
		System.out.println("IN>> [" + Thread.currentThread().getId() + "] message " +
				m.message + " processing failed " + "[" + m.failCount + "]");
// Вставляем в очередь на повторную обработку
		sendQueue.addLast((Integer) msgId);
		*/
	}

    @Override
	public Map<String, Object> getComponentConfiguration() {
		// TODO Auto-generated method stub
		return null;
	}

    @Override
	public void declareOutputFields(OutputFieldsDeclarer declarer) {
		this.getAvroTupleProducer().declareOutputFields(declarer);
	}


}
