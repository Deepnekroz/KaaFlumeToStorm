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

package com.storm.flume.producer;

import java.util.HashMap;
import java.util.Map;

import org.apache.flume.Event;
import org.apache.flume.event.EventBuilder;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.storm.flume.common.Constants;

import backtype.storm.tuple.Tuple;



@SuppressWarnings("serial")
public class SimpleAvroFlumeEventProducer implements AvroFlumeEventProducer {

	private static final Logger LOG = LoggerFactory.getLogger(SimpleAvroFlumeEventProducer.class);
	
	private static final String CHARSET = "UTF-8";
	
	public static String getCharset() {
		return CHARSET;
	}

	@SuppressWarnings("unchecked")
	public Event toEvent(Tuple input) throws Exception {
		
		Map<String, String> headers;
		Object headerObj;
		Object messageObj;
		String messageStr;
		
		/*If the number of parameters are two, they are assumed as MessageId and Message
		 *If the number of parameters are three, they are assumed as MessageId, Headers and Message
		 */
		if(input.size()==2){
			messageObj = input.getValue(1);
			headers = new HashMap<String, String>();
			headers.put(Constants.MESSAGE_ID, input.getString(0));
			headers.put(Constants.TIME_STAMP, String.valueOf(System.currentTimeMillis()));
		}else if(input.size()==3){
			headerObj = input.getValue(1);
			messageObj = input.getValue(2);
			headers = (Map<String, String>)headerObj;	
			
			LOG.debug("String format of object:" +  ((String)input.getValue(2)));
		}else{
			throw new IllegalStateException("Wrong format of touple expected 2 or 3 values. But found " + input.size());
		}

		messageStr = (String)messageObj;
		   
		LOG.debug("SimpleAvroFlumeEventProducer:MSG:" + messageStr);

		return EventBuilder.withBody(messageStr.getBytes(), headers);

	}

}
