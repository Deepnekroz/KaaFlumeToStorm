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

import java.util.UUID;

import org.apache.flume.Event;

import backtype.storm.topology.OutputFieldsDeclarer;
import backtype.storm.tuple.Fields;
import backtype.storm.tuple.Values;

import com.storm.flume.common.Constants;



@SuppressWarnings("serial")
public class HeaderTextMessageTupleProducer implements AvroTupleProducer{

	public Values toTuple(Event event) throws Exception {
		String msgID = event.getHeaders().get(Constants.MESSAGE_ID);
		
		//if MessageID header doesn't exists, set the MessageId
		if(null == msgID) {
			UUID randMsgID = UUID.randomUUID();
			msgID = randMsgID.toString();
			event.getHeaders().put(Constants.MESSAGE_ID, msgID);
		}
		String msg = new String(event.getBody());
		return new Values(msgID, event.getHeaders(),msg);
	}

	public void declareOutputFields(OutputFieldsDeclarer declarer) {
		declarer.declare(new Fields(Constants.MESSAGE_ID,Constants.HEADERS,Constants.MESSAGE));
	}

}
