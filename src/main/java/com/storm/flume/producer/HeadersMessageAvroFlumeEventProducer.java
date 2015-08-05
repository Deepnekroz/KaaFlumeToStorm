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

import backtype.storm.tuple.Tuple;
import com.storm.flume.common.Constants;
import org.apache.flume.Event;
import org.apache.flume.event.EventBuilder;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Map;



@SuppressWarnings("serial")
public class HeadersMessageAvroFlumeEventProducer implements AvroFlumeEventProducer {

    private static final Logger LOG = LoggerFactory.getLogger(HeadersMessageAvroFlumeEventProducer.class);

    private static final String CHARSET = "UTF-8";

    public static String getCharset() {
        return CHARSET;
    }

    @SuppressWarnings("unchecked")
    public Event toEvent(Tuple input) throws Exception {

        Map<String, String> headers;
        Object headerObj;
        String messageStr;

		/*If the number of parameters are two, they are assumed as headers and Message
		 *For any other types of input will be thrown an error.
		 */
        if (input.size() == 2) {
            headerObj = input.getValueByField(Constants.HEADERS);
            headers = (Map<String, String>) headerObj;
            messageStr = input.getStringByField(Constants.MESSAGE);
        } else {
            throw new IllegalStateException("Wrong format of touple expected 2. But found " + input.size());
        }

        LOG.debug("HeadersMessageAvroFlumeEventProducer:MSG:" + messageStr);


        return EventBuilder.withBody(messageStr.getBytes(), headers);

    }

}
