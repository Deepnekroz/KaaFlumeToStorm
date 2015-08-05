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

package com.storm.flume;

import backtype.storm.Config;
import backtype.storm.LocalCluster;
import backtype.storm.topology.TopologyBuilder;
import com.storm.flume.bolt.AvroSinkBolt;
import com.storm.flume.producer.AvroTupleProducer;
import com.storm.flume.producer.SimpleAvroFlumeEventProducer;
import com.storm.flume.producer.SimpleAvroTupleProducer;
import com.storm.flume.spout.FlumeSourceSpout;
import org.apache.log4j.Level;
import org.apache.log4j.Logger;

import java.io.FileInputStream;
import java.util.Properties;


public class Main {
    public static void main(String[] args) throws Throwable{
        Logger.getRootLogger().setLevel(Level.FATAL);


        Properties props = new Properties();
        props.load(new FileInputStream("/home/dmitry-sergeev/Downloads/apache-flume-1.6.0-bin/conf/flume-conf.properties.template"));


        TopologyBuilder builder = new TopologyBuilder();
        FlumeSourceSpout spout = new FlumeSourceSpout();

        AvroTupleProducer producer = new SimpleAvroTupleProducer();
        spout.setAvroTupleProducer(producer);


        builder.setSpout("FlumeSourceSpout", spout).addConfigurations(props);

        AvroSinkBolt bolt = new AvroSinkBolt();
        bolt.setProducer(new SimpleAvroFlumeEventProducer());
        builder.setBolt("AvroSinkBolt", bolt, 2).shuffleGrouping("FlumeSourceSpout").addConfigurations(props);

        Config config = new Config(); //Default configuration
        config.setDebug(false);

        LocalCluster cluster = new LocalCluster();
        cluster.submitTopology("T1", config, builder.createTopology());
        //Thread.sleep(1000*10);
        //cluster.shutdown();
    }
}
