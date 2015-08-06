

package com.storm.flume;

import backtype.storm.Config;
import backtype.storm.LocalCluster;
import backtype.storm.topology.TopologyBuilder;
import com.storm.flume.bolt.AvroSinkBolt;
import com.storm.flume.producer.AvroTupleProducer;
import com.storm.flume.producer.SimpleAvroFlumeEventProducer;
import com.storm.flume.producer.SimpleAvroTupleProducer;
import com.storm.flume.spout.FlumeSourceSpout;
import org.slf4j.LoggerFactory;
import java.io.FileInputStream;
import java.util.Properties;
import ch.qos.logback.classic.Level;
import ch.qos.logback.classic.Logger;

public class Main {
    public static void main(String[] args) throws Throwable{
        Logger root = (Logger)LoggerFactory.getLogger(Logger.ROOT_LOGGER_NAME);
        root.setLevel(Level.ERROR);
        Logger LOG = (Logger)LoggerFactory.getLogger(Main.class);

        if(args.length==0){
            LOG.error("No path to properties was given! Check args");
        }
        Properties props = new Properties();
        props.load(new FileInputStream(args[0]));

        TopologyBuilder builder = new TopologyBuilder();
        FlumeSourceSpout spout = new FlumeSourceSpout();

        AvroTupleProducer producer = new SimpleAvroTupleProducer();
        spout.setAvroTupleProducer(producer);

        builder.setSpout("FlumeSourceSpout", spout).addConfigurations(props);

        AvroSinkBolt bolt = new AvroSinkBolt();
        bolt.setProducer(new SimpleAvroFlumeEventProducer());
        //Set 2 threads bolt
        builder.setBolt("AvroSinkBolt", bolt, 2).shuffleGrouping("FlumeSourceSpout").addConfigurations(props);

        Config config = new Config(); //Default configuration
        config.setDebug(false);

        LocalCluster cluster = new LocalCluster();
        cluster.submitTopology("T1", config, builder.createTopology());
        LOG.info("Topology running...");

        //Kill topology if needed
        //cluster.shutdown();
    }
}
