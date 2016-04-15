package org.librairy.modeler.lda.helper;

import lombok.Getter;
import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaSparkContext;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.stereotype.Component;

import javax.annotation.PostConstruct;

/**
 * Created by cbadenes on 11/01/16.
 */
@Component
public class SparkHelper {

    private static final Logger LOG = LoggerFactory.getLogger(SparkHelper.class);

    @Value("${librairy.modeler.threads}")
    String threads; // 2

    @Value("${librairy.modeler.memory}")
    String memory; // 3g

    private SparkConf conf;

    @Getter
    private JavaSparkContext sc;


    @PostConstruct
    public void setup(){

        int processors = Runtime.getRuntime().availableProcessors();

        int mb = 1024*1024;

        long maxMemory = Runtime.getRuntime().maxMemory();

        long memPerProcess = maxMemory / mb / processors;



        // Initialize Spark Context
        this.conf = new SparkConf().
                setMaster("local["+processors+"]").
                setAppName("librairy-LDA-Modeler").
                set("spark.executor.memory", memPerProcess + "m").
                set("spark.driver.maxResultSize","0");

        LOG.info("Spark configured with " +  processors + " processors and " +  memPerProcess+"m per process");

        sc = new JavaSparkContext(conf);
    }

}
