package org.librairy.modeler.lda.helper;

import lombok.Getter;
import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaSparkContext;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.stereotype.Component;

import javax.annotation.PostConstruct;
import java.util.Arrays;

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

        String memPerProcess = (maxMemory / mb / processors) + "m";


        int cores = processors * 3;


        // Initialize Spark Context
        this.conf = new SparkConf().
                setMaster("local["+String.valueOf(cores)+"]").
                setAppName("librairy-LDA-Modeler").
                set("spark.executor.memory", memPerProcess).
                set("spark.driver.maxResultSize","0").
                set("spark.default.parallelism",String.valueOf(4*cores)).
                set("spark.executor.cores",String.valueOf(processors)).
                set("spark.serializer", "org.apache.spark.serializer.KryoSerializer")
        ;

        //this.conf.registerKryoClasses(new Class[]{Corpus.class});

        LOG.info("Spark configured with " +  cores + " processors and " +  memPerProcess+"m per process");

        sc = new JavaSparkContext(conf);
    }

}
