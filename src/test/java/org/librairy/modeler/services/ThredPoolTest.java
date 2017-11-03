package org.librairy.modeler.services;

import org.junit.Ignore;
import org.junit.Test;
import org.librairy.modeler.lda.tasks.LDATrainingTask;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.scheduling.concurrent.ThreadPoolTaskScheduler;

import java.text.SimpleDateFormat;
import java.util.Date;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ScheduledFuture;

/**
 * @author Badenes Olmedo, Carlos <cbadenes@fi.upm.es>
 */
public class ThredPoolTest {


    private static final Logger LOG = LoggerFactory.getLogger(ThredPoolTest.class);
    private ConcurrentHashMap<String,ScheduledFuture<?>> buildingTasks = new ConcurrentHashMap<>();
    private ThreadPoolTaskScheduler threadpool = new ThreadPoolTaskScheduler();

    SimpleDateFormat timeFormatter = new SimpleDateFormat("HH:mm:ssZ");


    String domainUri ="domain";
    int poolSize = 5;
    int events = 100;
    Long delay = 2000l;
    Long taskSleep = 10000l;

    @Test
    @Ignore
    public void exceedsSize() throws InterruptedException {



        threadpool.setPoolSize(poolSize);

        threadpool.initialize();


        for (int i=0; i<events; i++){
            new Thread(new UnexpectedEvent()).run();
            Thread.sleep(100);
        }

        LOG.info("waiting for partial interruption..");
        Thread.sleep(taskSleep/2);

        for (int i=0; i<events; i++){
            new Thread(new UnexpectedEvent()).run();
            Thread.sleep(100);
        }

        LOG.info("waiting for finish test..");
        Thread.sleep(30000);
    }




    private class UnexpectedEvent implements Runnable{



        @Override
        public void run() {

            LOG.info("Scheduled creation of a new topic model (LDA) for the domain: " + domainUri + " at " + timeFormatter.format(new Date(System.currentTimeMillis() + delay)));
            ScheduledFuture<?> task = buildingTasks.get(domainUri);
            if (task != null) {
                task.cancel(true);
//            this.threadpool.getScheduledThreadPoolExecutor().purge();
            }
            task = threadpool.schedule(new SampleTask(), new Date(System.currentTimeMillis() + delay));
            buildingTasks.put(domainUri,task);
        }
    }

    private class SampleTask implements Runnable{

        @Override
        public void run() {

            LOG.info("Task executing!!!!!!");

            try {
                Thread.sleep(taskSleep);
            } catch (InterruptedException e) {
                LOG.warn("Task cancelled");
            }

        }
    }
}
