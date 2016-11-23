/*
 * Copyright (c) 2016. Universidad Politecnica de Madrid
 *
 * @author Badenes Olmedo, Carlos <cbadenes@fi.upm.es>
 *
 */

package org.librairy.modeler.utils;

import org.junit.Test;
import org.librairy.modeler.lda.utils.UnifiedExecutor;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * @author Badenes Olmedo, Carlos <cbadenes@fi.upm.es>
 */
public class UnifiedExecutorTest {

    private static final Logger LOG = LoggerFactory.getLogger(UnifiedExecutorTest.class);

    @Test
    public void simple() throws InterruptedException {

        UnifiedExecutor executor = new UnifiedExecutor();
        executor.setup();



        for (int i =0; i < 10; i++){
            final Integer id = i;

            Thread t = new Thread(new Runnable() {
                @Override
                public void run() {
                    LOG.info("->Thread " + id + " ready to execute task");
                    executor.execute(() -> {
                        LOG.info("## Task from " + id + " executing");
                        try {
                            Thread.currentThread().sleep(20000);
                        } catch (InterruptedException e) {
                            e.printStackTrace();
                        }
                    });
                    LOG.info("-> Executed thread " + id);
                }
            });
            t.run();
        }

        LOG.info("sleeping");
        Thread.currentThread().sleep(60000);
        LOG.info("wake up!");

    }

}
