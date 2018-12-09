package com.jason.app.demo;

import com.jason.app.phase.AbstractPhase;
import com.jason.app.threads.ETLRunnable;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import java.util.Random;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;

/**
 * Created by user on 2015/12/16.
 */
public class Extract extends AbstractPhase {
    private static final Logger log= LogManager.getLogger(Extract.class);
    private ExecutorService es= Executors.newCachedThreadPool();

    public Extract(String phaseName) {
        super(phaseName);
    }

    @Override
    public void init() {
        log.info("======in init()=======");
        log.info("=====new 3 tasks in init====");
        for (int i = 0; i < 3; i++) {
            es.submit(new Task("[initTask "+i+"]", this));
        }

    }

    @Override
    public void mainwork() {
        log.info("======in mianwork()==========");
        log.info("=====new 5 tasks in mainwork====");
        for (int i = 0; i < 5; i++) {
            es.submit(new Task("[mainworkTask " + i + "]", this));
        }
    }

    @Override
    public void finish() {
        log.info("======in finish()==========");
        es.shutdownNow();
    }

    private class Task extends ETLRunnable{


        public Task(String taskName, AbstractPhase inWhichPhase) {
            super(taskName, inWhichPhase);
        }

        @Override
        public void doRun() {
            Random random=new Random();
            int sleepTime=random.nextInt(10);
            log.info("{} is going to sleep for {}...", this.getTaskName(), sleepTime);
            try {
                TimeUnit.SECONDS.sleep(sleepTime);
            } catch (InterruptedException e) {
                e.printStackTrace();
            }
            log.info("{} wake up.", this.getTaskName());
        }
    }
}
