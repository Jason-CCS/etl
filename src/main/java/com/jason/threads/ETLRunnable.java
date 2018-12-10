package com.jason.threads;

import com.jason.phase.AbstractPhase;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import java.util.concurrent.Phaser;

/**
 * Created by user on 2015/12/14.
 */
public abstract class ETLRunnable implements Runnable {
    private static final Logger log = LogManager.getLogger(ETLRunnable.class);
    private String taskName;
    private AbstractPhase whichPhase;
    private Phaser phaser;

    public ETLRunnable(String taskName, AbstractPhase inWhichPhase) {
        this.taskName = taskName;
        this.whichPhase=inWhichPhase;
        this.phaser = this.whichPhase.getPhaser();
        this.phaser.register();
    }

    public abstract void doRun();

    public void run() {
        try {
            doRun();
        } finally {
            log.debug("before de-register, phase: " + this.phaser.getPhase() + ", parties: " +
                    this.phaser.getRegisteredParties() + ", arrived: " + this.phaser.getArrivedParties());
            this.phaser.arriveAndDeregister();
            log.debug("after de-register, phase: " + this.phaser.getPhase() + ", parties: " +
                    this.phaser.getRegisteredParties() + ", arrived: " + this.phaser.getArrivedParties());
        }
    }

    public String getTaskName() {
        return taskName;
    }

    public void setTaskName(String taskName) {
        this.taskName = taskName;
    }

    public AbstractPhase getWhichPhase() {
        return whichPhase;
    }

    public void setWhichPhase(AbstractPhase whichPhase) {
        this.whichPhase = whichPhase;
    }
}
