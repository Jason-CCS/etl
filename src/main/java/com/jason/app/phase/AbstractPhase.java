package com.jason.app.phase;

import com.jason.app.ETLContext;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.Callable;
import java.util.concurrent.Phaser;

/**
 * Created by user on 2015/12/14.
 */
public abstract class AbstractPhase implements Callable<AbstractPhase> {
    private static final Logger log = LogManager.getLogger(AbstractPhase.class);
    private Phaser phaser = new Phaser(1);
    private Map<String, BlockingQueue> bqMap = new HashMap<>();
    private Status status = Status.BEFORE_INIT;
    private String phaseName;
    private ETLContext etlContext;

    public AbstractPhase(String phaseName) {
        this.phaseName = phaseName;
    }

    public AbstractPhase call(){
        this.init();
        this.setStatus(Status.INIT_START);
        this.phaser.arriveAndAwaitAdvance();
        // phase: 從0開始, parties: 註冊數, arrived: 到達數; 一旦註冊數==到達數，phase就會加一，arrived歸零
        log.debug("phase: " + this.phaser.getPhase() + ", parties: " +
                this.phaser.getRegisteredParties() + ", arrived: " + this.phaser.getArrivedParties());

        this.mainwork();
        this.setStatus(Status.MAIN_WORK_START);
        this.phaser.arriveAndAwaitAdvance();
        log.debug("phase: " + this.phaser.getPhase() + ", parties: " +
                this.phaser.getRegisteredParties() + ", arrived: " + this.phaser.getArrivedParties());

        this.finish();
        this.setStatus(Status.FINISHING);
        this.phaser.arriveAndDeregister();
        log.debug(this.getPhaseName()+" phase: " + this.phaser.getPhase() + ", parties: " +
                this.phaser.getRegisteredParties() + ", arrived: " + this.phaser.getArrivedParties());
        this.setStatus(Status.AFTER_FINISH);
        return this;
    }

    public Status getStatus() {
        return status;
    }

    public void setStatus(Status status) {
        this.status = status;
    }

    public ETLContext getEtlContext() {
        return etlContext;
    }

    public void setEtlContext(ETLContext etlContext) {
        this.etlContext = etlContext;
    }

    public Phaser getPhaser() {
        return phaser;
    }

    public String getPhaseName() {
        return phaseName;
    }

    public Map<String, BlockingQueue> getBqMap() {
        return bqMap;
    }

    public void setBqMap(Map<String, BlockingQueue> bqMap) {
        this.bqMap = bqMap;
    }

    public abstract void init();

    public abstract void mainwork();

    public abstract void finish();
}
