package com.jason.demo;

import com.jason.phase.AbstractPhase;
import com.jason.phase.Status;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import java.util.concurrent.BlockingQueue;
import java.util.concurrent.LinkedBlockingQueue;

/**
 * Created by jasonchang on 2018/9/16.
 * <p>
 * "Transform phase" is used to describe the middle phase of ETL.
 * Generally, take the data from "extract phase" and do some data processing, such as data reassemble, word processing,
 * or data computation.
 * Here, in this example, TransformPhase get the blockingQueue from extract phase, and take the data
 * from blockingQueue. Then, categorize each vote into three category: how many vote for A, how many vote for B, and
 * how many vote for C. Therefore, I will use three blockingQueue respectively to contains votes for three candidates.
 */
public class TransformPhase extends AbstractPhase {
    private static final Logger log = LogManager.getLogger(TransformPhase.class);
    private BlockingQueue bq;

    public TransformPhase(String phaseName) {
        super(phaseName);
        this.getBqMap().put("aQ", new LinkedBlockingQueue());
        this.getBqMap().put("bQ", new LinkedBlockingQueue());
        this.getBqMap().put("cQ", new LinkedBlockingQueue());
    }

    @Override
    public void init() {
        AbstractPhase extractPhase = this.getEtlContext().getPhase("extract");
        this.bq = extractPhase.getBqMap().get("q1");
    }

    @Override
    public void mainwork() {
        BlockingQueue aQueue = this.getBqMap().get("aQ");
        BlockingQueue bQueue = this.getBqMap().get("bQ");
        BlockingQueue cQueue = this.getBqMap().get("cQ");

        do {
            try {
                if (!bq.isEmpty()) {
                    String voteStr = (String) bq.take();
                    String[] voteAry = voteStr.split(" ");
                    String area = voteAry[0];
                    String who = voteAry[1];

                    switch (who) {
                        case "A":
                            aQueue.put(area);
                            break;
                        case "B":
                            bQueue.put(area);
                            break;
                        default:
                            cQueue.put(area);
                    }
                }
            } catch (InterruptedException e) {
                e.printStackTrace();
            }
        } while (!this.getEtlContext().getPhase("extract").getStatus().equals(Status.AFTER_FINISH) || !bq.isEmpty());
    }

    @Override
    public void finish() {

    }
}
