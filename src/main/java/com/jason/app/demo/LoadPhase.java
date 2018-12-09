package com.jason.app.demo;

import com.jason.app.phase.AbstractPhase;
import com.jason.app.phase.Status;
import com.jason.app.threads.ETLCallable;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import java.util.concurrent.BlockingQueue;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;

/**
 * Created by jasonchang on 2018/9/16.
 * <p>
 * "Load phase" is the last phase of ETL concept.
 * Usually it is used for data import into multiple databases.
 * In this demo, LoadPhase take data from three blockingQueue which are set in transform phase.
 * And try to do some count-up calculation and print out into console just as importing into database.
 * Mostly, some statistics of election are concerned by candidates and voters:
 * 1. rank of candidates and their amount of votes.
 * 2. the occupation rate of each area in single candidates
 * 3. the occupation rate of each candidate in single area
 */
public class LoadPhase extends AbstractPhase {
    private static final Logger log = LogManager.getLogger(TransformPhase.class);
    private ExecutorService es;

    public LoadPhase(String phaseName) {
        super(phaseName);
    }

    @Override
    public void init() {
        this.es = Executors.newCachedThreadPool();
    }

    @Override
    public void mainwork() {
        AbstractPhase transformPhase = this.getEtlContext().getPhase("transform");
        BlockingQueue aQ = transformPhase.getBqMap().get("aQ");
        BlockingQueue bQ = transformPhase.getBqMap().get("bQ");
        BlockingQueue cQ = transformPhase.getBqMap().get("cQ");

        /**
         * Future below is actually a String combined with "amount of votes in one queue/amount of votes by TP/
         * amount of votes by TC/amount of votes by KS
         */
        Future fa = es.submit(new voteCounter("count for A", this, aQ));
        Future fb = es.submit(new voteCounter("count for B", this, bQ));
        Future fc = es.submit(new voteCounter("count for C", this, cQ));

        try {
            String resultVoteForA = (String) fa.get();
            String[] strAryA = resultVoteForA.split("/");
            log.info("Amount for candidate A is {}", strAryA[0]);
            String resultVoteForB = (String) fb.get();
            String[] strAryB = resultVoteForB.split("/");
            log.info("Amount for candidate B is {}", strAryB[0]);
            String resultVoteForC = (String) fc.get();
            String[] strAryC = resultVoteForC.split("/");
            log.info("Amount for candidate C is {}", strAryC[0]);

            log.info("Statistics for candidate A is: TP area is {}%, TC area is {}%, KS area is {}%.",
                    Double.parseDouble(strAryA[1]) / Double.parseDouble(strAryA[0]) * 100,
                    Double.parseDouble(strAryA[2]) / Double.parseDouble(strAryA[0]) * 100,
                    Double.parseDouble(strAryA[3]) / Double.parseDouble(strAryA[0]) * 100);

            log.info("Statistics for candidate B is: TP area is {}%, TC area is {}%, KS area is {}%.",
                    Double.parseDouble(strAryB[1]) / Double.parseDouble(strAryB[0]) * 100,
                    Double.parseDouble(strAryB[2]) / Double.parseDouble(strAryB[0]) * 100,
                    Double.parseDouble(strAryB[3]) / Double.parseDouble(strAryB[0]) * 100);

            log.info("Statistics for candidate C is: TP area is {}%, TC area is {}%, KS area is {}%.",
                    Double.parseDouble(strAryC[1]) / Double.parseDouble(strAryC[0]) * 100,
                    Double.parseDouble(strAryC[2]) / Double.parseDouble(strAryC[0]) * 100,
                    Double.parseDouble(strAryC[3]) / Double.parseDouble(strAryC[0]) * 100);

            Double resultForTP = Double.parseDouble(strAryA[1]) + Double.parseDouble(strAryB[1]) + Double.parseDouble(strAryC[1]);
            Double resultForTC = Double.parseDouble(strAryA[2]) + Double.parseDouble(strAryB[2]) + Double.parseDouble(strAryC[2]);
            Double resultForKS = Double.parseDouble(strAryA[3]) + Double.parseDouble(strAryB[3]) + Double.parseDouble(strAryC[3]);
            log.info("Statistics for area TP is: A accounts for {}%, B accounts for {}%, C accounts for {}%.",
                    Double.parseDouble(strAryA[1]) / resultForTP * 100, Double.parseDouble(strAryB[1]) / resultForTP * 100,
                    Double.parseDouble(strAryC[1]) / resultForTP * 100);
            log.info("Statistics for area TC is: A accounts for {}%, B accounts for {}%, C accounts for {}%.",
                    Double.parseDouble(strAryA[2]) / resultForTC * 100, Double.parseDouble(strAryB[2]) / resultForTC * 100,
                    Double.parseDouble(strAryC[2]) / resultForTC * 100);
            log.info("Statistics for area KS is: A accounts for {}%, B accounts for {}%, C accounts for {}%.",
                    Double.parseDouble(strAryA[3]) / resultForKS * 100, Double.parseDouble(strAryB[3]) / resultForKS * 100,
                    Double.parseDouble(strAryC[3]) / resultForKS * 100);

        } catch (Exception e) {
            e.printStackTrace();
        }

    }

    @Override
    public void finish() {
        this.es.shutdownNow();
    }

    static class voteCounter extends ETLCallable {
        private BlockingQueue q;

        public voteCounter(String taskName, AbstractPhase inWhichPhase, BlockingQueue queue) {
            super(taskName, inWhichPhase);
            this.q = queue;
        }

        @Override
        public Object doCall() {
            AbstractPhase transformPhase = this.getWhichPhase().getEtlContext().getPhase("transform");
            int amount = 0, tpCounter = 0, tcCounter = 0, ksCounter = 0;
            try {
                do {
                    if (!q.isEmpty()) {
                        String area = (String) q.take();
                        log.debug(this.getTaskName() + ", area is " + area);
                        amount++;
                        switch (area) {
                            case "TP":
                                tpCounter++;
                                break;
                            case "TC":
                                tcCounter++;
                                break;
                            default:
                                ksCounter++;
                        }
                    }
                } while (!transformPhase.getStatus().equals(Status.AFTER_FINISH) || !q.isEmpty());
            } catch (InterruptedException e) {
                e.printStackTrace();
            }

            return amount + "/" + tpCounter + "/" + tcCounter + "/" + ksCounter;
        }
    }
}
