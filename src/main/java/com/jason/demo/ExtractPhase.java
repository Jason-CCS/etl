package com.jason.demo;

import com.jason.phase.AbstractPhase;
import com.jason.threads.ETLCallable;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import java.util.Random;
import java.util.concurrent.*;
import java.util.concurrent.atomic.AtomicInteger;

/**
 * Created by jasonchang on 2018/9/15.
 * <p>
 * Generally, "extract phase" is one of three processes to extract data from database(s).
 * This demo is for showing how to write an extract phase when extending to AbstractPhase class.
 * Here, assume an election event, and use extract phase to randomly generate three candidates:
 * A, B, C, and three areas: TP, TC, KS.
 * For example: TP B; this means a vote from area TP for candidate B.
 * Use this example to simulate a single datum extracted from database.
 * And by generating lots of them by this way to imitate many data is extract from database.
 */
public class ExtractPhase extends AbstractPhase {
    private static final Logger log = LogManager.getLogger(ExtractPhase.class);
    private int totalVotes;
    private ExecutorService es;

    public ExtractPhase(String phaseName, int totalVotes) {
        super(phaseName);
        this.totalVotes = totalVotes;

        /*
         * This framework use a Hashmap containing Key(any type) and BlockingQueue as value.
         * You have to use this to convert the data to next phase.
         * Use blockingQueue.put() to put data and blockingQueue.take() to take data and do other processing in next phase.
         * As codes below, plz put queues with the name you want, here name is "q1"
         * and only use one BlockingQueue for next phase to take. You can write more if necessary.
         * Each AbstractPhase has a private HashMap containing blockingQueue.
         * So just get it by this.getBqMap() while you extend AbstractPhase class and new your any type of queue which
         * implements interface BlockingQueue.
         */
        this.getBqMap().put("q1", new LinkedBlockingQueue());
    }

    /**
     * Each phase has three functions to finish a whole process in a single phase: init(), mainwork(), finish().
     * Write any code you think they should be put in initialization, init().
     * No any special restriction for these three functions, just write whatever making sense to you.
     */
    @Override
    public void init() {
        log.debug("in init()");

        /*
         * initialize any important setting before running mainwork
         * This example show initialize a ExecutorService only.
         */
        this.es = Executors.newCachedThreadPool();
    }

    @Override
    public void mainwork() {
        log.debug("in mainwork()");
        AtomicInteger amount = new AtomicInteger(0);
        // get the blockingQueue from bqMap which is natively hold by AbstractPhase
        BlockingQueue bq = this.getBqMap().get("q1");
        // use CompletionService to separate the task generating data and the task putting data into queue
        CompletionService cs = new ExecutorCompletionService(es);

        while (amount.get() < totalVotes) {
            try {
                cs.submit(new voteGenerator("task " + amount.incrementAndGet(), this));
            } catch (Exception e) {
                e.printStackTrace();
            }
        }

        for (int i = 0; i < totalVotes; i++) {
            try {
                bq.put(cs.take().get());
            } catch (Exception e) {
                e.printStackTrace();
            }
        }
    }

    /**
     * just terminate ExecutorService
     */
    @Override
    public void finish() {
        es.shutdownNow();
    }

    /**
     * The whole ETL(Extract, Transform, Load) is a work flow from first phase to the last phase.
     * Multiple data are convert from one to another.
     * ETLRunner will run init(), mainwork(), and finish() for you in the order.
     * However, if you run multi-threads in any function, you have to extends ETLRunnable and ETLCallable which are
     * adapted to notify ETL framework that all of threads finished their work in a single function.
     * Then, the phase can get notified and call next function automatically.
     * Below is a thread extends ETLCallable and is a vote generator to produce a random vote.
     */
    static class voteGenerator extends ETLCallable {
        private Random rand = new Random();
        private String[] area = {"TP", "TC", "KS"};
        private String[] candidates = {"A", "B", "C"};

        public voteGenerator(String taskName, AbstractPhase inWhichPhase) {
            super(taskName, inWhichPhase);
        }

        @Override
        public Object doCall() {
            try {
                TimeUnit.MILLISECONDS.sleep(rand.nextInt(5000));
            } catch (InterruptedException e) {
                e.printStackTrace();
            }
            String votedArea = area[rand.nextInt(area.length)];
            String voted = candidates[rand.nextInt(candidates.length)];
            log.debug(votedArea.concat(" ").concat(voted));

            return votedArea.concat(" ").concat(voted);
        }

        @Override
        public String toString() {
            return this.getTaskName();
        }
    }
}
