package com.jason;

import com.jason.phase.AbstractPhase;

import java.util.List;
import java.util.Map;
import java.util.concurrent.*;

/**
 * The <code>ETLRunner</code> is a main runner for running ETL framework.
 * You should give {@link AbstractPhase} list via <code>ETLRunner</code> constructor.
 * ETLRunner will run all the processes which you define in {@link AbstractPhase},
 * including init(), mainwork(), and finish().
 * <br>
 * After all submitted {@link AbstractPhase} are finished, <code>ETLRunner</code> as
 * a <code>Runnable</code> will shutdown itself.
 *
 * @author Jason Chang
 * @see AbstractPhase
 * @since 0.2
 */
public class ETLRunner implements Runnable {
    private ETLContext etlContext = new ETLContext();
    private String etlRunnerName;
    private List<AbstractPhase> phaseList;
    private ExecutorService es;
    private Map<String, List<BlockingQueue>> bqListMap;

    public ETLRunner(List<AbstractPhase> phaseList) {
        this("no name provided", phaseList);
    }

    public ETLRunner(String etlRunnerName, List<AbstractPhase> phaseList) {
        this.etlRunnerName = etlRunnerName;
        this.phaseList = phaseList;
        this.initETLRunner();
    }

    private void initETLRunner() {
        for (AbstractPhase p : this.phaseList) {
            p.setEtlContext(this.etlContext);
            this.etlContext.putPhase(p.getPhaseName(), p);
        }
    }

    private void execute() {
        this.es = Executors.newCachedThreadPool();
        CompletionService<AbstractPhase> cs = new ExecutorCompletionService<>(this.es);
        for (AbstractPhase p : this.phaseList) {
            cs.submit(p);
        }

        for (int i = 0; i < this.phaseList.size(); i++) {
            try {
                cs.take();
            } catch (InterruptedException e) {
                e.printStackTrace();
            }
        }

        finishETLRunner();
    }

    private void finishETLRunner() {
        this.es.shutdownNow();
    }

    public List<AbstractPhase> getPhaseList() {
        return phaseList;
    }

    public void setPhaseList(List<AbstractPhase> phaseList) {
        this.phaseList = phaseList;
    }

    public ETLContext getEtlContext() {
        return etlContext;
    }

    public void setEtlContext(ETLContext etlContext) {
        this.etlContext = etlContext;
    }

    public String getEtlRunnerName() {
        return etlRunnerName;
    }

    public void setEtlRunnerName(String etlRunnerName) {
        this.etlRunnerName = etlRunnerName;
    }

    public void run() {
        this.execute();
    }
}
