package com.jason.app.demo;

import com.jason.app.ETLRunner;
import com.jason.app.phase.AbstractPhase;

import java.util.ArrayList;
import java.util.List;

/**
 * Created by jasonchang on 2018/9/15.
 */
public class ETLDemoElection {
    public static void main(String[] args) {

        // 投票
        List<AbstractPhase> phases = new ArrayList<>();
        phases.add(new ExtractPhase("extract", 1000 ));
        phases.add(new TransformPhase("transform"));
        phases.add(new LoadPhase("load"));
        ETLRunner runner = new ETLRunner("election demo", phases);
        runner.run();

    }
}
