package com.jason.app.demo;

import com.jason.app.ETLRunner;
import com.jason.app.phase.AbstractPhase;

import java.util.ArrayList;
import java.util.List;

public class Main {
    public static void main(String[] args) {
        List<AbstractPhase> phaseList = new ArrayList<>();
        phaseList.add(new NewExtract("my phase 1"));
        ETLRunner runner = new ETLRunner(phaseList);
        runner.run();
    }
}
