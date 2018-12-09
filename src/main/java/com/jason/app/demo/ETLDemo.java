package com.jason.app.demo;

import com.jason.app.ETLRunner;
import com.jason.app.phase.AbstractPhase;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import java.util.ArrayList;
import java.util.List;

/**
 * Hello world!
 */
public class ETLDemo {
    private static final Logger log= LogManager.getLogger(ETLDemo.class);
    public static void main(String[] args) {
        log.info("-------start-------");
        Extract ep=new Extract("e1");
        List<AbstractPhase> phaseList=new ArrayList<>();
        phaseList.add(ep);
        ETLRunner etlRunner=new ETLRunner("My Runner",phaseList);
        etlRunner.run();
    }
}
