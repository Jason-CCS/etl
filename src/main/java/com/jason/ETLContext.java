package com.jason;

import com.jason.phase.AbstractPhase;

import java.util.HashMap;
import java.util.Map;

/**
 * Created by user on 2015/12/14.
 */
public class ETLContext{
    Map<String, AbstractPhase> phaseMap=new HashMap<String, AbstractPhase>();


    public void putPhase(String phaseName, AbstractPhase phase){
        if(phaseMap.containsKey(phaseName))
            throw new RuntimeException("phaseMap in ETLContext already contain this phaseName.");
        phaseMap.put(phaseName,phase);
    }

    public AbstractPhase getPhase(String phaseName){
        return this.phaseMap.get(phaseName);
    }
}
