package com.jason.demo;

import com.jason.ETLRunner;
import com.jason.phase.AbstractPhase;

import java.util.ArrayList;
import java.util.List;

/**
 * Created by jasonchang on 2018/9/15
 * 這是一個投票的Demo，有三個候選人A, B, C，三個投票區TP, TC, KS的模擬投票展示。
 * ExtractPhase模擬1000個人，在三個地區，隨機投給三個候選人，所有的投票結果會匯集到一個queue
 * TransformPhase會從這個queue拿出來，然後唱名是投給誰，然後放進相對應的候選人queue
 * LoadPhase有三個線程，非同步的從queue拿出來，計算此候選人的：總票數/TP地區票數/TC地區票數/KS地區票數
 * 最後即能得知，三個候選人分別的得票數，與各種統計資料
 */
public class ETLDemoElection {
    public static void main(String[] args) {

        List<AbstractPhase> phases = new ArrayList<>();

        phases.add(new ExtractPhase("extract", 1000 ));
        phases.add(new TransformPhase("transform"));
        phases.add(new LoadPhase("load"));
        ETLRunner runner = new ETLRunner("election demo", phases);
        runner.run();

    }
}
