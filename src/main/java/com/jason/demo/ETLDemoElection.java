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
 * 最後即能得知，三個候選人分別的得票數，與相對統計資料
 */
public class ETLDemoElection {
    public static void main(String[] args) {

        // 宣告一個List，裝所有的階段
        List<AbstractPhase> phases = new ArrayList<>();

        // 1. 裝第一個phase，投票階段，這邊設定1000張票
        phases.add(new ExtractPhase("extract", 1000 ));

        // 2. 裝第二個phase，分類階段
        phases.add(new TransformPhase("transform"));

        // 3. 裝第三個phase，計算階段
        phases.add(new LoadPhase("load"));

        // 宣告一個新的ETL Runner來跑這些階段
        ETLRunner runner = new ETLRunner("election demo", phases);

        // 跑起來，就開始了
        runner.run();
    }
}
