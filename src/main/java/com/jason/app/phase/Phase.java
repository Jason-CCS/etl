package com.jason.app.phase;

import java.util.concurrent.Callable;

/**
 * Created by user on 2015/12/14.
 */
public interface Phase<V> extends Callable<V> {
    void init();

    void mainwork();

    void finish();
}
