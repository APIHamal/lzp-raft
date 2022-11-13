package com.lizhengpeng.lraft.core;

import java.util.concurrent.Executor;
import java.util.concurrent.Executors;

/**
 * 任务处理器
 * @author lzp
 */
public class TaskExecutor {

    private static final Executor taskExecutor = Executors.newSingleThreadExecutor();

    public void submit(Runnable runnable) {
        taskExecutor.execute(runnable);
    }

}
