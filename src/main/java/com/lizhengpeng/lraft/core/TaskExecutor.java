package com.lizhengpeng.lraft.core;

import java.util.concurrent.*;

/**
 * 单线程的任务处理器将异步操作转为同步的操作
 * @author lzp
 */
public class TaskExecutor {

    private static final ScheduledExecutorService taskExecutor = Executors.newSingleThreadScheduledExecutor(r -> new Thread(r, "task schedule thread"));

    public Future<?> submit(Runnable runnable) {
        return taskExecutor.submit(runnable);
    }

    public ScheduledFuture<?> submit(Runnable runnable, int timeOut) {
        return taskExecutor.schedule(runnable, timeOut, TimeUnit.MILLISECONDS);
    }

}
