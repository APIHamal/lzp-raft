package com.lizhengpeng.lraft.core;

import java.util.concurrent.*;

/**
 * 单线程的任务处理器将异步操作转为同步的操作
 * @author lzp
 */
public class TaskExecutor {

    private static final ScheduledExecutorService taskExecutor = Executors.newSingleThreadScheduledExecutor(r -> new Thread(r, "task schedule thread"));

    private static final ScheduledExecutorService replicateExecutor = Executors.newSingleThreadScheduledExecutor(r -> new Thread(r, "task schedule thread"));

    private static final ThreadPoolExecutor threadPoolExecutor = new ThreadPoolExecutor(8, 16, 60, TimeUnit.SECONDS, new ArrayBlockingQueue<>(1024));

    public Future<?> submit(Runnable runnable) {
        return taskExecutor.submit(runnable);
    }

    public <T> Future<T> submit(Callable<T> callable) {
        return taskExecutor.submit(callable);
    }

    public ScheduledFuture<?> submit(Runnable runnable, int timeOut) {
        return taskExecutor.schedule(runnable, timeOut, TimeUnit.MILLISECONDS);
    }

    public ScheduledFuture<?> replicateTask(Runnable runnable, int timeOut) { // 重复的任务
        return replicateExecutor.scheduleWithFixedDelay(runnable, timeOut, timeOut, TimeUnit.SECONDS);
    }

    public void async(Runnable runnable) {
        threadPoolExecutor.submit(runnable);
    }

}