package com.lizhengpeng.lraft.core;

/**
 * 状态机接口
 * @author lzp
 */
public interface StateMachine {

    /**
     * 状态机提交任务
     * @param task
     * @param logIndex
     */
    void apply(Task task, Long logIndex);

}
