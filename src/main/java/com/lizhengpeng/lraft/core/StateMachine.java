package com.lizhengpeng.lraft.core;

/**
 * 状态机接口
 * @author lzp
 */
public interface StateMachine {

    public void apply(String command);

}
