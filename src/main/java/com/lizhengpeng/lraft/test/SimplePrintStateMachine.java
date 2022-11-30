package com.lizhengpeng.lraft.test;

import com.lizhengpeng.lraft.core.StateMachine;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class SimplePrintStateMachine implements StateMachine {

    public static final SimplePrintStateMachine INSTANCE = new SimplePrintStateMachine();

    private static final Logger logger = LoggerFactory.getLogger(SimplePrintStateMachine.class);

    @Override
    public void apply(String command, Long logIndex) {
        logger.info("状态机执行  => {}  raftLog索引 => {}", command, logIndex);
    }

}
