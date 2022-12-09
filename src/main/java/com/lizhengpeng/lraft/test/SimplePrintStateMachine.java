package com.lizhengpeng.lraft.test;

import com.lizhengpeng.lraft.core.StateMachine;
import com.lizhengpeng.lraft.core.Status;
import com.lizhengpeng.lraft.core.Task;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class SimplePrintStateMachine implements StateMachine {

    public static final SimplePrintStateMachine INSTANCE = new SimplePrintStateMachine();

    private static final Logger logger = LoggerFactory.getLogger(SimplePrintStateMachine.class);

    @Override
    public void apply(Task task, Long logIndex) {
        logger.info("状态机执行  => {}  raftLog索引 => {}", task, logIndex);
        if (task.getRpcClient() != null) {
            task.getRpcClient().sendMessage(Status.success("写入数据成功"));
        }
    }

}
