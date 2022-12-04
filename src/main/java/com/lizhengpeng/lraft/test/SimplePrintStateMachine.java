package com.lizhengpeng.lraft.test;

import com.lizhengpeng.lraft.core.Snapshot;
import com.lizhengpeng.lraft.core.SnapshotWriter;
import com.lizhengpeng.lraft.core.StateMachine;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.nio.charset.StandardCharsets;

public class SimplePrintStateMachine implements StateMachine {

    public static final SimplePrintStateMachine INSTANCE = new SimplePrintStateMachine();

    private static final Logger logger = LoggerFactory.getLogger(SimplePrintStateMachine.class);

    @Override
    public void apply(String command, Long logIndex) {
        logger.info("状态机执行  => {}  raftLog索引 => {}", command, logIndex);
    }

    @Override
    public void writeSnapshot(SnapshotWriter snapshotWriter) {
        logger.info("状态机执行 => snapshot");
        for (int index = 0;index < 100;index++) {
            snapshotWriter.write("hello raft snapshot".getBytes(StandardCharsets.UTF_8));
        }
    }

    @Override
    public void readSnapshot(Snapshot snapshot) {
        logger.info("状态机执行 => snapshot");
    }

}
