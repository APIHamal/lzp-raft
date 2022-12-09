package com.lizhengpeng.lraft.test;

import com.lizhengpeng.lraft.core.FileLogManager;
import com.lizhengpeng.lraft.core.RaftOptions;
import com.lizhengpeng.lraft.core.Task;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class FileLogTest {

    private static final Logger logger = LoggerFactory.getLogger(FileLogTest.class);

    public static void main(String[] args) {
        testAppendLog();
    }

    /**
     * 测试日志写入
     */
    public static void testAppendLog() {
        RaftOptions raftOptions = new RaftOptions();
        raftOptions.setLogDir("C:\\raft_dir");
        FileLogManager logManager = new FileLogManager(raftOptions);
        for (int index = 1;index <= 10000;index++) {
            logger.info("append log => {}", logManager.appendLog(Task.payload("hello world")));
        }
        logManager.cleanSuffix(666);
        logger.info("append log => {}", logManager.appendLog(Task.payload("hello world")));
        logger.info("append log => {}", logManager.appendLog(Task.payload("hello world")));
        logManager.cleanPrefix(10);
        logger.info("append log => {}", logManager.appendLog(Task.payload("hello world")));
        logger.info("append log => {}", logManager.appendLog(Task.payload("hello world")));
        logger.info("append log => {}", logManager.appendLog(Task.payload("hello world")));
        logger.info("append log => {}", logManager.appendLog(Task.payload("hello world")));
        logger.info("append log => {}", logManager.appendLog(Task.payload("hello world")));
        logger.info("append log => {}", logManager.appendLog(Task.payload("hello world")));
        logManager.cleanPrefix(17);
        logManager.cleanSuffix(250);
        logger.info("append log => {}", logManager.appendLog(Task.payload("hello world")));
        logger.info("append log => {}", logManager.appendLog(Task.payload("hello world")));
        logManager.cleanPrefix(-1);
        logger.info("append log => {}", logManager.appendLog(Task.payload("hello world")));
        logger.info("append log => {}", logManager.appendLog(Task.payload("hello world")));
        logManager.cleanSuffix(-1);
        logger.info("append log => {}", logManager.appendLog(Task.payload("hello world")));
        logger.info("append log => {}", logManager.appendLog(Task.payload("hello world")));
        logger.info("append log => {}", logManager.appendLog(Task.payload("hello world")));
        logger.info("append log => {}", logManager.appendLog(Task.payload("hello world")));
        logManager.cleanPrefix(2);
        logManager.cleanPrefix(3);
        logManager.cleanSuffix(0l);
    }

}
