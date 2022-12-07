package com.lizhengpeng.lraft.test;

import com.lizhengpeng.lraft.core.FileLogManager;
import com.lizhengpeng.lraft.core.LogEntry;
import com.lizhengpeng.lraft.core.RaftOptions;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class FileLogTest {

    private static final Logger logger = LoggerFactory.getLogger(FileLogTest.class);

    public static void main(String[] args) {
        testReplicateLog();
    }

    /**
     * 测试日志复制
     */
    public static void testReplicateLog() {
        RaftOptions raftOptions = new RaftOptions();
        raftOptions.setLogDir("C:\\raft_dir");
        FileLogManager logManager = new FileLogManager(raftOptions);
        logger.info("append log => {}", logManager.appendLog("hello world"));
        logger.info("append log => {}", logManager.appendLog("hello world"));
        logger.info("append log => {}", logManager.appendLog("hello world"));
        // 复制一条日志
        logger.info(logManager.replicateLog(0, 3, LogEntry.builder().term(0).index(4).entries("hello").build()) + "");
        logger.info(logManager.replicateLog(0, 3, LogEntry.builder().term(0).index(4).entries("hello").build()) + "");
        logger.info(logManager.replicateLog(0, 4, LogEntry.builder().term(0).index(5).entries("hello").build()) + "");
        logger.info(logManager.replicateLog(0, 3, LogEntry.builder().term(0).index(4).entries("hello").build()) + "");
        logger.info(logManager.replicateLog(0, 3, LogEntry.builder().term(0).index(4).entries("hello").build()) + "");
        logger.info(logManager.replicateLog(0, 4, LogEntry.builder().term(0).index(5).entries("hello").build()) + "");
        logger.info(logManager.replicateLog(0, 5, LogEntry.builder().term(0).index(6).entries("hello").build()) + "");
        logger.info(logManager.replicateLog(0, 6, LogEntry.builder().term(0).index(7).entries("hello").build()) + "");
        logger.info(logManager.replicateLog(0, 7, LogEntry.builder().term(0).index(8).entries("hello").build()) + "");
        logManager.cleanPrefix(5l);
        logManager.cleanSuffix(8l);
        logger.info(logManager.replicateLog(0, 3, LogEntry.builder().term(0).index(4).entries("hello").build()) + "");
        logger.info(logManager.replicateLog(0, 3, LogEntry.builder().term(0).index(4).entries("hello").build()) + "");
        logger.info(logManager.replicateLog(0, 4, LogEntry.builder().term(0).index(5).entries("hello").build()) + "");
        logger.info("append log => {}", logManager.appendLog("hello world"));
        logger.info("append log => {}", logManager.appendLog("hello world"));

    }

    /**
     * 测试日志写入
     */
    public void testAppendLog() {
        RaftOptions raftOptions = new RaftOptions();
        raftOptions.setLogDir("C:\\raft_dir");
        FileLogManager logManager = new FileLogManager(raftOptions);
        for (int index = 1;index <= 10000;index++) {
            logger.info("append log => {}", logManager.appendLog("hello world"));
        }
        logManager.cleanSuffix(666);
        logger.info("append log => {}", logManager.appendLog("hello world"));
        logger.info("append log => {}", logManager.appendLog("hello world"));
        logManager.cleanPrefix(10);
        logger.info("append log => {}", logManager.appendLog("hello world"));
        logger.info("append log => {}", logManager.appendLog("hello world"));
        logger.info("append log => {}", logManager.appendLog("hello world"));
        logger.info("append log => {}", logManager.appendLog("hello world"));
        logger.info("append log => {}", logManager.appendLog("hello world"));
        logger.info("append log => {}", logManager.appendLog("hello world"));
        logManager.cleanPrefix(17);
        logManager.cleanSuffix(250);
        logger.info("append log => {}", logManager.appendLog("hello world"));
        logger.info("append log => {}", logManager.appendLog("hello world"));
        logManager.cleanPrefix(-1);
        logger.info("append log => {}", logManager.appendLog("hello world"));
        logger.info("append log => {}", logManager.appendLog("hello world"));
        logManager.cleanSuffix(-1);
        logger.info("append log => {}", logManager.appendLog("hello world"));
        logger.info("append log => {}", logManager.appendLog("hello world"));
        logger.info("append log => {}", logManager.appendLog("hello world"));
        logger.info("append log => {}", logManager.appendLog("hello world"));
        logManager.cleanPrefix(2);
        logManager.cleanPrefix(3);
        logManager.cleanSuffix(0l);
    }

}
