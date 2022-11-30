package com.lizhengpeng.lraft.test;

import com.lizhengpeng.lraft.core.FileLogManager;
import com.lizhengpeng.lraft.core.LogEntry;
import com.lizhengpeng.lraft.core.RaftOptions;

public class FileLogTest {
    public static void main(String[] args) {
        RaftOptions raftOptions = new RaftOptions();
        raftOptions.setLogDir("C:\\raft_dir");
        FileLogManager logManager = new FileLogManager(raftOptions);
        for (int index = 0;index < 10000;index++) {
            logManager.appendLog("hello world" +index);
        }
        System.out.println(logManager.getRaftMeta());
        // 日志复制的情况
        LogEntry logEntry = LogEntry.builder()
                .term(0L)
                .index(128L)
                .entries("jejeje")
                .build();
        logManager.replicateLog(0L,128L, logEntry);
        logManager.replicateLog(0L,64L, logEntry);


    }
}
