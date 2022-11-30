package com.lizhengpeng.lraft.core;

import cn.hutool.core.io.FileUtil;
import com.lizhengpeng.lraft.exception.RaftException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.File;
import java.io.IOException;
import java.io.RandomAccessFile;
import java.util.Arrays;
import java.util.Map;
import java.util.TreeMap;

import static com.lizhengpeng.lraft.core.RaftUtils.*;

/**
 * 基于文件的日志管理
 * @author lzp
 */
public class FileLogManager implements LogManager {

    private static final Logger logger = LoggerFactory.getLogger(FileLogManager.class);

    private String logFileDir;

    private String metaFile;

    private long logFileMaxSize;

    private TreeMap<Long, LogFile> logTreeMap = new TreeMap<>();

    private RaftMeta raftMeta;

    private RaftOptions raftOptions;

    /**
     * 默认的构造函数加载raft日志文件
     * @param raftOptions
     * @throws RaftException
     */
    public FileLogManager(RaftOptions raftOptions) throws RaftException {
        this.raftOptions = raftOptions;
        this.logFileMaxSize = raftOptions.logFileMaxSize; // 单个文件的最大写入尺寸
        this.logFileDir = raftOptions.logDir + File.separator + "log";
        this.metaFile = raftOptions.logDir + File.separator + "meta";
        loadLogFile(logFileDir);
        loadRaftMeta();
    }

    /**
     * 获取元数据信息
     * @return
     */
    public RaftMeta getRaftMeta() {
        return raftMeta;
    }

    /**
     * 获取raft集群的相关元信息
     * @return
     */
    public void loadRaftMeta(){
        try {
            File metaFile = new File(this.metaFile);
            if (!metaFile.exists() && !metaFile.createNewFile()) {
                throw new RaftException("create meta file failed");
            }
            if (metaFile.length() == 0) {
                raftMeta = new RaftMeta();
            } else {
                RandomAccessFile accessFile = new RandomAccessFile(metaFile, "r");
                int length = accessFile.readInt();
                byte[] buffer = new byte[length];
                int hasRead = accessFile.read(buffer);
                if (hasRead != length) {
                    throw new RaftException("read raft meta failed");
                }
                raftMeta = RaftUtils.readObj(buffer, RaftMeta.class);
                try {
                    accessFile.close();
                } catch (Exception e) {
                    logger.info("close raft meta access file exception", e);
                }
            }
        } catch (Exception e) {
            throw new RaftException("load raft meta exception", e);
        }
    }

    /**
     * 持久化raftMeta相关信息
     */
    public synchronized void updateRaftMeta() {
        try {
            RandomAccessFile accessFile = new RandomAccessFile(metaFile, "rw");
            byte[] buffer = RaftUtils.writeObj(raftMeta);
            accessFile.writeInt(buffer.length);
            accessFile.write(buffer);
            accessFile.close();
        } catch (Exception e) {
            throw new RaftException("update raft meta failed", e);
        }
    }

    /**
     * 加载原始的日志文件
     * @param logDirPath
     * @throws Exception
     */
    public void loadLogFile(String logDirPath) {
        try {
            File logDirFile = new File(logDirPath);
            if (!logDirFile.exists() && !logDirFile.mkdirs()) {
                throw new RaftException("create log dir failed");
            }
            File[] logFiles = logDirFile.listFiles();
            if (logFiles != null) {
                Arrays.asList(logFiles).forEach(logFile -> {
                    String fileName = logFile.getName(); // 日志文件的具体名称
                    long firstIndex = Long.parseLong(fileName); // 日志的文件名为保存的第一个日志条目的编号
                    LogFile file = new LogFile(); // 创建日志文件
                    file.setFileName(fileName); // 绑定文件名
                    file.setStartIndex(firstIndex);
                    file.setEndIndex(-1); // 默认表示文件可写
                    file.initLogFile(logFile); // 加载日志文件
                    logTreeMap.put(firstIndex, file);
                });
            }
        } catch (Exception e) {
            throw new RaftException("load log file exception", e);
        }
    }

    /**
     * leader节点直接写入数据到日志中
     * @param entries
     * @return
     */
    @Override
    public long appendLog(String entries) {
        return this.appendLog(raftMeta.getCurrentTerm(), entries);
    }

    /**
     * leader节点直接写入数据到日志中
     * @param entries
     * @return
     */
    @Override
    public long appendLog(Long term, String entries) {
        long logIndex = raftMeta.getLastLogIndex() + 1; // lastLogIndex默认为0从0开始增加但是raft中日志索引从1开始
        LogEntry logEntry = LogEntry.builder()
                .term(term)
                .index(logIndex)
                .entries(entries)
                .build();
        try {
            boolean createNew = false;
            if (logTreeMap.isEmpty()) {
                createNew = true;
            } else {
                LogFile latestLogFile = logTreeMap.lastEntry().getValue();
                if (latestLogFile == null || latestLogFile.getFileSize() >= logFileMaxSize) { // 文件尺寸已经达到了阈值不能再继续进行写入
//                    latestLogFile.closeFile();
                    createNew = true;
                }
            }
            LogFile logFile;
            if (createNew) {
                File newLogFile = new File(logFileDir + File.separator + String.format("%030d", logIndex));
                if (!newLogFile.createNewFile()) {
                    logger.warn("create log file => {} failed", String.format("%030d", logIndex));
                    throw new RaftException("create log file exception");
                }
                logFile = new LogFile(); // 创建日志文件
                logFile.setFileName(newLogFile.getName()); // 绑定文件名
                logFile.setStartIndex(logIndex); // 起始的索引
                logFile.setEndIndex(-1); // 默认表示文件可写
                logFile.initLogFile(newLogFile); // 加载日志文件
                logTreeMap.put(logIndex, logFile);
            } else {
                logFile = logTreeMap.lastEntry().getValue(); // 直接使用当前已存在的日志文件
            }
            logFile.appendLogEntry(logEntry);
            logFile.setEndIndex(logIndex); // 更新最后一条日志的索引
            // 更新最后一条日志的索引
            raftMeta.setLastLogIndex(logIndex);
            updateRaftMeta();
            return logIndex; // 返回当前写入的日志的索引位置(这样状态机可以选择在应用到这个日志索引之后再返回表示日志复制到了大部分的节点)
        } catch (Exception e) {
            throw new RaftException("append log to raft exception", e);
        }
    }

    /**
     * 接收从leader发送来的日志数据
     * @param preLogTerm
     * @param preLogIndex
     * @param raftLog
     * @return
     */
    @Override
    public boolean replicateLog(Long preLogTerm, Long preLogIndex, LogEntry raftLog) {
        try {
            // 如果是整个集群的第一条日志则直接添加即可
            if (compare(preLogTerm, 0L) && compare(preLogIndex, 0L)) {
                // 日志文件非空则需要进行删除复制第一条数据一定是清空的状态
                cleanRaftLog(-1L); // 清空所有的日志数据
                this.appendLog(raftLog.getTerm(), raftLog.getEntries());
                return true;
            } else {
                // 判断日志是否匹配如果不匹配则需要进行回退
                LogEntry logEntry = this.getLogEntry(preLogIndex);
                if (logEntry == null || !compare(logEntry.getTerm(), preLogTerm)) {
                    return false;
                } else {
                    // 存在匹配日志项目的情况下需要判断
                    // preLogIndex与最后的日志项索引匹配则应该直接添加
                    if (preLogIndex == raftMeta.getLastLogIndex()) {
                        this.appendLog(raftLog.getTerm(), raftLog.getEntries());
                        return true;
                    } else {
                        // 日志在preLogIndex的位置匹配则需要删除preLogIndex之后的所有日志(可能存在)
                        // raft是强领导者模型一切日志以leader为准
                        // 先清除不一致的数据再进行同步
                        cleanRaftLog(preLogIndex + 1);
                        this.appendLog(raftLog.getTerm(), raftLog.getEntries()); // 进行日志的添加操作
                    }
                    return true;
                }
            }
        } catch (Exception e) {
            throw new RaftException("replicate log exception", e);
        }
    }

    /**
     * 清理与leader不同步的日志数据
     * @param logIndex
     */
    private void cleanRaftLog(long logIndex) throws IOException {
        if (logIndex > raftMeta.getLastLogIndex()) {
            return;
        }
        while(!logTreeMap.isEmpty()) {
            Map.Entry<Long, LogFile> entry = logTreeMap.lastEntry();
            if (logIndex < entry.getKey()) { // key保存的是每个日志文件的第一条日志条目的索引
                // 如果logIndex小于第一条日志条目的索引则表示整个文件都不匹配
                // 因此需要整个文件都删除
                entry.getValue().closeFile(); // 关闭文件内部的randomAccess对象
                File oldFile = new File(logFileDir + File.separator + entry.getValue().getFileName());
                FileUtil.del(oldFile);
                logTreeMap.remove(entry.getKey()); // 清除内存中的文件引用
                // 更新数据信息
                raftMeta.setLastLogIndex(entry.getKey() - 1); // index是递增的(如果全部清空了此时lastLogIndex为0符合预期)
                updateRaftMeta();
            } else {
                entry.getValue().removeFrom(logIndex); // 清空指定的列表
                // 删除多余的日志条目
                raftMeta.setLastLogIndex(logIndex - 1);
                updateRaftMeta();
                break;
            }
        }
    }

    /**
     * 获取指定的日志条目
     * @param index
     * @return
     */
    @Override
    public LogEntry getLogEntry(Long index) {
        if (logTreeMap.isEmpty()) {
            return null;
        }
        LogFile logFile = logTreeMap.floorEntry(index) != null ? logTreeMap.floorEntry(index).getValue() : null;
        if (logFile == null) {
            return null;
        }
        return logFile.getLogEntry(index);
    }

    /**
     * 获取最后一个日志条目
     * @return
     */
    @Override
    public LogEntry getLastLog() {
        LogEntry lastLog = this.getLogEntry(raftMeta.getLastLogIndex());
        if (lastLog == null) {
            lastLog = LogEntry.builder()
                    .term(0L)
                    .index(0L)
                    .build();
        }
        return lastLog;
    }

}
