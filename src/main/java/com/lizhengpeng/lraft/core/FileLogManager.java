package com.lizhengpeng.lraft.core;

import cn.hutool.core.collection.CollUtil;
import cn.hutool.core.util.ArrayUtil;
import com.lizhengpeng.lraft.exception.RaftException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.File;
import java.io.RandomAccessFile;
import java.util.Arrays;
import java.util.TreeMap;

/**
 * 基于文件的日志管理
 * @author lzp
 */
public class FileLogManager implements LogManager {

    private static final Logger logger = LoggerFactory.getLogger(FileLogManager.class);

    private String logDir;

    private String metaFile;

    private long logFileMaxSize;

    private TreeMap<Long, LogFile> logTreeMap = new TreeMap<>();

    private volatile RaftMeta raftMeta;

    /**
     * 默认的构造函数加载raft日志文件
     * @param raftOptions
     * @throws RaftException
     */
    public FileLogManager(RaftOptions raftOptions) throws RaftException {
        logFileMaxSize = raftOptions.getLogFileMaxSize(); // 单个文件的最大写入尺寸
        logDir = raftOptions.getLogDir() + File.separator + "log";
        metaFile = raftOptions.getLogDir() + File.separator + "meta";
        loadLogFile();
        reloadRaftMeta();
    }

    /**
     * 获取元数据信息
     * @return
     */
    public synchronized RaftMeta getRaftMeta() {
        return raftMeta;
    }

    /**
     * 添加日志文件到当前的集合中
     * @param firstIndex
     * @param logFile
     */
    public void loadLogFile(Long firstIndex, LogFile logFile) {
        if (logTreeMap.containsKey(firstIndex)) {
            logger.error("duplicate start index => {}", firstIndex);
            throw new RaftException("duplicate start index");
        }
        logTreeMap.put(firstIndex, logFile);
    }

    /**
     * 删除指定的日志
     * @param firstIndex
     */
    public void unLoadLogFile(Long firstIndex) {
        if (!logTreeMap.containsKey(firstIndex)) {
            logger.error("unLoad log failed index => {}", firstIndex);
            throw new RaftException("unLoad log failed");
        }
        logTreeMap.remove(firstIndex);
    }

    /**
     * 获取raft对象
     */
    public synchronized RaftMeta reloadRaftMeta(){
        try {
            File raftMetaFile = new File(metaFile);
            if (!raftMetaFile.exists() && !raftMetaFile.createNewFile()) {
                logger.info("create meta file => {} failed", metaFile);
                throw new RaftException("create meta file failed");
            }
            // 读取meta文件内容
            RandomAccessFile metaAccessFile = AccessUtils.openRandomAccess(raftMetaFile);
            if (metaAccessFile.length() == 0) { // 注意:firstLogIndex只是用来确定写入的条目的开始索引
                raftMeta = RaftMeta.builder()   // 整个raft集群中第一条日志的索引是1
                        .firstLogIndex(1l)      // 如果存在存在快照时候并且快照的lastLogIndex并没有大于当前
                        .build();               // 日志的lastLogIndex则需要调整firstLogIndex否则大多数
            } else {                            // 情况下都不需要进行调整
                int dataLength = metaAccessFile.readInt();
                byte[] buffer = new byte[dataLength];
                int hasRead = metaAccessFile.read(buffer);
                if (hasRead != dataLength) {
                    throw new RaftException("read raft meta failed");
                }
                raftMeta = RaftUtils.readObj(buffer, RaftMeta.class);
                AccessUtils.closeAccessFile(metaAccessFile);
            }
            return raftMeta;
        } catch (Exception e) {
            logger.info("load raft meta exception");
            throw new RaftException(e);
        }
    }

    /**
     * 持久化raftMeta相关信息
     * 将数据序列化后写入到文件中
     * @param raftMeta
     */
    public synchronized void updateRaftMeta(RaftMeta raftMeta) {
        try {
            File raftMetaFile = new File(metaFile);
            if (!raftMetaFile.exists() && !raftMetaFile.createNewFile()) {
                logger.info("create meta file => {} failed", metaFile);
                throw new RaftException("create meta file failed");
            }
            // 将对象进行序列化后写入文件
            byte[] buffer = RaftUtils.writeObj(raftMeta);
            RandomAccessFile accessFile = AccessUtils.openRandomAccess(raftMetaFile);
            accessFile.writeInt(buffer.length);
            accessFile.write(buffer);
            AccessUtils.closeAccessFile(accessFile);
        } catch (Exception e) {
            logger.info("write raft meta exception");
            throw new RaftException(e);
        }
    }

    /**
     * 加载当前指定目录下的日志文件
     * 文件名对应写入时第一条日志的索引
     */
    public synchronized void loadLogFile() {
        try {
            File logFileDir = new File(logDir);
            if (!logFileDir.exists() && !logFileDir.mkdirs()) {
                throw new RaftException("create log file dir failed");
            }
            File[] logFiles = logFileDir.listFiles();
            if (ArrayUtil.isNotEmpty(logFiles)) {
                Arrays.asList(logFiles).forEach(file -> {
                    String logName = file.getName(); // 日志文件的名称代表第一条条目的索引
                    long startIndex = Long.parseLong(logName);
                    LogFile logFile = new LogFile(this, logDir, logName); // 创建日志文件
                    logFile.setFirstIndex(startIndex);
                    logFile.setLastIndex(0); // 日志写入的最后一条条目的索引
                    logFile.reloadLogFile(); // 加载日志文件
                    logTreeMap.put(startIndex, logFile);
                });
            }
        } catch (Exception e) {
            logger.info("load log file in dir => {} exception", logDir);
            throw new RaftException(e);
        }
    }

    /**
     * leader节点直接写入数据到日志中
     * @param entries
     * @return
     */
    @Override
    public synchronized long appendLog(String entries) {
        // 获取当前的term并进行写入
        return appendLog(reloadRaftMeta().getCurrentTerm(), entries);
    }

    /**
     * leader节点直接写入数据到日志中
     * @param entries
     * @return
     */
    @Override
    public synchronized long appendLog(long term, String entries) {
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
                if (latestLogFile.getFileSize() >= logFileMaxSize) { // 文件尺寸已经达到了阈值不能再继续进行写入
                    latestLogFile.closeFile(); // 关闭文件流
                    createNew = true;
                }
            }
            LogFile logFile;
            if (createNew) {
                String newFileName = String.format("%030d", logIndex); // 文件名长度不足30位的时候使用0进行填充
                File newLogFile = new File(logDir, newFileName);
                if (!newLogFile.createNewFile()) {
                    logger.warn("create new log file => {} failed", String.format("%030d", logIndex));
                    throw new RaftException("create log file exception");
                }
                logFile = new LogFile(this, logDir, newFileName); // 创建日志文件
                logFile.setLogFileName(newFileName); // 绑定文件名
                logFile.setFirstIndex(logIndex); // 起始的索引
                logFile.setLastIndex(0);
                logTreeMap.put(logIndex, logFile);
            } else {
                logFile = logTreeMap.lastEntry().getValue(); // 直接使用当前已存在的日志文件
            }
            logFile.appendLogEntry(logEntry);
            // 更新最后一条日志的索引
            raftMeta.setLastLogIndex(logIndex);
            updateRaftMeta(raftMeta);

            return logIndex; // 返回当前写入的日志的索引位置(这样状态机可以选择在应用到这个日志索引之后再返回表示日志复制到了大部分的节点)
        } catch (Exception e) {
            throw new RaftException("append log to raft exception", e);
        }
    }

    /**
     * 清理与leader不同步的日志数据
     * 这个清理是指节点之间同步的日志存在冲突时
     * 清除与leader节点不同步的日志
     * @param logIndex
     */
    public synchronized boolean cleanSuffix(long logIndex) {
        try {
            // 如果不存在任何的日志
            if (logTreeMap.isEmpty() || logIndex > reloadRaftMeta().getLastLogIndex()) {
                logger.warn("currently does clean any logs");
                return false;
            }
            // 清除所有的文件数据
            if (logIndex <= reloadRaftMeta().getFirstLogIndex()) {
                logTreeMap.values().forEach(item -> item.destroy());
                logTreeMap.clear(); // 清空列表
                // todo 这里要判断是否存在快照如果快照不存在则firstLogIndex始终都是1则lastLogIndex为0
                // 重新设置下一条写入的日志的索引
                // firstLogIndex为即将写入的第一条索引的Id
                raftMeta.setFirstLogIndex(1);
                raftMeta.setLastLogIndex(0); // firstLogIndex只有在涉及快照的时候才会更新否则永远都是默认值1
            } else {
                // 从最新的日志开始删除
                while (CollUtil.isNotEmpty(logTreeMap)) {
                    LogFile logFile = logTreeMap.lastEntry().getValue();
                    if (logFile.getFirstIndex() >= logIndex) { // 整个文件的日志都需要删除
                        logFile.destroy();
                        logTreeMap.remove(logFile.getFirstIndex());
                        continue;
                    } else if (logIndex > logFile.getLastIndex()) { // 已经删除完成
                        break;
                    } else {
                        logFile.deleteSuffix(logIndex); // 从尾部开始删除
                        if (logFile.entrySize() == 0) {
                            logFile.destroy();
                            logTreeMap.remove(logFile.getFileSize());
                        }
                        break;
                    }
                }
                // 可能删除了所有的日志
                raftMeta.setLastLogIndex(logTreeMap.lastEntry().getValue().getLastEntry().getIndex()); // 为当前最新的日志记录的索引
            }
            updateRaftMeta(raftMeta);
            return true;
        } catch (Exception e) {
            logger.info("log manager clean suffix exception", e);
            throw new RaftException(e);
        }
    }

    /**
     * 应用状态机之后需要删除旧的数据
     * @param logIndex
     * @return
     */
    public synchronized boolean cleanPrefix(long logIndex) {
        try {
            // 如果不存在任何的日志
            if (logTreeMap.isEmpty() || logIndex < reloadRaftMeta().getFirstLogIndex()) {
                logger.warn("currently does not clean any logs");
                return false;
            }
            if (logIndex >= reloadRaftMeta().getLastLogIndex()) {
                logTreeMap.values().forEach(item -> item.destroy());
                logTreeMap.clear(); // 清空列表
                // todo 这里要判断是否存在快照如果快照不存在则firstLogIndex始终都是1则lastLogIndex为0
                // 重新设置下一条写入的日志的索引
                // firstLogIndex为即将写入的第一条索引的Id
                // 这里firstLogIndex应该为快照中的lastLogIndex+1
                raftMeta.setFirstLogIndex(1);
                raftMeta.setLastLogIndex(0);
            } else {
                // 从旧的历史日志开始遍历
                while (CollUtil.isNotEmpty(logTreeMap)) {
                    LogFile logFile = logTreeMap.firstEntry().getValue();
                    if (logIndex >= logFile.getLastIndex()) { // 整个文件的日志都需要删除
                        logFile.destroy();
                        logTreeMap.remove(logFile.getFirstIndex());
                        continue;
                    } else if (logIndex < logFile.getFirstIndex()) {
                        break;
                    } else {
                        logFile.removePrefix(logIndex); // prefix删除涉及到新segment文件的生成
                        if (logFile.entrySize() == 0) { // 二次检查实际上应该不会发生这种情况如果一个前缀删除了所有的日志则会自动被清空
                            logFile.destroy();          // 如果删除了一部分则会生成一个新的segment文件当前文件会自动的从集合中卸载
                        }
                        continue; // 前缀删除可能会导致新的文件生成则全局的firstLogIndex更新
                    }
                }
                // 可能删除了所有的日志
                raftMeta.setFirstLogIndex(logTreeMap.firstEntry().getValue().getFirstIndex()); // 为当前最新的日志记录的索引
            }
            updateRaftMeta(raftMeta);
            return true;
        } catch (Exception e) {
            logger.info("log manager clean prefix exception", e);
            throw new RaftException(e);
        }
    }


    /**
     * 复制从leader发送来的日志数据
     * @param preLogTerm
     * @param preLogIndex
     * @param raftLog
     * @return
     */
    @Override
    public synchronized boolean replicateLog(long preLogTerm, long preLogIndex, LogEntry raftLog) {
        try {
            // 判断日志的增加是否连续
            // raft算法中索引的增长应该是递增的
            if (preLogIndex + 1 != raftLog.getIndex()) {
                logger.warn("raft log index => {} but pre log index => {}", raftLog.getIndex(), preLogIndex);
                throw new RaftException("replicate log index mismatching");
            }
            // 判断preLogIndex所在的日志是否匹配
            // todo 这里在加入了快照之后要加上快照的处理
            if (preLogTerm == 0 && preLogIndex == 0) {
                // 整个集群的第一条日志数据应该直接添加
                // 但是如果当前已经存在提交的日志则是出现了异常
                if (reloadRaftMeta().getCommittedIndex() > 0) {
                    logger.warn("replicate log conflict exception log index => {} committed index", raftLog.getIndex(), raftMeta.getCommittedIndex());
                    throw new RaftException("receive raft first log but current some part log committed");
                } else {
                    // 清空整个日志列表然后添加
                    // 注意：写入的term与index只能与leader发送出来的相同
                    cleanSuffix(0l);
                    appendLog(raftLog.getTerm(), raftLog.getEntries());
                }
            } else {
                // 判断日志是否匹配如果不匹配则需要进行回退
                LogEntry logEntry = getLogEntry(preLogIndex);
                if (logEntry == null || preLogTerm != logEntry.getTerm()) { // term应该始终和leader保持一致
                    return false; // 不匹配的时候直接返回
                } else {
                    // 删除可能存在的冲突的日志数据
                    cleanSuffix(raftLog.getIndex());
                    // 判断当前写入的日志索引与待写入的日志索引是否一致
                    if (reloadRaftMeta().getLastLogIndex() + 1 != raftLog.getIndex()) {
                        logger.warn("replicate log occur conflict log index => {} expect next log index", raftLog.getIndex(), raftMeta.getLastLogIndex() + 1);
                        throw new RaftException("replicate log occur conflict log index mismatching next log index");
                    }
                    appendLog(raftLog.getTerm(), raftLog.getEntries()); // 追加到日志文件
                }
            }
            return true;
        } catch (Exception e) {
            logger.error("replicate log occur exception", e);
            return false;
        }
    }

    /**
     * 获取指定的日志条目
     * @param index
     * @return
     */
    @Override
    public LogEntry getLogEntry(long index) {
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
