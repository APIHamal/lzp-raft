package com.lizhengpeng.lraft.core;

import cn.hutool.core.collection.CollUtil;
import cn.hutool.core.collection.ListUtil;
import com.lizhengpeng.lraft.exception.RaftException;
import lombok.Getter;
import lombok.Setter;

import java.io.File;
import java.io.IOException;
import java.io.RandomAccessFile;
import java.util.ArrayList;
import java.util.List;

/**
 * 具体的日志文件
 * @author lzp
 */
@Setter
@Getter
public class LogFile {

    private String fileName;

    private long startIndex;

    private long endIndex;

    private RandomAccessFile accessFile;

    private List<LogEntry> logEntries = new ArrayList<>();

    /**
     * 加载现有的日志文件
     * @param logFile
     */
    public void initLogFile(File logFile){
        try {
            if (!logFile.exists() || !logFile.canRead() || !logFile.canWrite()) {
                throw new RaftException("load log file failed");
            }
            accessFile = new RandomAccessFile(logFile, "rw");
            long offset = 0; // 判断文件是否读取完成
            while (offset < accessFile.length()) {
                // 先读取数据的长度再读取实际的数据内容
                int length = accessFile.readInt();
                byte[] buffer = new byte[length];
                int hasRead = accessFile.read(buffer);
                if (hasRead != length) {
                    throw new RaftException("read log file failed");
                }
                // 反序列化对象
                LogEntry logEntry = RaftUtils.readObj(buffer, LogEntry.class);
                if (logEntry != null) {
                    logEntries.add(logEntry);
                }
                offset = accessFile.getFilePointer(); // 记录当前指针的位置(总是指向下一个读取的位置当其等于file.length()的时候表示读取完成)
            }
            if (CollUtil.isNotEmpty(logEntries)) { // 设置当前日志文件记录的偏移量起始和结束值
                startIndex = logEntries.get(0).getIndex();
                endIndex = logEntries.get(logEntries.size() - 1).getIndex();
            }
        } catch (Exception e) {
            throw new RaftException("load log file exception", e);
        }
    }

    /**
     * 删除指定索引后面的数据
     * @param index
     */
    public void removeFrom(long index) throws IOException {
        // 如果删除的是起始位置
        if (index == startIndex) {
            logEntries.clear();
            endIndex = 0;
            accessFile.setLength(0L);
            accessFile.seek(0L);
        } else {
            // 转为实际list中下标索引的映射
            int relativeIndex = (int) (index - startIndex);
            List<LogEntry> newList = ListUtil.toList(logEntries.subList(0, relativeIndex)); // 进行数组的截断
            // 重新将日志写入到文件中
            accessFile.setLength(0L);
            accessFile.seek(0L); // 设置写指针的位置
            logEntries.clear();
            newList.forEach(item -> {
                this.appendLogEntry(item); // 重新写入文件的位置
            });
            if (!logEntries.isEmpty()) { // 设置endIndex为末尾数据的索引
                endIndex = logEntries.get(logEntries.size() - 1).getIndex();
            } else {
                endIndex = 0; // 清空的情况
            }
        }
    }

    /**
     * 添加日志条目
     * @param logEntry
     */
    public void appendLogEntry(LogEntry logEntry){
        try {
            logEntries.add(logEntry);
            byte[] buffer = RaftUtils.writeObj(logEntry);
            accessFile.writeInt(buffer.length);
            accessFile.write(buffer);
        } catch (Exception e) {
            throw new RaftException("append log to file failed", e);
        }
    }

    /**
     * 获取指定的日志消息
     * @param index
     * @return
     */
    public LogEntry getLogEntry(long index) {
        if (logEntries.isEmpty()) {
            return null;
        }
        if (index < startIndex || index > endIndex) {
            return null;
        }
        return logEntries.get((int) (index - startIndex));
    }

    /**
     * 判断当前文件是否可以继续写入日志
     * 文件容量达到指定的尺寸时则不能继续写入
     * @return
     */
    public long getFileSize() throws IOException {
        return accessFile.length();
    }

    /**
     * 关闭文件流对象
     */
    public void closeFile() {
        try {
            accessFile.close();
        } catch (Exception e) {
            throw new RaftException("close log file failed", e);
        }
    }

    @Override
    public String toString() {
        return "LogFile{" +
                "fileName='" + fileName + '\'' +
                ", startIndex=" + startIndex +
                ", endIndex=" + endIndex +
                ", accessFile=" + accessFile +
                ", logEntries=" + logEntries +
                '}';
    }
}
