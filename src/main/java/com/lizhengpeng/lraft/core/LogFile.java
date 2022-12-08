package com.lizhengpeng.lraft.core;

import cn.hutool.core.collection.CollUtil;
import cn.hutool.core.io.FileUtil;
import com.lizhengpeng.lraft.exception.RaftException;
import lombok.Builder;
import lombok.Getter;
import lombok.Setter;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.File;
import java.io.IOException;
import java.io.RandomAccessFile;
import java.util.ArrayList;
import java.util.List;
import java.util.Objects;

/**
 * 具体的日志文件
 * @author lzp
 */
@Setter
@Getter
public class LogFile {

    private static final Logger logger = LoggerFactory.getLogger(LogFile.class);

    private FileLogManager logManager;

    private String logDir; // 当前日志文件所属的目录

    private String logFileName; // 当前日志文件的名称

    private long firstIndex;

    private long lastIndex;

    private List<LogSegment> segments;

    private volatile RandomAccessFile accessFile;

    /**
     * 初始化日志文件
     * @param logDir
     * @param logFileName
     */
    public LogFile(FileLogManager fileLogManager, String logDir, String logFileName) {
        this.logManager = fileLogManager;
        this.logDir = logDir;
        this.logFileName = logFileName;
        this.segments = new ArrayList<>();
    }

    /**
     * 加载当前的日志文件
     * 并做好内存映射
     */
    public void reloadLogFile(){
        try {
            File logFile = new File(logDir, logFileName);
            if (!logFile.exists() || !logFile.isFile() || !logFile.canRead() || !logFile.canWrite()) {
                logger.info("reload log file => {} failed", logFile);
                throw new RaftException("reload log file failed");
            }
            // 如果重新加载可能之前已经存在对应的文件流
            AccessUtils.closeAccessFile(this.accessFile);
            segments.clear(); // 清空列表
            accessFile = AccessUtils.openRandomAccess(logFile); // 同步的写入磁盘

            long offset = 0; // 判断文件是否读取完成
            while (offset < accessFile.length()) {
                // 先读取数据的长度再读取实际的数据内容
                int dataLength = accessFile.readInt();
                byte[] buffer = new byte[dataLength];
                int hasRead = accessFile.read(buffer);
                if (hasRead != dataLength) {
                    logger.info("offset => {} data length => {} read length => {}", offset, dataLength, hasRead);
                    throw new RaftException("read log segment failed");
                }
                // 反序列化JSON对象
                LogEntry logEntry = RaftUtils.readObj(buffer, LogEntry.class);
                LogSegment logSegment = LogSegment.builder()
                        .offset(offset)
                        .size((Integer.SIZE / Byte.SIZE) + dataLength)
                        .logEntry(logEntry)
                        .build();
                segments.add(logSegment); // 添加到列表中
                offset = accessFile.getFilePointer(); // 记录当前指针的位置(总是指向下一个读取的位置当其等于file.length()的时候表示读取完成)
                lastIndex = logEntry.getIndex(); // 更新最后一条日志的索引
            }
        } catch (Exception e) {
            logger.info("load log file exception", e);
            throw new RaftException(e);
        }
    }

    /**
     * 写入/写出操作前置检查
     * @throws IOException
     */
    public void preOperateCheck() throws IOException {
        // 检查日志文件是否正常的创建
        File logFile = new File(logDir, logFileName);
        if (!logFile.exists() && !logFile.createNewFile()) {
            logger.error("pre check => create log file => {} failed", logFile);
            throw new RaftException("pre operate check failed");
        }
        // 检查内存中的数据与文件是否匹配
        if (CollUtil.isNotEmpty(segments) && (lastIndex - firstIndex + 1) != segments.size()) {
            logger.error("pre check => memory size => {}, last index => {}, first index => {} unMatch", segments.size(), firstIndex, lastIndex);
            throw new RaftException("pre operate check failed");
        }
        // 检查文件流
        if (accessFile == null) {
            accessFile = AccessUtils.openRandomAccess(logFile);
            accessFile.seek(accessFile.length()); // 移动到文件的末尾指针
        }
    }

    /**
     * 写入日志列表前的检查操作
     * @param logEntry
     */
    public void appendPreCheck(LogEntry logEntry) throws IOException {
        // 判断写入的日志是否是连续的
        // 列表为空写入的日志索引必须跟firstIndex相同
        // 后续递增进行写入
        this.preOperateCheck();
        if (CollUtil.isEmpty(segments) && logEntry.getIndex() != firstIndex) {
            logger.info("append pre check failed,segments is empty! first index => {}, entry index => {}", firstIndex, logEntry.getIndex());
            throw new RaftException("append log file failed");
        } else if (CollUtil.isNotEmpty(segments) && logEntry.getIndex() != lastIndex + 1) {
            logger.info("append pre check failed,segments expect next index => {}, entry index => {}", (lastIndex + 1), logEntry.getIndex());
            throw new RaftException("append log file failed");
        }
    }

    /**
     * 判断当前日志文件是否可以继续写入数据
     * 日志文件到达阈值后不允许继续写入
     * @return
     * @throws IOException
     */
    public long getFileSize() throws IOException {
        preOperateCheck(); // 前置检查防止出现异常
        return accessFile.length();
    }

    /**
     * 关闭文件流对象
     */
    public void closeFile() {
        AccessUtils.closeAccessFile(accessFile);
        accessFile = null; // 清空后可以重新打开
    }

    /**
     * 获取当前最后一条日志数据
     * @return
     */
    public LogEntry getLastEntry() {
        return CollUtil.isEmpty(segments) ? null : segments.get(segments.size() - 1).getLogEntry();
    }

    /**
     * 销毁当前的文件和数据
     */
    public void destroy() {
        try {
            AccessUtils.closeAccessFile(accessFile); // 关闭文件流对象
            File logFile = new File(logDir, logFileName);
            FileUtil.del(logFile);
            if (CollUtil.isNotEmpty(segments)) { // 清空列表
                segments.clear();
            }
        } catch (Exception e) {
            logger.info("destroy log file => {} failed", logDir + File.separator + logFileName);
            throw new RaftException(e);
        }
    }

    /**
     * 添加日志条目到列表中
     * @param logEntry
     */
    public void appendLogEntry(LogEntry logEntry){
        try {
            appendPreCheck(logEntry); // 写入的前置检查
            byte[] buffer = RaftUtils.writeObj(logEntry);
            // 添加到列表中
            LogSegment segment = LogSegment.builder()
                    .offset(accessFile.getFilePointer())
                    .size((Integer.SIZE / Byte.SIZE) + buffer.length)
                    .logEntry(logEntry)
                    .build();
            // 写入到实际的文件中
            accessFile.writeInt(buffer.length);
            accessFile.write(buffer);
            // 更新最后索引的位置
            // 写入文件成功了最后再更新索引中的位置
            // 文件写入可能会失败但是内存的写入99.999%情况下都是成功的
            segments.add(segment);
            lastIndex = logEntry.getIndex();
        } catch (Exception e) {
            logger.info("append log => {} failed", logEntry);
            throw new RaftException(e);
        }
    }

    /**
     * 获取当前写入的日志的数量
     * @return
     */
    public long entrySize() {
        return segments == null ? 0 : segments.size();
    }

    /**
     * 获取指定的日志消息
     * @param index
     * @return
     */
    public LogEntry getLogEntry(long index) {
        // 只能读取当前区间内的数据
        if (index < firstIndex || index > lastIndex) {
            return null;
        }
        int relIndex = (int) (index - firstIndex); // 转为相对的位置
        return segments.get(relIndex).getLogEntry();
    }

    /**
     * 判断当前是否包含指定的日志条目
     * @param logEntry
     * @return
     */
    public boolean contains(LogEntry logEntry) {
        LogEntry tmpEntry = this.getLogEntry(logEntry.getIndex());
        return tmpEntry != null && tmpEntry.getTerm() == logEntry.getTerm();
    }

    /**
     * 从指定的索引开始删除苏剧
     * @param index
     * @throws IOException
     */
    public boolean deleteSuffix(long index) {
        try {
            preOperateCheck();
            // 如果删除的索引是第一条或者之前的位置
            if (index > lastIndex) {
                logger.warn("delete suffix index => {} great than last index => {}", index, lastIndex);
                return false;
            } else if (index <= firstIndex) { // 索引小于当前的第一条日志条目则全部删除
                lastIndex = -1;
                segments.clear();
                accessFile.setLength(0); // 清空日志文件的内容
                return true;
            } else {
                // 索引转为相对于列表的下标索引
                int relIndex = (int) (index - firstIndex);
                // 只保留firstIndex到index-1之间的内容
                int destIndex = segments.size() - 1;
                while (destIndex >= relIndex) {
                    segments.remove(destIndex);
                    destIndex--;
                }
                // 更新最后一条日志的索引
                LogSegment lastSegment = segments.get(destIndex);
                lastIndex = lastSegment.getLogEntry().getIndex();
                // 更新文件指针的位置(当前最后一条条目在文件中的偏移量+条目占用的空间(byte))
                long offset = lastSegment.getOffset() + lastSegment.getSize();
                accessFile.setLength(offset); // 截断文件会自动的移动指针
                return true;
            }
        } catch (Exception e) {
            logger.info("delete log suffix first index => {}, last index => {}, delete index => {} exception", firstIndex, lastIndex, index);
            throw new RaftException(e);
        }
    }

    /**
     * 从日志的开始日志一直删除到指定的索引
     * @param index
     */
    public boolean removePrefix(long index) {
        try {
            preOperateCheck();
            if (index < firstIndex) {
                logger.warn("delete prefix index => {} great than last index => {}", index, lastIndex);
                return false;
            } else if (index >= lastIndex) { // 索引大于最后一条条目的索引则全部删除
                lastIndex = -1;
                segments.clear();
                accessFile.setLength(0); // 清空日志文件的内容
                return true;
            } else {
                // 由于文件名对应第一条日志条目的索引
                // 如果未完全删除则需要生成新的文件并且重新添加数据
                // 索引转为相对于列表的下标索引
                int relIndex = (int) (index - firstIndex);
                List<LogSegment> tmpSegments = segments.subList(relIndex + 1, segments.size());
                if (CollUtil.isEmpty(tmpSegments)) {
                    logger.error("sub list failed, index => {}, relIndex => {}, first index => {}, last index => {}", index, relIndex, firstIndex, lastIndex);
                    throw new RaftException("create sub list failed");
                }
                // 创建一个新的临时文件
                // 用来写入剩余数据(注意文件名为第一条数据的索引)
                // 使用0填充文件名到指定的长度
                String newFileName = String.format("%030d", tmpSegments.get(0).getLogEntry().getIndex());
                File newFile = new File(logDir, newFileName);
                if (newFile.exists() && !newFile.delete()) {
                    logger.error("delete tmp file => {} failed", newFile);
                    throw new RaftException("delete tmp file failed");
                }
                if (!newFile.exists() && !newFile.createNewFile()) {
                    logger.error("create tmp file => {} failed", newFile);
                    throw new RaftException("pre operate check failed");
                }

                // 加载新的文件到集合中
                LogFile newLogFile = new LogFile(logManager, logDir, newFileName);
                newLogFile.setFirstIndex(Long.parseLong(newFileName)); // 设置第一条条目的索引
                newLogFile.setLastIndex(0); // 日志写入的最后一条条目的索引
                tmpSegments.forEach(segment -> newLogFile.appendLogEntry(segment.getLogEntry()));
                logManager.loadLogFile(Long.parseLong(newFileName), newLogFile);

                // 从集合中删除当前的日志文件不再使用
                logManager.unLoadLogFile(firstIndex);
                segments.clear(); // 清空列表
                AccessUtils.closeAccessFile(accessFile); // 关闭文件流
                // 删除旧的文件和数据
                File currentLogFile = new File(logDir, logFileName);
                FileUtil.del(currentLogFile);
                return true;
            }
        } catch (Exception e) {
            logger.info("delete log prefix first index => {}, last index => {}, delete index => {} exception", firstIndex, lastIndex, index);
            throw new RaftException(e);
        }
    }

    /**
     * 内存中的日志与现有的列表中记录的映射关系
     * @author lzp
     */
    @Builder
    @Getter
    private static class LogSegment {

        private long offset; // 记录位于硬盘上的偏移量

        private long size; // 完整日志记录的长度

        private LogEntry logEntry;

        @Override
        public boolean equals(Object obj) {
            if (this == obj) {
                return true;
            }
            if (obj == null || getClass() != obj.getClass()) {
                return false;
            }
            LogSegment that = (LogSegment) obj;
            return logEntry != null && logEntry.equals(that.logEntry);
        }

        @Override
        public int hashCode() {
            return Objects.hash(logEntry);
        }
    }

    @Override
    public String toString() {
        return "LogFile{" +
                "logDir='" + logDir + '\'' +
                ", logFileName='" + logFileName + '\'' +
                ", firstIndex=" + firstIndex +
                ", lastIndex=" + lastIndex +
                ", segments=" + segments +
                ", accessFile=" + accessFile +
                '}';
    }
}
