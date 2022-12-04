package com.lizhengpeng.lraft.core;

import com.alibaba.fastjson2.JSONObject;
import com.lizhengpeng.lraft.exception.RaftException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.File;
import java.io.IOException;
import java.io.RandomAccessFile;
import java.nio.charset.StandardCharsets;

/**
 * 日志快照写入的简单封装
 * @author
 */
public class SnapshotWriter {

    private static final Logger logger = LoggerFactory.getLogger(SnapshotWriter.class);

    private RandomAccessFile snapshotAccessFile;

    private String snapshotDir;

    /**
     * 快照写入初始化
     * @param baseDir
     */
    public SnapshotWriter(String baseDir) {
        try {
            this.snapshotDir = baseDir + File.separator + System.currentTimeMillis();
            File tempDir = new File(snapshotDir);
            if (!tempDir.exists() && !tempDir.mkdirs()) {
                throw new RaftException("create temp snapshot dir failed");
            }
            File snapshotFile = new File(snapshotDir + File.separator + "snapshot");
            if (!snapshotFile.createNewFile()) {
                throw new RaftException("create new temp snapshot dir failed");
            }
            snapshotAccessFile = new RandomAccessFile(snapshotFile, "rw");
        } catch (Exception e) {
            throw new RaftException("create new snapshot file failed");
        }
    }

    /**
     * 写入指定的日志快照数据
     * 直接调用流进行写入即可
     * @param buffer
     */
    public void write(byte[] buffer) {
        try {
            snapshotAccessFile.write(buffer);
        } catch (Exception e) {
            throw new RaftException("write snapshot data failed", e);
        }
    }

    /**
     * 更新元数据信息
     * @param snapshotMeta
     */
    public void writeSnapshotMeta(SnapshotMeta snapshotMeta) {
        try {
            File metaFile = new File(snapshotDir + File.separator + "meta");
            if (!metaFile.exists() && !metaFile.createNewFile()) {
                throw new RaftException("create snapshot file failed");
            }
            RandomAccessFile accessFile = new RandomAccessFile(metaFile, "rw");
            accessFile.write(JSONObject.toJSONString(snapshotMeta).getBytes(StandardCharsets.UTF_8));
            accessFile.close();
        } catch (Exception e) {
            throw new RaftException("read snapshot meta exception", e);
        }
    }

    /**
     * 快照写入成功
     */
    public void complete() throws IOException {
        snapshotAccessFile.close(); // 状态机写入成功后关闭文件流
    }

    /**
     * 关闭当前的文件
     */
    public void release() {
        try {
            snapshotAccessFile.close();
        } catch (Exception e) {
            logger.info("close snapshot exception", e);
        }
    }

}
