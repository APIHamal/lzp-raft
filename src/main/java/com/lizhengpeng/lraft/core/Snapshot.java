package com.lizhengpeng.lraft.core;

import cn.hutool.core.collection.CollUtil;
import com.alibaba.fastjson2.JSONObject;
import com.lizhengpeng.lraft.exception.RaftException;
import lombok.Getter;
import lombok.Setter;

import java.io.File;
import java.io.RandomAccessFile;
import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.Comparator;
import java.util.List;

/**
 * 日志快照的实现
 * @author lzp
 */
@Setter
@Getter
public class Snapshot {

    private String snapshotDir;

    private List<String> snapshotDirNames = new ArrayList<>();

    private volatile SnapshotWriter localWriter; // 本地的快照写入

    /**
     * 创建日志快照数据
     * @param baseDir
     */
    public Snapshot(String baseDir) {
        this.snapshotDir = baseDir + File.separator + "snapshot";
        File destDir = new File(snapshotDir);
        if (!destDir.exists() && !destDir.mkdirs()) {
            throw new RaftException("create snapshot dir failed");
        }
        this.reloadSnapshot();
    }

    /**
     * 重新加载快照数据
     */
    public void reloadSnapshot() {
        try {
            snapshotDirNames.clear();
            File destDir = new File(this.snapshotDir);
            if (!destDir.exists() || !destDir.isDirectory()) {
                return;
            }
            File[] files = destDir.listFiles();
            for (File file : files) {
                snapshotDirNames.add(file.getName());
            }
            // 按照时间戳倒叙排列
            snapshotDirNames.sort(Comparator.comparing(String::toString).reversed());
        } catch (Exception e) {
            throw new RaftException("reload snapshot exception", e);
        }
    }

    /**
     * 获取快照写入对象
     * @return
     */
    public SnapshotWriter getSnapshotWriter() {
        return new SnapshotWriter(snapshotDir);
    }

    /**
     * 读取日志快照的元数据
     * @return
     */
    public SnapshotMeta readMeta() {
        SnapshotMeta meta = SnapshotMeta
                .builder()
                .lastLogIndex(0l)
                .lastLogTerm(0l)
                .build();
        try {
            if (CollUtil.isEmpty(snapshotDirNames)) { // 当前没有任何数据
                return meta;
            }
            File metaFile = new File(snapshotDirNames.get(0) + File.separator + "meta");
            if (metaFile.exists()) {
                RandomAccessFile randomAccessFile = new RandomAccessFile(metaFile, "r");
                byte[] buffer = new byte[(int) randomAccessFile.length()];
                randomAccessFile.read(buffer);
                meta = JSONObject.parseObject(new String(buffer, StandardCharsets.UTF_8), SnapshotMeta.class);
                randomAccessFile.close();
            }
        } catch (Exception e) {
            throw new RaftException("read snapshot meta exception", e);
        }
        return meta;
    }

    /**
     * 读取指定的快照数据
     * @param offset
     * @return
     */
    public byte[] readSnapshot(long offset) {
        try {
            if (CollUtil.isEmpty(snapshotDirNames)) { // 当前没有任何数据
                throw new RaftException("read snapshot exception");
            }
            File metaFile = new File(snapshotDirNames.get(0) + File.separator + "snapshot");
            RandomAccessFile readFile = new RandomAccessFile(metaFile, "r");
            byte[] buffer = new byte[512];
            int hasRead = readFile.read(buffer);
            if (hasRead == -1 || hasRead == 0) {
                return new byte[0]; // 读取完成
            }
            byte[] newBuffer = new byte[hasRead];
            System.arraycopy(buffer, 0, newBuffer, 0, hasRead);
            return newBuffer; // 实际读取到的数据
        } catch (Exception e) {
            throw new RaftException("read snapshot meta exception", e);
        }
    }

}
