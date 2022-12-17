package com.lizhengpeng.lraft.core;

import com.lizhengpeng.lraft.exception.RaftException;
import com.lizhengpeng.lraft.request.InstallSnapshotMsg;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.File;
import java.io.RandomAccessFile;

/**
 * snapshot读取
 * @author lzp
 */
public class SnapshotReader {

    private static final Logger logger = LoggerFactory.getLogger(SnapshotReader.class);

    private String snapshotFile;

    public SnapshotReader(String snapshotFile) {
        this.snapshotFile = snapshotFile;
    }

    /**
     * 读取snapshot相关的数据
     * @param snapshotMsg
     * @return
     */
    public long reader(InstallSnapshotMsg snapshotMsg) throws Exception {
        File snapshotFile = new File(this.snapshotFile);
        if (!snapshotFile.exists() || !snapshotFile.isFile()) {
            throw new RaftException("snapshot file not found, snapshot reader exception");
        }
        try (RandomAccessFile accessFile = AccessUtils.openRandomAccess(snapshotFile)) {
            if (snapshotMsg.getOffset() >= accessFile.length()) {
                snapshotMsg.setData(new byte[0]);
                snapshotMsg.setLastPart(Boolean.TRUE);
                return 0l;
            }
            // 每次读取16个字节
            accessFile.seek(snapshotMsg.getOffset());
            byte[] buffer = new byte[16];
            int hasRead = accessFile.read(buffer);
            // 创建新的缓冲区
            if (hasRead < buffer.length) {
                byte[] newBuf = new byte[hasRead];
                System.arraycopy(buffer, 0, newBuf, 0, hasRead);
                snapshotMsg.setData(newBuf);
            } else {
                snapshotMsg.setData(buffer);
            }
            return hasRead;
        }
    }
}
