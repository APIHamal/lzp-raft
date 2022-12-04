package com.lizhengpeng.lraft.test;

import com.lizhengpeng.lraft.core.Snapshot;
import com.lizhengpeng.lraft.core.SnapshotMeta;
import com.lizhengpeng.lraft.core.SnapshotWriter;

import java.io.IOException;
import java.nio.charset.StandardCharsets;

public class SnapshotTest {
    public static void main(String[] args) throws IOException {
        Snapshot snapshot = new Snapshot("C:\\raft_dir");
        SnapshotWriter snapshotWriter = snapshot.getSnapshotWriter();
        snapshotWriter.write("hello world".getBytes(StandardCharsets.UTF_8));
        snapshotWriter.write("hello raft".getBytes(StandardCharsets.UTF_8));
        snapshotWriter.complete();
        SnapshotMeta meta = SnapshotMeta.builder()
                .lastLogIndex(0l)
                .lastLogTerm(0l)
                .build();
        snapshotWriter.writeSnapshotMeta(meta);


    }
}
