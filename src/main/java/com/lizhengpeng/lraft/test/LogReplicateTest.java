package com.lizhengpeng.lraft.test;

import com.lizhengpeng.lraft.core.RaftCodec;
import com.lizhengpeng.lraft.request.ClientRequestMsg;

import java.io.IOException;
import java.io.OutputStream;
import java.net.Socket;

public class LogReplicateTest {
    public static void main(String[] args) throws IOException {
        Socket socket = new Socket("127.0.0.1", 8080);
        OutputStream outputStream = socket.getOutputStream();
        ClientRequestMsg msg = new ClientRequestMsg();
        msg.setMsg("hello raft");
        byte[] bytes = RaftCodec.encode(msg);
        outputStream.write(bytes);
        outputStream.flush();
        socket.close();
    }
}
