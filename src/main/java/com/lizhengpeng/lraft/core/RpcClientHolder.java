package com.lizhengpeng.lraft.core;

import lombok.Getter;
import lombok.Setter;

import java.net.Socket;
import java.util.concurrent.locks.Lock;
import java.util.concurrent.locks.ReentrantLock;

/**
 * rpc客户端对象
 * @author lzp
 */
@Setter
@Getter
public class RpcClientHolder {

    private Lock holderLock = new ReentrantLock();

    private volatile Socket socket;

    public void lock() {
        holderLock.lock();
    }

    public void release() {
        holderLock.unlock();;
    }

}
