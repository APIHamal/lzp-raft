package com.lizhengpeng.lraft.exception;

/**
 * 通用异常处理
 * @author lzp
 */
public class RaftException extends RuntimeException {

    public RaftException(String msg) {
        super(msg);
    }

    public RaftException(String msg, Throwable throwable) {
        super(msg, throwable);
    }

}
