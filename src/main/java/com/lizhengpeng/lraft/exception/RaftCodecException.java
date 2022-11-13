package com.lizhengpeng.lraft.exception;

/**
 * 编解码器异常处理
 * @author lzp
 */
public class RaftCodecException extends RuntimeException {

    public RaftCodecException(String msg) {
        super(msg);
    }

    public RaftCodecException(String msg, Throwable throwable) {
        super(msg, throwable);
    }

}
