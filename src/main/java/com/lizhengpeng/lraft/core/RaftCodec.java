package com.lizhengpeng.lraft.core;

import com.alibaba.fastjson2.JSONObject;
import com.lizhengpeng.lraft.exception.RaftCodecException;
import com.lizhengpeng.lraft.exception.RaftException;
import com.lizhengpeng.lraft.request.AppendLogMsg;
import com.lizhengpeng.lraft.request.RequestVoteMsg;
import com.lizhengpeng.lraft.response.AppendLogRes;
import com.lizhengpeng.lraft.response.RequestVoteRes;

import java.nio.charset.StandardCharsets;

/**
 * raft消息的编解码器
 * type(byte)+object(byte)
 * @author lzp
 */
public class RaftCodec {

    private static final byte REQUEST_VOTE_REQ = 1;

    private static final byte REQUEST_VOTE_RES = 2;

    private static final byte APPEND_LOG_REQ = 3;

    private static final byte APPEND_LOG_RES = 4;

    /**
     * 对指定的消息编码
     * @param message
     * @return
     */
    public static byte[] encode(Object message) {
        try {
            byte[] jsonBytes = JSONObject.toJSONString(message).getBytes(StandardCharsets.UTF_8);
            byte[] encode = new byte[jsonBytes.length + 1];
            if (message instanceof RequestVoteMsg) {
                encode[0] = REQUEST_VOTE_REQ;
            } else if (message instanceof RequestVoteRes) {
                encode[0] = REQUEST_VOTE_RES;
            } else if (message instanceof AppendLogMsg) {
                encode[0] = APPEND_LOG_REQ;
            } else if (message instanceof AppendLogRes) {
                encode[0] = APPEND_LOG_RES;
            } else {
                throw new RaftException("encode error un support message type");
            }
            System.arraycopy(jsonBytes, 0, encode, 1, jsonBytes.length);
            return encode;
        } catch (Exception e) {
            throw new RaftCodecException("encode message exception", e);
        }
    }

    /**
     * 对指定的内容进行解码操作
     * @param message
     * @return
     */
    public static Object decode(byte[] message) {
        try {
            if (message == null || message.length <= 1) {
                throw new RaftException("rpc message format error");
            }
            byte[] buffer = new byte[message.length -1];
            System.arraycopy(message, 1, buffer, 0, buffer.length);
            if (message[0] == REQUEST_VOTE_REQ) {
                return JSONObject.parseObject(new String(buffer, StandardCharsets.UTF_8), RequestVoteMsg.class);
            } else if (message[0] == REQUEST_VOTE_RES) {
                return JSONObject.parseObject(new String(buffer, StandardCharsets.UTF_8), RequestVoteRes.class);
            } else if (message[0] == APPEND_LOG_REQ) {
                return JSONObject.parseObject(new String(buffer, StandardCharsets.UTF_8), AppendLogMsg.class);
            } else if (message[0] == APPEND_LOG_RES) {
                return JSONObject.parseObject(new String(buffer, StandardCharsets.UTF_8), AppendLogRes.class);
            } else {
                throw new RaftException("decode error un support message type");
            }
        } catch (Exception e) {
            throw new RaftCodecException("decode message exception", e);
        }
    }

}
