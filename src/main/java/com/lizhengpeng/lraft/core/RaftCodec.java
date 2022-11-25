package com.lizhengpeng.lraft.core;

import cn.hutool.core.util.StrUtil;
import com.alibaba.fastjson2.JSONObject;
import com.lizhengpeng.lraft.exception.RaftCodecException;
import com.lizhengpeng.lraft.exception.RaftException;
import com.lizhengpeng.lraft.request.ClientRequestMsg;
import com.lizhengpeng.lraft.request.AppendLogMsg;
import com.lizhengpeng.lraft.request.RefreshLeaderMsg;
import com.lizhengpeng.lraft.request.RequestVoteMsg;
import com.lizhengpeng.lraft.response.*;

import java.nio.charset.StandardCharsets;

/**
 * raft消息的编解码器
 * type(byte)+object(byte)
 * @author lzp
 */
public class RaftCodec {

    public static final Integer HEAD_LENGTH = 6;

    private static final byte REQUEST_VOTE_REQ = 1;

    private static final byte REQUEST_VOTE_RES = 2;

    private static final byte APPEND_LOG_REQ = 3;

    private static final byte APPEND_LOG_RES = 4;

    private static final byte APPEND_LOG_ENTRY = 5;

    private static final byte REFRESH_LEADER_REQ = 6;

    private static final byte REFRESH_LEADER_RES = 7;

    private static final byte CLIENT_REQUEST_RES = 8;

    private static final byte REDIRECT_RES = 9;

    /**
     * 对指定的消息编码
     * @param message
     * @return
     */
    public static byte[] encode(Object message) {
        try {
            byte[] jsonBytes = JSONObject.toJSONString(message).getBytes(StandardCharsets.UTF_8); // 消息内容编码
            byte[] encode = new byte[HEAD_LENGTH + 1 + jsonBytes.length]; // 整个报文的总长度
            // 头部6个字节字符串表示整个报文内容的长度
            String bodySize = String.valueOf(1 + jsonBytes.length);
            byte[] headBytes = StrUtil.fillBefore(bodySize, '0', HEAD_LENGTH).getBytes(StandardCharsets.UTF_8); // 用0填充head
            System.arraycopy(headBytes, 0, encode, 0, HEAD_LENGTH);

            if (message instanceof RequestVoteMsg) {
                encode[HEAD_LENGTH] = REQUEST_VOTE_REQ;
            } else if (message instanceof RequestVoteRes) {
                encode[HEAD_LENGTH] = REQUEST_VOTE_RES;
            } else if (message instanceof AppendLogMsg) {
                encode[HEAD_LENGTH] = APPEND_LOG_REQ;
            } else if (message instanceof AppendLogRes) {
                encode[HEAD_LENGTH] = APPEND_LOG_RES;
            } else if (message instanceof ClientRequestMsg) {
                encode[HEAD_LENGTH] = APPEND_LOG_ENTRY; // leader节点追加日志
            } else if(message instanceof RefreshLeaderMsg) {
                encode[HEAD_LENGTH] = REFRESH_LEADER_REQ; // 获取集群的leader节点信息
            } else if(message instanceof RefreshLeaderRes) {
                encode[HEAD_LENGTH] = REFRESH_LEADER_RES; // 获取集群的leader节点信息
            } else if (message instanceof AppendLogCall) {
                encode[HEAD_LENGTH] = CLIENT_REQUEST_RES; // 返回客户端响应
            } else if (message instanceof RedirectRes) {
                encode[HEAD_LENGTH] = REDIRECT_RES;
            } else {
                throw new RaftException("encode error un support message type");
            }
            System.arraycopy(jsonBytes, 0, encode, HEAD_LENGTH + 1, jsonBytes.length);
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
                throw new RaftException("decode rpc message error");
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
            } else if (message[0] == APPEND_LOG_ENTRY) {
                return JSONObject.parseObject(new String(buffer, StandardCharsets.UTF_8), ClientRequestMsg.class);
            } else if (message[0] == REFRESH_LEADER_REQ) {
                return JSONObject.parseObject(new String(buffer, StandardCharsets.UTF_8), RefreshLeaderMsg.class);
            } else if (message[0] == REFRESH_LEADER_RES) {
                return JSONObject.parseObject(new String(buffer, StandardCharsets.UTF_8), RefreshLeaderRes.class);
            } else if (message[0] == CLIENT_REQUEST_RES) {
                return JSONObject.parseObject(new String(buffer, StandardCharsets.UTF_8), AppendLogCall.class);
            } else if (message[0] == REDIRECT_RES) {
                return JSONObject.parseObject(new String(buffer, StandardCharsets.UTF_8), RedirectRes.class);
            } else {
                throw new RaftException("decode error un support message type");
            }
        } catch (Exception e) {
            throw new RaftCodecException("decode message exception", e);
        }
    }

}
