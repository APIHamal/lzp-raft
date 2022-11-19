package com.lizhengpeng.lraft.core;

import lombok.Builder;
import lombok.Getter;
import lombok.Setter;

/**
 * 日志抽象
 * @author lzp
 */
@Setter
@Getter
@Builder
public class RaftLog {

    private static final Integer HEART_BEAT_LOG = 1;

    private static final Integer GENERAL_LOG = 2;

    private Integer kind;

    private Long term;

    private Long index;

    private String entries;

    private Long preLogIndex;

    private Long preLogTerm;

}
