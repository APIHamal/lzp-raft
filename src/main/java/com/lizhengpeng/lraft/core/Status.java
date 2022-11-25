package com.lizhengpeng.lraft.core;

import lombok.Builder;
import lombok.Getter;
import lombok.Setter;

/**
 * 异步任务执行结果
 * @author lzp
 */
@Setter
@Getter
@Builder
public class Status {

    private Boolean ok; // 任务是否正常执行

    private String reason; // 原因

}
