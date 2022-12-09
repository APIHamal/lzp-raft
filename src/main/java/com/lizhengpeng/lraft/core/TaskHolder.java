package com.lizhengpeng.lraft.core;

import cn.hutool.core.util.StrUtil;
import lombok.Getter;
import lombok.Setter;

import java.util.Objects;

/**
 * task对象的管理
 * 当任务未提交的时候清除任务池
 * @author
 */
@Setter
@Getter
public class TaskHolder {

    private String uuid; // 任务的标识

    private long startTime; // 放入池中的时间

    public TaskHolder(String uuid) {
        this.uuid = uuid;
        this.startTime = System.currentTimeMillis();
    }

    public static TaskHolder holder(String uuid) {
        return new TaskHolder(uuid);
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;
        TaskHolder that = (TaskHolder) o;
        return StrUtil.isNotBlank(uuid) && StrUtil.equals(uuid, that.uuid);
    }

    @Override
    public int hashCode() {
        return Objects.hash(uuid);
    }
}
