package com.tosit.project.dao.impl;

/**
 * 配置加载管理类
 * <p>
 * Created by Wanghan on 2017/3/11.
 * Copyright © Wanghan SCU. All Rights Reserved
 */

import com.tosit.project.dao.ITaskDAO;
import com.tosit.project.domain.Task;
import com.tosit.project.jdbc.JDBCHelper;

import java.sql.ResultSet;

public class TaskDAOImpl implements ITaskDAO {
    /**
     * 构造并返回TaskDAO实例
     *
     * @param taskId
     * @return
     */
    public Task findById(long taskId) {
        final Task task = new Task();
        String sql = "select * from task where task_id=?";
        Object[] params = {taskId};

        JDBCHelper jdbcHelper = JDBCHelper.getInstanse();
        jdbcHelper.executeQuery(sql, params, new JDBCHelper.QueryCallback() {
            public void process(ResultSet rs) throws Exception {
                if (rs.next()) {
                    long taskId = rs.getLong(1);
                    task.setTaskId(taskId);
                }
            }
        });

        return null;
    }
}