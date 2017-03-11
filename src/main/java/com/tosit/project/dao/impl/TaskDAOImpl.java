package com.tosit.project.dao.impl;

/**
 * Created by mac on 2017/3/11.
 */

import com.tosit.project.dao.ITaskDAO;
import com.tosit.project.domain.Task;
import com.tosit.project.jdbc.JDBCHelper;

import java.sql.ResultSet;

a

public class TaskDAOImpl implements ITaskDAO {

    @Override
    public Task findByID(long taskid) {
        final Task task = new Task();
        String sql = "select * from task where task_id=?";
        Object[] params = {taskid};

        JDBCHelper jdbcHelper = JDBCHelper.getInstanse();
        jdbcHelper.executeQuery(sql, params, new JDBCHelper.QueryCallback() {
            @Override
            public void process(ResultSet rs) throws Exception {
                if (rs.next()) {
                    long taskid = rs.getLong(1);
                    task.setTaskid(taskid);
                }
            }
        });

        return null;
    }
}