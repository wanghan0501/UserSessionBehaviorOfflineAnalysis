package com.tosit.project.dao.factory;

import com.tosit.project.dao.ITaskDAO;
import com.tosit.project.dao.impl.TaskDAOImpl;

/**
 * 数据访问对象工厂类
 * <p>
 * Created by Wanghan on 2017/3/11.
 * Copyright © Wanghan SCU. All Rights Reserved
 */
public class DAOFActory {
    /**
     * 构造并返回TaskDAO实例
     *
     * @return
     */
    public static ITaskDAO getTaskDAO() {
        return new TaskDAOImpl();
    }
}
