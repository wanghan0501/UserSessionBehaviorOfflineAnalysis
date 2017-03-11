package com.tosit.project.jdbc;
import com.tosit.project.conf.ConfigurationManager;
import com.tosit.project.constants.Constants;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.util.LinkedList;

/**
 * JDBC辅助组件
 *
 * 在正式的项目的代码编写过程中，是完全严格按照大公司的coding标准来的
 * 也就是说，在代码中，是不能出现任何hard code（硬编码）的字符
 * 比如“张三”、“com.mysql.jdbc.Driver”
 * 所有这些东西，都需要通过常量来封装和使用
 *
 * @author zhuhongjin
 *
 */
public class JDBCHelper {

    // 第一步：在静态代码块中，直接加载数据库的驱动
    // 加载驱动，不是直接简单的，使用com.mysql.jdbc.Driver就可以了
    // 之所以说，不要硬编码，他的原因就在于这里
    //
    // com.mysql.jdbc.Driver只代表了MySQL数据库的驱动
    // 那么，如果有一天，我们的项目底层的数据库要进行迁移，比如迁移到Oracle，或者是DB2、SQLServer
    // 那么，就必须很费劲的在代码中，找，找到硬编码了com.mysql.jdbc.Driver的地方，然后改成
    // 其他数据库的驱动类的类名
    // 所以正规项目，是不允许硬编码的，那样维护成本很高
    //
    // 通常，我们都是用一个常量接口中的某个常量，来代表一个值
    // 然后在这个值改变的时候，只要改变常量接口中的常量对应的值就可以了
    //
    // 项目，要尽量做成可配置的
    // 就是说，我们的这个数据库驱动，更进一步，也不只是放在常量接口中就可以了
    // 最好的方式，是放在外部的配置文件中，跟代码彻底分离
    // 常量接口中，只是包含了这个值对应的key的名字
    static {
        String driver = ConfigurationManager.getProperty(Constants.JDBC_DRIVER);
        try {
            Class.forName(driver);
        } catch (ClassNotFoundException e) {
            e.printStackTrace();
        }
    }

    // 第二步，实现JDBCHelper的单例化
    // 为什么要实现代理化呢？因为它的内部要封装一个简单的内部的数据库连接池
    // 为了保证数据库连接池有且仅有一份，所以就通过单例的方式
    // 保证JDBCHelper只有一个实例，实例中只有一份数据库连接池
    private static JDBCHelper instanse = null;
    public static JDBCHelper getInstanse(){
        if (instanse == null){
            synchronized (JDBCHelper.class){
                if (instanse == null){
                    instanse = new JDBCHelper();
                }
            }

        }
        return instanse;
    }

    //数据库连接池
    private LinkedList<Connection> datasource = new LinkedList<Connection>();

    /**
     *
     * 第三步：实现单例的过程中，创建唯一的数据库连接池
     *
     * 私有化构造方法
     *
     * JDBCHelper在整个程序运行声明周期中，只会创建一次实例
     * 在这一次创建实例的过程中，就会调用JDBCHelper()构造方法
     * 此时，就可以在构造方法中，去创建自己唯一的一个数据库连接池
     *
     */
    private JDBCHelper() {
        int datasourceSize = ConfigurationManager.getInteger(Constants.DBC_DATASOURCE_SIZE);
        //创建指定连接数量的数据库连接池
        for (int i = 0; i < datasourceSize; i++) {
            boolean local = ConfigurationManager.getBoolean(Constants.SPARK_LOCAL);
            String url = null;
            String user = null;
            String password = null;

            if (local) {
                url = ConfigurationManager.getProperty(Constants.JDBC_URL);
                user = ConfigurationManager.getProperty(Constants.JDBC_USER);
                password = ConfigurationManager.getProperty(Constants.JDBC_PASSWORD);
            } else {
                url = ConfigurationManager.getProperty(Constants.JDBC_URL_PROD);
                user = ConfigurationManager.getProperty(Constants.JDBC_USER_PROD);
                password = ConfigurationManager.getProperty(Constants.JDBC_PASSWORD_PROD);
            }

            try {
                Connection conn = DriverManager.getConnection(url, user, password);
                datasource.push(conn);
            } catch (Exception e) {
                e.printStackTrace();
            }
        }
    }


    /**
     * 第四步，提供获取数据库连接的方法
     * 有可能，你去获取的时候，这个时候，连接都被用光了，你暂时获取不到数据库连接
     * 所以我们要自己编码实现一个简单的等待机制，去等待获取到数据库连接
     */
    public synchronized Connection getConnection() {
        while (datasource.size() == 0) {
            try {
                Thread.sleep(10);
            } catch (InterruptedException e) {
                e.printStackTrace();
            }
        }
        return datasource.poll();
    }


    /**
     * 执行查询SQL语句
     * @param sql
     * @param params
     * @param callback
     */
    public void executeQuery(String sql,Object[] params,QueryCallback callback){
        Connection conn = null;
        PreparedStatement pstmt = null;
        ResultSet rs = null;

        try {
            conn = getConnection();
            pstmt = conn.prepareStatement(sql);

            if(params != null && params.length > 0) {
                for(int i = 0; i < params.length; i++) {
                    pstmt.setObject(i + 1, params[i]);
                }
            }

            rs = pstmt.executeQuery();

            callback.process(rs);
        } catch (Exception e) {
            e.printStackTrace();
        } finally {
            if(conn != null) {
                datasource.push(conn);
            }
        }
    }



    /**
     * 静态内部类：查询回调接口
     * @author Administrator
     *
     */
    public static interface QueryCallback{

        /**
         * 处理查询结果
         * @param rs
         * @throws Exception
         */
        void process(ResultSet rs) throws Exception;

    }

}
