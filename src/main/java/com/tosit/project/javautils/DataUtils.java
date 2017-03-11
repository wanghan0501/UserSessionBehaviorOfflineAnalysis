package com.tosit.project.javautils;

import java.text.*;
import java.util.Date;
/**
 * Created by mac on 2017/3/11.
 */
public class DataUtils {

    private static final SimpleDateFormat TIME_FORMAT= new SimpleDateFormat("yyyy-MM-dd HH-mm-ss");

    /**
     * 获取当天日期
     * 格式：yyyy-MM-dd，即年月日
     * @return String
     */
    public static String getToday(){
        return TIME_FORMAT.format(new Date());
    }

    /**
     * 对时间进行解析
     * @param time 日期
     * @return Data
     */
    public static Date parseTime(String time){

        try{
            return TIME_FORMAT.parse(time);
        }catch (ParseException e){
            e.printStackTrace();
        }

        return null;
    }

    /**
     * 判断一个时间是否在另一个时间之前
     * @param time1 日期
     * @param time2 日期
     * @return boolean
     */
    public static boolean before(String time1,String time2){

        try{
            Date date1=TIME_FORMAT.parse(time1);
            Date date2=TIME_FORMAT.parse(time2);
            if(date1.before(date2)) return true;
        }catch (ParseException e){
            e.printStackTrace();
        }

        return false;
    }

    /**
     * 判断一个时间是否在另一个时间之后
     * @param time1 日期
     * @param time2 日期
     * @return boolean
     */
    public static boolean after(String time1,String time2){
        try{
            Date date1=TIME_FORMAT.parse(time1);
            Date date2=TIME_FORMAT.parse(time2);
            if(date1.after(date2)) return true;
        }catch (ParseException e){
            e.printStackTrace();
        }

        return false;
    }

}
