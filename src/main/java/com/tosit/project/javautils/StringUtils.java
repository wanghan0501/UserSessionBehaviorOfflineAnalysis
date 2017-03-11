package com.tosit.project.javautils;

/**
 * Created by mac on 2017/3/11.
 */

public class StringUtils {

    /**
     * 用于消除字符串首位的逗号
     *
     * @param str 字符串
     * @return String
     */
    public static String trimComma(String str) {
        if (str.startsWith(",")) {
            str = str.substring(1);
        }
        if (str.endsWith(",")) {
            str = str.substring(0, str.length() - 1);
        }

        return str;
    }

    /**
     * 从字符串中提取指定的字段
     *
     * @param str       字符串
     * @param delimiter 分隔符
     * @param field     指定的字段
     * @return String
     */
    public static String getFieldFromConcatString(String str, String delimiter, String field) {

        try {
            String[] fileds = str.split(delimiter);

            for (String concatFiled : fileds) {
                if (concatFiled.split("=").length == 2) {
                    String fieldName = concatFiled.split("=")[0];
                    String fieldValue = concatFiled.split("=")[1];
                    if (fieldName.equals(field)) {
                        return fieldValue;
                    }
                }
            }
        } catch (Exception e) {
            e.printStackTrace();
        }

        return null;
    }


    /**
     * 改变字符串中指定的字段对应的值
     *
     * @param str           字符串
     * @param delimiter     分隔符
     * @param field         字段
     * @param newfiledValue 新值
     * @return String
     */
    public static String setFieldInConcatString(String str, String delimiter, String field, String newfiledValue) {


        String[] fields = str.split(delimiter);
        for (int i = 0; i < fields.length; i++) {
            String fieldName = fields[i].split("=")[0];
            if (fieldName.equals(field)) {
                String concatField = fieldName + "=" + newfiledValue;
                fields[i] = concatField;
                break;
            }
        }

        StringBuffer buffer = new StringBuffer("");

        for (int i = 0; i < fields.length; i++) {
            buffer.append(fields[i]);
            if (i < fields.length - 1) {
                buffer.append("|");
            }
        }

        return buffer.toString();
    }

}

