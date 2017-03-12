package com.tosit.project.exception;

/**
 * 字符串分割异常类
 * <p>
 * Created by Wanghan on 2017/3/12.
 * Copyright © Wanghan SCU. All Rights Reserved
 */
public class StringSepatorException extends Exception {
    /**
     * Constructs an StringSepatorException with nothing.
     */
    public StringSepatorException() {
        super();
    }

    /**
     * Constructs an StringSepatorException with the specified detail message.
     *
     * @param message
     */
    public StringSepatorException(String message) {
        super(message);
    }

    /**
     * Constructs an StringSepatorException with the specified detail message and cause.
     *
     * @param message
     * @param cause
     */
    public StringSepatorException(String message, Throwable cause) {
        super(message, cause);
    }

    /**
     * Constructs an TaskException with the specified cause
     *
     * @param cause
     */
    public StringSepatorException(Throwable cause) {
        super(cause);
    }

}
