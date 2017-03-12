package com.tosit.project.exception;

/**
 * json参数异常
 * <p>
 * Created by Wanghan on 2017/3/12.
 * Copyright © Wanghan SCU. All Rights Reserved
 */
public class ParameterException extends Exception {
    /**
     * Constructs an ParameterException with nothing.
     */
    public ParameterException() {
        super();
    }

    /**
     * Constructs an ParameterException with the specified detail message.
     *
     * @param message
     */
    public ParameterException(String message) {
        super(message);
    }

    /**
     * Constructs an ParameterException with the specified detail message and cause.
     *
     * @param message
     * @param cause
     */
    public ParameterException(String message, Throwable cause) {
        super(message, cause);
    }

    /**
     * Constructs an ParameterException with the specified cause
     *
     * @param cause
     */
    public ParameterException(Throwable cause) {
        super(cause);
    }
}
