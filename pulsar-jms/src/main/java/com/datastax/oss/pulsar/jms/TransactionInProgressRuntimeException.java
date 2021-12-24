package com.datastax.oss.pulsar.jms;

/**
 * @author 方俊锋[OF2593]
 * company qianmi.com
 * Date 2021-12-21
 */
public class TransactionInProgressRuntimeException extends JMSRuntimeException {

    /**
     * Explicitly set serialVersionUID to be the same as the implicit serialVersionUID of the Jakarta Messaging 2.0 version
     */
    private static final long serialVersionUID = -916492460069513065L;

    /**
     * Constructs a {@code TransactionInProgressRuntimeException} with the specified detail message
     *
     * @param detailMessage a description of the exception
     **/
    public TransactionInProgressRuntimeException(String detailMessage) {
        super(detailMessage);
    }

    /**
     * Constructs a {@code TransactionInProgressRuntimeException} with the specified detail message and error code.
     *
     * @param detailMessage a description of the exception
     * @param errorCode a provider-specific error code
     **/
    public TransactionInProgressRuntimeException(String detailMessage, String errorCode) {
        super(detailMessage, errorCode);
    }

    /**
     * Constructs a {@code TransactionInProgressRuntimeException} with the specified detail message, error code and cause
     *
     * @param detailMessage a description of the exception
     * @param errorCode a provider-specific error code
     * @param cause the underlying cause of this exception
     */
    public TransactionInProgressRuntimeException(String detailMessage, String errorCode, Throwable cause) {
        super(detailMessage, errorCode, cause);
    }

}
