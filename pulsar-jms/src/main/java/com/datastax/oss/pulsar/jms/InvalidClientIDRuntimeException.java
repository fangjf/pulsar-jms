package com.datastax.oss.pulsar.jms;

/**
 * @author 方俊锋[OF2593]
 * company qianmi.com
 * Date 2021-12-21
 */
public class InvalidClientIDRuntimeException extends JMSRuntimeException {

    /**
     * Explicitly set serialVersionUID to be the same as the implicit serialVersionUID of the Jakarta Messaging 2.0 version
     */
    private static final long serialVersionUID = -1997236208457896631L;

    /**
     * Constructs a {@code InvalidClientIDRuntimeException} with the specified detail message
     *
     * @param detailMessage a description of the exception
     **/
    public InvalidClientIDRuntimeException(String detailMessage) {
        super(detailMessage);
    }

    /**
     * Constructs a {@code InvalidClientIDRuntimeException} with the specified detail message and error code.
     *
     * @param detailMessage a description of the exception
     * @param errorCode a provider-specific error code
     **/
    public InvalidClientIDRuntimeException(String detailMessage, String errorCode) {
        super(detailMessage, errorCode);
    }

    /**
     * Constructs a {@code InvalidClientIDRuntimeException} with the specified detail message, error code and cause
     *
     * @param detailMessage a description of the exception
     * @param errorCode a provider-specific error code
     * @param cause the underlying cause of this exception
     */
    public InvalidClientIDRuntimeException(String detailMessage, String errorCode, Throwable cause) {
        super(detailMessage, errorCode, cause);
    }
}
