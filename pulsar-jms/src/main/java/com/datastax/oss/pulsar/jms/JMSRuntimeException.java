package com.datastax.oss.pulsar.jms;

/**
 * This is the root class of all unchecked exceptions in the Jakarta Messaging API.
 *
 * <p>
 * In additional to the detailMessage and cause fields inherited from {@code Throwable}, this class also allows a
 * provider-specific errorCode to be set.
 *
 * @author 方俊锋[OF2593]
 * company qianmi.com
 * Date 2021-12-21
 */
public class JMSRuntimeException extends RuntimeException {

    /**
     * Explicitly set serialVersionUID to be the same as the implicit serialVersionUID of the Java Message Service 1.1 version
     */
    private static final long serialVersionUID = -5204332229969809982L;

    /**
     * Provider-specific error code.
     **/
    private String errorCode = null;

    /**
     * Constructs a {@code JMSRuntimeException} with the specified detail message and error code.
     *
     * @param detailMessage a description of the exception
     * @param errorCode a provider-specific error code
     **/
    public JMSRuntimeException(String detailMessage, String errorCode) {
        super(detailMessage);
        this.errorCode = errorCode;
    }

    /**
     * Constructs a {@code JMSRuntimeException} with the specified detail message
     *
     * @param detailMessage a description of the exception
     **/
    public JMSRuntimeException(String detailMessage) {
        super(detailMessage);
    }

    /**
     * Constructs a {@code JMSRuntimeException} with the specified detail message, error code and cause
     *
     * @param detailMessage a description of the exception
     * @param errorCode a provider-specific error code
     * @param cause the underlying cause of this exception
     */
    public JMSRuntimeException(String detailMessage, String errorCode, Throwable cause) {
        super(detailMessage, cause);
        this.errorCode = errorCode;
    }

    /**
     * Returns the vendor-specific error code.
     *
     * @return the provider-specific error code
     **/
    public String getErrorCode() {
        return this.errorCode;
    }
}
