package com.datastax.oss.pulsar.jms;

/**
 * @author 方俊锋[OF2593]
 * company qianmi.com
 * Date 2021-12-21
 */
public class MessageNotWriteableRuntimeException extends JMSRuntimeException {

    /**
     * Explicitly set serialVersionUID to be the same as the implicit serialVersionUID of the Jakarta Messaging 2.0 version
     */
    private static final long serialVersionUID = 6075922984499850209L;

    /**
     * Constructs a {@code MessageNotWriteableRuntimeException} with the specified reason and error code.
     *
     * @param reason a description of the exception
     * @param errorCode a string specifying the vendor-specific error code
     *
     **/
    public MessageNotWriteableRuntimeException(String reason, String errorCode) {
        super(reason, errorCode);
    }

    /**
     * Constructs a {@code MessageNotWriteableRuntimeException} with the specified reason. The error code defaults to null.
     *
     * @param reason a description of the exception
     **/
    public MessageNotWriteableRuntimeException(String reason) {
        super(reason);
    }

    /**
     * Constructs a {@code MessageNotWriteableRuntimeException} with the specified detail message, error code and cause
     *
     * @param detailMessage a description of the exception
     * @param errorCode a provider-specific error code
     * @param cause the underlying cause of this exception
     */
    public MessageNotWriteableRuntimeException(String detailMessage, String errorCode, Throwable cause) {
        super(detailMessage, errorCode, cause);
    }

}
