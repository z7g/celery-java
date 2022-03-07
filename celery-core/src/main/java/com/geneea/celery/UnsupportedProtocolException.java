package com.geneea.celery;

import com.google.common.collect.ImmutableSet;

import java.util.Collection;
import java.util.Set;

/**
 * Thrown when the caller can't process the URI because it can't handle its protocol (scheme).
 */
public class UnsupportedProtocolException extends RuntimeException {

    public final String protocol;
    public final Set<String> supportedProtocols;

    public UnsupportedProtocolException(String protocol, Collection<String> supportedProtocols) {
        this.protocol = protocol;
        this.supportedProtocols = ImmutableSet.copyOf(supportedProtocols);
    }

    @Override
    public String getMessage() {
        return String.format("Unsupported protocol: \"%s\". Supported protocols are: %s", protocol, supportedProtocols);
    }
}
