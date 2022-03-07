package com.geneea.celery.backends.rabbit;

import com.geneea.celery.spi.BackendFactory;

import com.google.common.collect.ImmutableSet;
import com.rabbitmq.client.Connection;
import com.rabbitmq.client.ConnectionFactory;
import org.apache.http.client.utils.URIBuilder;

import java.io.IOException;
import java.net.URI;
import java.net.URISyntaxException;
import java.security.KeyManagementException;
import java.security.NoSuchAlgorithmException;
import java.util.Set;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.TimeoutException;

import static com.google.common.base.Preconditions.checkArgument;

/**
 * A factory class for {@link RabbitBackend}.
 */
public class RabbitBackendFactory implements BackendFactory {

    private static final ImmutableSet<String> PROTOCOLS = ImmutableSet.of("rpc");

    @Override
    public Set<String> getProtocols() {
        return PROTOCOLS;
    }

    @Override
    public RabbitBackend createBackend(URI uri, ExecutorService executor) throws IOException, TimeoutException {
        // Replace rpc:// -> amqp:// to be consistent with the Python API. The Python API uses rpc:// to designate AMQP
        // used in the manner of one return queue per client as opposed to one queue per returned message (the original
        // amqp:// protocol).
        //
        // The underlying RabbitMQ library wouldn't understand the rpc scheme so we construct the URI it can understand.
        checkArgument(PROTOCOLS.contains(uri.getScheme()), "the protocol must be rpc://");

        ConnectionFactory factory = new ConnectionFactory();
        try {
            URI correctSchemeUri = new URIBuilder(uri).setScheme("amqp").build();
            factory.setUri(correctSchemeUri);
        } catch (NoSuchAlgorithmException | KeyManagementException | URISyntaxException e) {
            throw new IOException(e);
        }

        Connection connection = factory.newConnection(executor);
        return new RabbitBackend(connection.createChannel());
    }
}
