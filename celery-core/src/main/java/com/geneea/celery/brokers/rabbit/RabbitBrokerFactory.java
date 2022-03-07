package com.geneea.celery.brokers.rabbit;

import com.geneea.celery.spi.BrokerFactory;

import com.google.common.collect.ImmutableSet;
import com.rabbitmq.client.Connection;
import com.rabbitmq.client.ConnectionFactory;

import java.io.IOException;
import java.net.URI;
import java.net.URISyntaxException;
import java.security.KeyManagementException;
import java.security.NoSuchAlgorithmException;
import java.util.Set;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.TimeoutException;

/**
 * A factory class for {@link RabbitBroker}.
 */
public class RabbitBrokerFactory implements BrokerFactory {

    private static final Set<String> PROTOCOLS = ImmutableSet.of("amqp", "amqps");

    @Override
    public Set<String> getProtocols() {
        return PROTOCOLS;
    }

    @Override
    public RabbitBroker createBroker(URI uri, ExecutorService executor) throws IOException, TimeoutException {
        ConnectionFactory factory = new ConnectionFactory();
        try {
            factory.setUri(uri);
        } catch (NoSuchAlgorithmException | KeyManagementException | URISyntaxException e) {
            throw new IOException(e);
        }

        return new RabbitBroker(factory.newConnection(executor));
    }
}
