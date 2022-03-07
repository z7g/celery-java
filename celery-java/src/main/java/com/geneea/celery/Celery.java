package com.geneea.celery;

import com.geneea.celery.spi.BackendFactory;
import com.geneea.celery.spi.BrokerFactory;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.google.common.util.concurrent.ListenableFuture;
import lombok.Builder;

import javax.annotation.Nonnull;
import javax.annotation.Nullable;
import java.util.ServiceLoader;
import java.util.concurrent.ExecutorService;

/**
 * A client allowing you to submit a task and get a {@link ListenableFuture} describing the result.
 * <p>
 *     Use {@link CeleryTask @CeleryTask} annotation to have auto-generated proxies for tasks.
 * </p>
 */
public class Celery extends CeleryClientCore {

    /**
     * Create a Celery client that can submit tasks and get the results from the backend.
     *
     * @param brokerUri connection to broker that will dispatch messages
     * @param backendUri connection to backend providing responses
     * @param queue routing tag (specifies into which Rabbit queue the messages will go)
     * @param executor override for the used executor service
     * @param jsonMapper override for the used JSON mapper
     */
    @Builder
    Celery(
            @Nonnull final String brokerUri,
            @Nullable final String backendUri,
            @Nullable final String queue,
            @Nullable final ExecutorService executor,
            @Nullable final ObjectMapper jsonMapper
    ) {
        super(brokerUri, backendUri, queue, executor, jsonMapper);
    }

    @Override
    protected Iterable<BrokerFactory> findBrokers() {
        return ServiceLoader.load(BrokerFactory.class);
    }

    @Override
    protected Iterable<BackendFactory> findBackends() {
        return ServiceLoader.load(BackendFactory.class);
    }
}
