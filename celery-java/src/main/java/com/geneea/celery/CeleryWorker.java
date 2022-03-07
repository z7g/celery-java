package com.geneea.celery;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.rabbitmq.client.Connection;
import lombok.Builder;

import javax.annotation.Nonnull;
import javax.annotation.Nullable;
import java.io.IOException;

/**
 * A worker that listens on RabbitMQ queue and executes <em>CeleryTask</em>.
 * You can either embed it into your project or start it stand-alone via {@link CeleryWorkerCLI}.
 * <p>
 *     Use {@link CeleryTask @CeleryTask} annotation to have auto-generated proxies for tasks.
 * </p>
 */
public class CeleryWorker extends CeleryWorkerCore {

    /**
     * Create a Celery worker that can listen on a queue and execute tasks.
     *
     * @param connection the RabbitMQ connection to be used
     * @param queue routing tag (specifies the Rabbit queue where to listen)
     * @param jsonMapper override for the used JSON mapper
     */
    @Builder
    CeleryWorker(
            @Nonnull final Connection connection,
            @Nullable final String queue,
            @Nullable final ObjectMapper jsonMapper
    ) throws IOException {
        super(connection, queue, jsonMapper);
    }

    @Override
    protected Object findTask(String className) {
        return TaskRegistry.getTask(className);
    }
}
