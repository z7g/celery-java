
package com.geneea.celery;

import com.geneea.celery.backends.rabbit.RabbitBackend;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.node.ArrayNode;
import com.fasterxml.jackson.databind.node.ObjectNode;
import com.google.common.base.Stopwatch;
import com.google.common.collect.Streams;
import com.rabbitmq.client.AMQP.BasicProperties;
import com.rabbitmq.client.Connection;
import com.rabbitmq.client.ConnectionFactory;
import com.rabbitmq.client.DefaultConsumer;
import com.rabbitmq.client.Envelope;
import lombok.extern.slf4j.Slf4j;

import javax.annotation.Nonnull;
import javax.annotation.Nullable;
import java.io.Closeable;
import java.io.IOException;
import java.lang.reflect.Method;
import java.net.URISyntaxException;
import java.security.GeneralSecurityException;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeoutException;
import java.util.concurrent.locks.ReentrantLock;
import java.util.regex.Matcher;
import java.util.regex.Pattern;
import java.util.stream.Stream;

/**
 * The core implementation of a worker that listens on <b>RabbitMQ</b> queue and executes tasks.
 * It always uses the {@link RabbitBackend}.
 */
@Slf4j
public abstract class CeleryWorkerCore extends DefaultConsumer implements Closeable {

    private static final Pattern TASK_NAME = Pattern.compile("^(.+)#(.+)$");

    private final ReentrantLock taskRunning = new ReentrantLock();
    private final RabbitBackend backend;
    private final String queue;
    private final ObjectMapper jsonMapper;

    /**
     * @param connection the RabbitMQ connection to be used
     * @param queue routing tag (specifies the Rabbit queue where to listen)
     * @param jsonMapper override for the used JSON mapper
     * @throws java.io.IOException if the connection I/O problem occurs
     */
    protected CeleryWorkerCore(
            @Nonnull final Connection connection,
            @Nullable final String queue,
            @Nullable final ObjectMapper jsonMapper
    ) throws IOException {
        super(connection.createChannel());
        this.queue = queue != null ? queue : "celery";
        this.jsonMapper = jsonMapper != null ? jsonMapper : new ObjectMapper();
        this.backend = new RabbitBackend(getChannel(), this.jsonMapper);
    }

    /**
     * Creates new RabbitMQ connection.
     * @param uri the connection URI
     * @param executor override for the used executor service
     * @return new RabbitMQ connection
     * @throws java.io.IOException if the connection I/O problem occurs
     * @throws java.util.concurrent.TimeoutException if connecting times out
     */
    public static Connection connect(
            @Nonnull final String uri,
            @Nullable final ExecutorService executor
    ) throws IOException, TimeoutException {
        try {
            ConnectionFactory factory = new ConnectionFactory();
            factory.setUri(uri);
            return factory.newConnection(executor != null ? executor : Executors.newCachedThreadPool());
        } catch (URISyntaxException | GeneralSecurityException e) {
            throw new IllegalArgumentException(e);
        }
    }

    /**
     * Starts this worker listening on a RabbitMQ queue.
     * @throws java.io.IOException if an I/O problem occurs
     */
    public final void start() throws IOException {
        // max number of unacknowledged messages "in-flight" from the queue to the consumer
        getChannel().basicQos(2, false);
        getChannel().queueDeclare(queue, true, false, false, null);
        getChannel().basicConsume(queue, false, "", true, false, null, this);

        Runtime.getRuntime().addShutdownHook(new Thread(() -> {
            try {
                close();
                join();
            } catch (IOException e) {
                log.error("ShutdownHook exception", e);
            }
        }));
    }

    @Override
    public final void handleDelivery(
            String consumerTag, Envelope envelope,
            BasicProperties properties, byte[] body
    ) throws IOException {
        String taskId = properties.getHeaders().get("id").toString();
        taskRunning.lock();
        try {
            Stopwatch stopwatch = Stopwatch.createStarted();
            String message = new String(body, properties.getContentEncoding());

            JsonNode payload = jsonMapper.readTree(message);

            String taskName = properties.getHeaders().get("task").toString();
            Object result = processTask(
                    taskName,
                    (ArrayNode) payload.get(0),
                    (ObjectNode) payload.get(1)
            );

            log.info("CeleryTask {}[{}] succeeded in {}", taskName, taskId, stopwatch);
            log.debug("CeleryTask {}[{}] result was: {}", taskName, taskId, result);

            backend.reportResult(taskId, properties.getReplyTo(), properties.getCorrelationId(), result);

            getChannel().basicAck(envelope.getDeliveryTag(), false);
        } catch (DispatchException e) {
            Throwable cause = e.getCause() != null ? e.getCause() : e;
            log.error(String.format("CeleryTask %s - dispatch error", taskId), e);
            backend.reportException(taskId, properties.getReplyTo(), properties.getCorrelationId(), cause);
            getChannel().basicAck(envelope.getDeliveryTag(), false);
        } catch (IOException e) {
            log.error(String.format("CeleryTask %s - processing error", taskId), e);
            backend.reportException(taskId, properties.getReplyTo(), properties.getCorrelationId(), e);
            getChannel().basicNack(envelope.getDeliveryTag(), false, false);
        } catch (RuntimeException e) {
            log.error(String.format("CeleryTask %s - runtime error", taskId), e);
            backend.reportException(taskId, properties.getReplyTo(), properties.getCorrelationId(), e);
            getChannel().basicNack(envelope.getDeliveryTag(), false, false);
        } finally {
            taskRunning.unlock();
        }
    }

    /**
     * Implements a particular search method of component discovery.
     * @param className class name for a task to be executed
     * @return the found "task object", or {@code null} when it could not be found
     */
    protected abstract Object findTask(String className);

    private Object processTask(String taskName, ArrayNode args, ObjectNode kwargs) throws DispatchException {
        Matcher matcher = TASK_NAME.matcher(taskName);
        if (!matcher.matches()) {
            throw new DispatchException(
                    "This worker can only process tasks with name in form \"package.ClassName#method\", " +
                    "got: \"%s\"", taskName
            );
        }
        String className = matcher.group(1);
        String methodName = matcher.group(2);

        Object taskObj = findTask(className);
        if (taskObj == null) {
            throw new DispatchException("CeleryTask \"%s\" could not be found!", className);
        }

        Method method = Stream.of(taskObj.getClass().getDeclaredMethods())
                .filter(m -> m.getName().equals(methodName))
                .findFirst().orElse(null);
        if (method == null) {
            throw new DispatchException("CeleryTask \"%s:%s\" could not be found!", className, methodName);
        }

        try {
            Object[] convertedArgs = Streams.mapWithIndex(
                    Stream.of(method.getParameterTypes()),
                    (paramType, i) -> jsonMapper.convertValue(args.get((int) i), paramType)
            ).toArray();

            return method.invoke(taskObj, convertedArgs);
        } catch (IllegalArgumentException | ReflectiveOperationException e) {
            Throwable cause = e.getCause() != null ? e.getCause() : e;
            throw new DispatchException(cause, "Error calling %s", method);
        }
    }

    @Override
    public final void close() throws IOException {
        getChannel().abort();
        backend.close();
    }

    private void join() {
        taskRunning.lock();
        taskRunning.unlock();
    }
}
