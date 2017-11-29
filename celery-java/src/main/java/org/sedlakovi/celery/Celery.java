package org.sedlakovi.celery;


import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.node.ArrayNode;
import com.google.common.base.Joiner;
import com.google.common.util.concurrent.ListenableFuture;
import com.google.common.util.concurrent.SettableFuture;
import org.sedlakovi.celery.spi.Backend;
import org.sedlakovi.celery.spi.Broker;
import org.sedlakovi.celery.spi.Message;

import java.io.IOException;
import java.net.InetAddress;
import java.util.*;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.Future;

/**
 * A client allowing you to submit a task and get a {@link Future} describing the result.
 */
public class Celery {
    private final String clientId;
    private final String clientName;
    private final ObjectMapper jsonMapper;
    private final Broker broker;
    private final String queue;
    private final Optional<Backend.ResultsProvider> resultsProvider;

    public Celery(Broker broker, String queue) throws IOException {
        this(broker, queue, null);
    }

    /**
     * Creates Celery client pushing tasks to the default queue - "celery".
     *
     */
    public Celery(Broker broker, Backend backend) throws IOException {
        this(broker, "celery", backend);
    }

    /**
     * Create a Celery client that can submit tasks and get the results from the backend.
     *
     * @param broker for dispatching messages
     * @param backend task backend
     * @param queue routing tag (specifies into which Rabbit queue the messages will go)
     * @throws IOException
     */
    public Celery(Broker broker, String queue, Backend backend) throws IOException {
        this.broker = broker;
        this.queue = queue;
        this.clientId = UUID.randomUUID().toString();
        this.clientName = clientId + "@" + InetAddress.getLocalHost().getHostName();
        this.jsonMapper = new ObjectMapper();

        if (backend == null) {
            resultsProvider = Optional.empty();
        } else {
            resultsProvider = Optional.of(backend.resultsProviderFor(clientId));
        }
    }

    public Celery(Broker broker) throws IOException {
        this(broker, "celery");
    }

    /**
     * Submit a Java task for processing. You'll probably not need to call this method. rather use @{@link CeleryTask}
     * annotation.
     *
     * @param taskClass task implementing class
     * @param method method in {@code taskClass} that does the work
     * @param args positional arguments for the method (need to be JSON serializable)
     * @return asynchronous result
     *
     * @throws IOException
     */
    public AsyncResult<?> submit(Class<?> taskClass, String method, Object[] args) throws IOException {
        return submit(taskClass.getName() + "#" + method, args);
    }

    /**
     * Submit a task by name. A low level method for submitting arbitrary tasks that don't have their proxies
     * generated by @{@link CeleryTask} annotation.
     *
     * @param name task name as understood by the worker
     * @param args positional arguments for the method (need to be JSON serializable)
     * @return asynchronous result
     * @throws IOException
     */
    public AsyncResult<?> submit(String name, Object[] args) throws IOException {
        String taskId = UUID.randomUUID().toString();

        ArrayNode payload = jsonMapper.createArrayNode();
        ArrayNode argsArr = payload.addArray();
        for (Object arg : args) {
            argsArr.addPOJO(arg);
        }
        payload.addObject();
        payload.addObject()
                .putNull("callbacks")
                .putNull("chain")
                .putNull("chord")
                .putNull("errbacks");

        Message message = broker.newMessage();
        message.setBody(jsonMapper.writeValueAsBytes(payload));
        message.setContentEncoding("utf-8");
        message.setContentType("application/json");

        Message.Headers headers = message.getHeaders();
        headers.setId(taskId);
        headers.setTaskName(name);
        headers.setArgsRepr("(" + Joiner.on(", ").join(args) + ")");
        headers.setOrigin(clientName);
        if (resultsProvider.isPresent()) {
            headers.setReplyTo(clientId);
        }

        message.send(queue);

        Future<Object> result;
        if (resultsProvider.isPresent()) {
            result = resultsProvider.get().getResult(taskId);
        } else {
            result = CompletableFuture.completedFuture(null);
        }
        return new AsyncResultImpl<Object>(result);
    }

    public interface AsyncResult<T> {
        boolean isDone();

        T get() throws ExecutionException, InterruptedException;
    }

    private class AsyncResultImpl<T> implements AsyncResult<T> {

        private final Future<T> future;

        AsyncResultImpl(Future<T> future) {
            this.future = future;
        }

        @Override
        public boolean isDone() {
            return future.isDone();
        }

        @Override
        public T get() throws ExecutionException, InterruptedException {
            return future.get();
        }
    }
}
