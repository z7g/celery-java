package com.geneea.celery;

import com.geneea.celery.spi.Backend;
import com.geneea.celery.spi.Backend.ResultsProvider;
import com.geneea.celery.spi.BackendFactory;
import com.geneea.celery.spi.Broker;
import com.geneea.celery.spi.BrokerFactory;
import com.geneea.celery.spi.Message;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.node.ArrayNode;
import com.google.common.base.Suppliers;
import com.google.common.collect.ImmutableSet;
import com.google.common.util.concurrent.Futures;
import com.google.common.util.concurrent.ListenableFuture;
import lombok.extern.slf4j.Slf4j;

import javax.annotation.Nonnull;
import javax.annotation.Nullable;
import java.io.Closeable;
import java.io.IOException;
import java.io.UncheckedIOException;
import java.net.InetAddress;
import java.net.URI;
import java.net.UnknownHostException;
import java.util.Collection;
import java.util.Optional;
import java.util.Set;
import java.util.UUID;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeoutException;
import java.util.function.Supplier;
import java.util.stream.Collectors;
import java.util.stream.Stream;

/**
 * The core implementation of a Celery client. It should work with any {@link Broker} or {@link Backend}.
 *
 * Thread safety: this client (and any of its subclasses) can be used to submit tasks concurrently from multiple
 * threads.
 */
@Slf4j
public abstract class CeleryClientCore implements Closeable {

    private static final String CONTENT_TYPE = "application/json";
    private static final String ENCODING = "utf-8";

    private final String clientId = UUID.randomUUID().toString();
    private final String clientName = clientId + "@" + getLocalHostName();

    private final URI brokerUri;
    private final URI backendUri;
    private final String queue;

    private final ObjectMapper jsonMapper;
    private final ExecutorService executor;

    // Memoized suppliers help us to deal with a connection that can't be established yet. It may fail several times
    // with an exception but when it succeeds, it then always returns the same instance.
    //
    // This is tailored for the RabbitMQ connections - they fail to be created if the host can't be reached but they
    // can heal automatically. If other brokers/backends don't work this way, we might need to rework it.
    private final Supplier<Optional<ResultsProvider<?>>> resultsProvider =
            Suppliers.memoize(this::resultsProviderSupplier);
    private final Supplier<Broker> broker = Suppliers.memoize(this::brokerSupplier);

    /**
     * @param brokerUri connection to broker that will dispatch messages
     * @param backendUri connection to backend providing responses
     * @param queue routing tag (specifies into which queue the messages will go)
     * @param executor override for the used executor service
     * @param jsonMapper override for the used JSON mapper
     */
    protected CeleryClientCore(
            @Nonnull final String brokerUri,
            @Nullable final String backendUri,
            @Nullable final String queue,
            @Nullable final ExecutorService executor,
            @Nullable final ObjectMapper jsonMapper
    ) {
        this.brokerUri = URI.create(brokerUri);
        this.backendUri = backendUri != null ? URI.create(backendUri) : null;
        this.queue = queue != null ? queue : "celery";

        this.executor = executor != null ? executor : Executors.newCachedThreadPool();
        this.jsonMapper = jsonMapper != null ? jsonMapper : new ObjectMapper();
    }

    /**
     * Implements a particular search method of component discovery.
     * @return available {@link BrokerFactory factories} for creating {@link Broker} instances
     */
    protected abstract Iterable<BrokerFactory> findBrokers();

    /** Gets {@link Broker} for the configured {@code brokerUri} and {@code queue}. */
    private Broker brokerSupplier() {
        try {
            ImmutableSet.Builder<String> knownProtocols = ImmutableSet.builder();

            for (BrokerFactory factory: findBrokers()) {
                Set<String> factoryProtocols = factory.getProtocols();
                knownProtocols.addAll(factoryProtocols);

                if (factoryProtocols.contains(brokerUri.getScheme())) {
                    Broker b = factory.createBroker(brokerUri, executor);
                    b.declareQueue(queue);
                    return b;
                }
            }

            throw new UnsupportedProtocolException(brokerUri.getScheme(), knownProtocols.build());
        } catch (IOException e) {
            throw new UncheckedIOException(e);
        } catch (TimeoutException e) {
            throw new RuntimeException(e);
        }
    }

    /**
     * Implements a particular search method of component discovery.
     * @return available {@link BackendFactory factories} for creating {@link Backend} instances
     */
    protected abstract Iterable<BackendFactory> findBackends();

    /** Gets {@link ResultsProvider} for the configured {@code backendUri} and {@code clientId}. */
    private Optional<ResultsProvider<?>> resultsProviderSupplier() {
        if (backendUri == null) {
            return Optional.empty();
        }

        try {
            ImmutableSet.Builder<String> knownProtocols = ImmutableSet.builder();

            for (BackendFactory factory: findBackends()) {
                Collection<String> factoryProtocols = factory.getProtocols();
                knownProtocols.addAll(factoryProtocols);

                if (factoryProtocols.contains(backendUri.getScheme())) {
                    ResultsProvider<?> rp = factory
                            .createBackend(backendUri, executor)
                            .resultsProviderFor(clientId);
                    return Optional.of(rp);
                }
            }

            throw new UnsupportedProtocolException(backendUri.getScheme(), knownProtocols.build());
        } catch (IOException e) {
            throw new UncheckedIOException(e);
        } catch (TimeoutException e) {
            throw new RuntimeException(e);
        }
    }

    private static String getLocalHostName() {
        try {
            return InetAddress.getLocalHost().getHostName();
        } catch (UnknownHostException e) {
            return "unknown";
        }
    }

    /**
     * Submit a Java task for processing. You'll probably not need to call this method.
     *
     * @param taskClass task implementing class
     * @param method method in {@code taskClass} that does the work
     * @param args positional arguments for the method (need to be JSON serializable)
     * @param <T> task type
     * @param <R> task result type
     * @return asynchronous result
     *
     * @throws IOException if the message couldn't be sent
     */
    public final <T, R> ListenableFuture<R> submit(Class<T> taskClass, String method, Object[] args) throws IOException {
        return submit(taskClass.getName() + "#" + method, args);
    }

    /**
     * Submit a task by name. A low level method for submitting arbitrary tasks. The message constructed by this
     * method and sent to the underlying broker conforms Celery Message Protocol Version 2.
     *
     * @param name task name as understood by the worker
     * @param args positional arguments for the method (need to be JSON serializable)
     * @param <R> task result type
     * @return asynchronous result
     *
     * @throws IOException if the message couldn't be sent
     * @see <a href="http://docs.celeryproject.org/en/latest/internals/protocol.html">Celery Message Protocol Version 2</a>
     */
    public final <R> ListenableFuture<R> submit(String name, Object[] args) throws IOException {
        // Get the provider early to increase the chance to find out there is a connection problem before actually
        // sending the message.
        //
        // This will help for example in the case when the connection can't be established at all. The connection may
        // still drop after sending the message but there isn't much we can do about it.
        Optional<ResultsProvider<?>> rp = resultsProvider.get();
        String taskId = UUID.randomUUID().toString();

        ArrayNode payload = jsonMapper.createArrayNode();
        // args
        ArrayNode argsArr = payload.addArray();
        for (Object arg : args) {
            argsArr.addPOJO(arg);
        }
        // kwargs
        payload.addObject();
        // embed
        payload.addObject()
                .putNull("callbacks")
                .putNull("chain")
                .putNull("chord")
                .putNull("errbacks");

        Message message = broker.get().newMessage();
        message.setBody(jsonMapper.writeValueAsBytes(payload));
        message.setContentEncoding(ENCODING);
        message.setContentType(CONTENT_TYPE);

        Message.Headers headers = message.getHeaders();
        headers.setId(taskId);
        // presence of "task" header implies Version 2 Celery protocol
        headers.setTaskName(name);
        headers.setArgsRepr(
            Stream.of(args).map(CeleryClientCore::toDebugString).collect(Collectors.joining(", ", "(", ")"))
        );
        headers.setOrigin(clientName);
        if (rp.isPresent()) {
            headers.setReplyTo(clientId);
        }

        message.send(queue);
 
        if (rp.isPresent()) {
            @SuppressWarnings("unchecked")
            ResultsProvider<R> provider = (ResultsProvider<R>) rp.get();
            return provider.getResult(taskId);
        } else {
            return Futures.immediateFuture(null);
        }
    }
    
    public CeleryResult apply(String name, Object[] args) { 
    	
    	ListenableFuture<?> future=null;
		try {
			future=submit(name,args);
		} catch (IOException e) {
			e.printStackTrace();
		}
    	
		return new CeleryResult(future);
    }
    
    public String apply2(String name, Object[] args) throws IOException{
    	 Optional<ResultsProvider<?>> rp = resultsProvider.get();
         String taskId = UUID.randomUUID().toString();

         ArrayNode payload = jsonMapper.createArrayNode();
         // args
         ArrayNode argsArr = payload.addArray();
         for (Object arg : args) {
             argsArr.addPOJO(arg);
         }
         // kwargs
         payload.addObject();
         // embed
         payload.addObject()
                 .putNull("callbacks")
                 .putNull("chain")
                 .putNull("chord")
                 .putNull("errbacks");

         Message message = broker.get().newMessage();
         message.setBody(jsonMapper.writeValueAsBytes(payload));
         message.setContentEncoding(ENCODING);
         message.setContentType(CONTENT_TYPE);

         Message.Headers headers = message.getHeaders();
         headers.setId(taskId);
         // presence of "task" header implies Version 2 Celery protocol
         headers.setTaskName(name);
         headers.setArgsRepr(
             Stream.of(args).map(CeleryClientCore::toDebugString).collect(Collectors.joining(", ", "(", ")"))
         );
         headers.setOrigin(clientName);
         if (rp.isPresent()) {
             headers.setReplyTo(clientId);
         }

         message.send(queue);     
            
         return taskId; 
    }

    @Override
    public final void close() throws IOException {
        broker.get().close();
        Optional<Backend> b = resultsProvider.get()
                .map(ResultsProvider::getBackend);
        if (b.isPresent()) {
            b.get().close();
        }
    }

    /**
     * Converts an arbitrary object to a String. It checks for {@code null} and tries to produce reasonably
     * short string representations:
     *
     * <li>{@link CharSequence} instances are trimmed if longer then 100; the trimmed string will show the beginning
     * and the ending characters
     * <li>simple objects (eg. {@link Number}, {@link Boolean}, etc) are converted using their {@code toString()}
     * <li>complex objects are converted to their class name and identity hashcode
     *
     * @param o object to convert
     * @return a string representation of the object
     */
    private static String toDebugString(Object o) {
        if (o == null) {
            return "null";
        } else if (o instanceof CharSequence) {
            CharSequence s = (CharSequence) o;
            return s.length() < 100
                ? "\"" + s.toString() + "\""
                : "\"" + s.subSequence(0, 30) + "..." + s.subSequence(s.length() - 30, s.length()) + "\"";
        } else if (o instanceof Number || o instanceof Boolean) {
            return o.toString();
        } else {
            return o.getClass().getName() + "@" + Integer.toHexString(System.identityHashCode(o));
        }
    }
}
