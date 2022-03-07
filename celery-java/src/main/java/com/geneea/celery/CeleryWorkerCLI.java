package com.geneea.celery;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.rabbitmq.client.Connection;
import net.sourceforge.argparse4j.ArgumentParsers;
import net.sourceforge.argparse4j.inf.ArgumentParser;
import net.sourceforge.argparse4j.inf.ArgumentParserException;
import net.sourceforge.argparse4j.inf.Namespace;

import java.io.IOException;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeoutException;

/**
 * Simple CLI for the {@link CeleryWorker}, you can supply your tasks on classpath like this:
 * <pre>
 * java -cp celery-java-xyz.jar:your-tasks.jar com.geneea.celery.CeleryWorkerCLI --concurrency 8
 * </pre>
 */
public class CeleryWorkerCLI {

    public static void main(final String[] args) throws IOException {
        final ArgumentParser parser = ArgumentParsers.newFor(CeleryWorkerCLI.class.getSimpleName()).build();
        parser.addArgument("-q", "--queue").dest("queue").setDefault("celery")
                .help("Celery queue to watch");
        parser.addArgument("-c", "--concurrency").dest("numWorkers").type(Integer.class).setDefault(2)
                .help("Number of concurrent tasks to process");
        parser.addArgument("-b", "--broker").dest("broker").setDefault("amqp://localhost/%2F")
                .help("Broker URL, e.g. amqp://localhost//");

        final Namespace ns = parser.parseArgsOrFail(args);
        final String queue = ns.get("queue");
        final int numWorkers = ns.get("numWorkers");
        final String broker = ns.get("broker");

        final Connection connection;
        try {
            connection = CeleryWorker.connect(broker, Executors.newCachedThreadPool());
        } catch (IOException | TimeoutException | IllegalArgumentException e) {
            parser.handleError(new ArgumentParserException("bad \"broker\" argument", e, parser));
            System.exit(1);
            return;
        }

        final ObjectMapper jsonMapper = new ObjectMapper();
        for (int i = 0; i < numWorkers; i++) {
            CeleryWorker.builder()
                    .connection(connection)
                    .queue(queue)
                    .jsonMapper(jsonMapper)
                    .build()
                    .start();
        }

        System.out.printf("Started consuming tasks from queue %s.%n", queue);
        System.out.println("Known tasks:");
        for (String taskName : TaskRegistry.getRegisteredTaskNames()) {
            System.out.printf("  - %s%n", taskName);
        }
    }

}
