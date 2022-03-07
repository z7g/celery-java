package com.geneea.celery;

/**
 * Service (instantiated by {@link java.util.ServiceLoader}) instantiating a {@link CeleryTask} within {@link CeleryWorker}.
 * <p>
 * Such loader is generated automatically when processing the {@link CeleryTask} annotation.
 */
public interface CeleryTaskLoader<T> {
    T loadTask();
}
