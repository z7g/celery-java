package com.geneea.celery;

import com.google.common.collect.ImmutableMap;
import com.google.common.collect.Streams;

import java.util.Map;
import java.util.ServiceLoader;
import java.util.Set;
import java.util.function.Function;

/**
 * Loads registered {@link CeleryTask} services and provides them by their class name.
 */
class TaskRegistry {

    // holder that ensures lazy loading
    private static final class TasksHolder {
        private static final Map<String, ?> TASKS = Streams
                .stream(ServiceLoader.load(CeleryTaskLoader.class))
                .map(CeleryTaskLoader::loadTask)
                .collect(ImmutableMap.toImmutableMap((v) -> v.getClass().getName(), Function.identity()));
    }

    static Set<String> getRegisteredTaskNames() {
        return TasksHolder.TASKS.keySet();
    }

    @SuppressWarnings("unchecked")
    static <T> T getTask(String taskName) {
        return (T) TasksHolder.TASKS.get(taskName);
    }
}
