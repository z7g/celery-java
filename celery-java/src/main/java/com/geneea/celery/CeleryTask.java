package com.geneea.celery;

import java.lang.annotation.ElementType;
import java.lang.annotation.Retention;
import java.lang.annotation.RetentionPolicy;
import java.lang.annotation.Target;

/**
 * Marks your code as a <em>CeleryTask</em>.
 * <p>
 * The annotation processor included in this module then generates two classes - {@code *Proxy} and {@code *Loader}.
 * <p>
 * All parameters and return types must be JSON-serializable.
 * <p>
 * In order for the {@link CeleryWorker} to find your Tasks, you must register them as a service in {@code META-INF/services}.
 * An easy way to do it is to annotate your CeleryTask implementation with {@code org.kohsuke.MetaInfServices} annotation. See
 * example tasks in the examples module.
 */
@Retention(RetentionPolicy.CLASS)
@Target(ElementType.TYPE)
public @interface CeleryTask {
}
