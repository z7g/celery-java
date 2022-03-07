package com.geneea.celery.backends.rabbit;

import com.geneea.celery.spi.BackendFactory;

import org.kohsuke.MetaInfServices;

@MetaInfServices(BackendFactory.class)
public class RabbitBackendService extends RabbitBackendFactory {

}
