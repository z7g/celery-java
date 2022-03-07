package com.geneea.celery.brokers.rabbit;

import com.geneea.celery.spi.BrokerFactory;

import org.kohsuke.MetaInfServices;

@MetaInfServices(BrokerFactory.class)
public class RabbitBrokerService extends RabbitBrokerFactory {

}
