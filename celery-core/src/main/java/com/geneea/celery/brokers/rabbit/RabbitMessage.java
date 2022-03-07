package com.geneea.celery.brokers.rabbit;

import com.geneea.celery.spi.Message;

import com.google.common.base.Strings;
import com.rabbitmq.client.AMQP;

import java.io.IOException;
import java.util.Arrays;
import java.util.HashMap;
import java.util.Map;

/**
 * Implementation of RabbitMQ message for {@link RabbitBroker}.
 */
class RabbitMessage implements Message {

    private final RabbitBroker broker;
    private final RabbitMessageHeaders headers;
    private final AMQP.BasicProperties.Builder props;

    private byte[] body = null;

    public RabbitMessage(RabbitBroker broker) {
        this.broker = broker;
        headers = new RabbitMessageHeaders();
        props = new AMQP.BasicProperties.Builder()
                .deliveryMode(2)
                .priority(0);
    }

    @Override
    public void setBody(byte[] body) {
        this.body = body;
    }

    @Override
    public void setContentEncoding(String contentEncoding) {
        props.contentEncoding(contentEncoding);
    }

    @Override
    public void setContentType(String contentType) {
        props.contentType(contentType);
    }

    @Override
    public Headers getHeaders() {
        return headers;
    }

    @Override
    public void send(String queue) throws IOException {
        queue = Strings.isNullOrEmpty(queue) ? "celery" : queue;
        AMQP.BasicProperties messageProperties = props.headers(headers.map).build();
        broker.getChannel().basicPublish("", queue, messageProperties, body);
    }

    class RabbitMessageHeaders implements Headers {

        private final Map<String, Object> map = new HashMap<>();

        RabbitMessageHeaders() {
            // http://docs.celeryproject.org/en/latest/internals/protocol.html
            // required:
            // task
            // id
            // root_id
            map.put("parent_id", null);
            map.put("lang", "py"); // sic
            map.put("group", null);

            // optional:
            // meth
            // shadow
            // origin
            map.put("timelimit", Arrays.asList(null, null));
            map.put("retries", 0);
            map.put("kwargsrepr", "{}");
            map.put("argsrepr", null);
            map.put("expires", null);
            map.put("eta", null);
        }

        @Override
        public void setId(String id) {
            props.correlationId(id);
            map.put("root_id", id);
            map.put("id", id);
        }

        @Override
        public void setArgsRepr(String argsRepr) {
            map.put("argsrepr", argsRepr);
        }

        @Override
        public void setOrigin(String origin) {
            map.put("origin", origin);
        }

        @Override
        public void setReplyTo(String clientId) {
            props.replyTo(clientId);
        }

        @Override
        public void setTaskName(String task) {
            map.put("task", task);
        }
    }
}
