package ru.cinimex.cachalot;

import java.util.ArrayList;
import java.util.Collection;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.concurrent.CompletionService;
import java.util.concurrent.ExecutorCompletionService;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.function.Supplier;
import java.util.stream.IntStream;
import javax.jms.ConnectionFactory;
import javax.jms.Message;
import javax.jms.TextMessage;
import javax.sql.DataSource;

import org.junit.Assert;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.springframework.jdbc.core.JdbcTemplate;
import org.springframework.jms.core.JmsTemplate;
import org.springframework.test.context.junit4.SpringRunner;

import lombok.AccessLevel;
import lombok.NoArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import ru.cinimex.cachalot.validation.JdbcValidationRule;

import static org.hamcrest.core.IsCollectionContaining.hasItems;
import static org.springframework.util.Assert.notEmpty;
import static org.springframework.util.Assert.notNull;

@Slf4j
@RunWith(SpringRunner.class)
@SuppressWarnings({"unused", "FieldCanBeLocal", "WeakerAccess"})
@NoArgsConstructor(access = AccessLevel.PROTECTED)
public abstract class CachalotEntrails {

    private JmsCachalotEntrails jmsCachalotEntrails;
    private JdbcCachalotEntrails jdbcCachalotEntrails;
    private CompletionService<String> cachalotTummy;
    private boolean traceOn;
    private final Collection<String> digested = new ArrayList<>();

    /**
     * Configure your test flow using {@link CachalotEntrails} dsl.
     *
     * @throws Exception in case you wanna say something.
     */
    protected abstract void feed() throws Exception;

    /**
     * Local logs will contain all information about configuration and processing.
     * The output can be quite complex.
     * @return self.
     */
    public CachalotEntrails enableDataTrace() {
        traceOn = true;
        return this;
    }

    /**
     * Indicates, that your test use jms as underlying system. Method accepts
     * {@link javax.jms.ConnectionFactory} as input and opens different scope of
     * jms related api calls.
     * @param factory is {@link ConnectionFactory}
     * @return nested config as {@link JmsCachalotEntrails}
     */
    public JmsCachalotEntrails usingJms(final ConnectionFactory factory) {
        jmsCachalotEntrails = new JmsCachalotEntrails(factory);
        return jmsCachalotEntrails;
    }

    /**
     * Indicates, that your test use database for manipulating data before/after execution.
     * @param dataSource is {@link DataSource}
     * @return nested config as {@link JdbcCachalotEntrails}
     */
    public JdbcCachalotEntrails withState(final DataSource dataSource) {
        jdbcCachalotEntrails = new JdbcCachalotEntrails(dataSource);
        return jdbcCachalotEntrails;
    }

    private void revealWomb(String say, Object... what) {
        if (traceOn) {
            log.info(say, what);
        }
    }

    @Test
    public void deepSwim() throws Exception {
        revealWomb("Prepare to deep swim");
        feed();
        revealWomb("Cachalot feeded");

        //Sync cause we don't need parallel here
        Optional.ofNullable(jdbcCachalotEntrails).ifPresent(cachalotEntrails -> {
            for (Supplier<? extends String> supplier : cachalotEntrails.initialState) {
                String query = supplier.get();
                revealWomb("Calling {}", query);
                cachalotEntrails.jdbcTemplate.execute(query);
            }
        });

        //Sync cause we don't need parallel here
        Optional.ofNullable(jmsCachalotEntrails).ifPresent(cachalotEntrails -> {
            revealWomb("Prepare to send {} into {}", cachalotEntrails.inMessage, cachalotEntrails.inQueue);
            cachalotEntrails.jmsTemplate.send(cachalotEntrails.inQueue, session -> {
                TextMessage message = session.createTextMessage();
                if (cachalotEntrails.inMessage != null) {
                    message.setText(cachalotEntrails.inMessage);
                }
                for (Map.Entry<String, ? super Object> header : cachalotEntrails.headers.entrySet()) {
                    message.setObjectProperty(header.getKey(), header.getValue());
                }
                revealWomb("Message {} created", message);
                return message;
            });
            revealWomb("Message successfully sent into {}", cachalotEntrails.inQueue);
        });

        final Collection<Future<String>> calls = new ArrayList<>();

        //Multithreaded response consuming. Cause we could receive more than one response for one request.
        Optional.ofNullable(jmsCachalotEntrails).ifPresent(cachalotEntrails -> {
            if (cachalotEntrails.expectingResponse) {
                long timeout = cachalotEntrails.timeout;

                ExecutorService executor = Executors.newCachedThreadPool(runnable -> {
                    Thread thread = new Thread(runnable);
                    thread.setName("CachalotWatcher ");
                    return thread;
                });

                try {
                    //Possibility to uncontrolled growth.
                    cachalotTummy = new ExecutorCompletionService<>(executor);

                    List<String> queues = cachalotEntrails.outQueues;
                    List<JmsTemplate> receivers = cachalotEntrails.receivers;

                    String error = "Inconsistent state. Receivers count must match queues count";
                    Assert.assertEquals(error, queues.size(), receivers.size());

                    //noinspection CodeBlock2Expr
                    IntStream.range(0, queues.size()).forEach(i -> {
                        calls.add(cachalotTummy.submit(() -> {
                            revealWomb("Calling response from {} with timeout {} millis", queues.get(i), timeout);
                            JmsTemplate receiver = receivers.get(i);
                            receiver.setReceiveTimeout(timeout);
                            Message message = receiver.receive(queues.get(i));
                            //Avoid null check, but works only fro text message for now.
                            if (message instanceof TextMessage) {
                                revealWomb("Received text message");
                                return ((TextMessage) message).getText();
                            }
                            revealWomb("Received unknown type jms message");
                            return null;
                        }));
                    });
                } finally {
                    executor.shutdown();
                }

                Future<String> call;
                while (calls.size() > 0) {
                    try {
                        //block until a callable completes
                        call = cachalotTummy.take();
                        revealWomb("Received completed future: {}", call);
                        calls.remove(call);
                        //Get message, if the Callable was able to create it.
                        String message = call.get();
                        if (message == null) {
                            Assert.fail("Message was not received in configured timeout: " + timeout + " millis");
                        }
                        revealWomb("Received message:\n{}", message);
                        digested.add(message);
                    } catch (Exception e) {
                        Throwable cause = e.getCause();
                        log.error("Message receiving failed due to: " + cause, e);

                        for (Future<String> future : calls) {
                            //Try to cancel all pending tasks.
                            future.cancel(true);
                        }
                        //Fail
                        Assert.fail("Message receiving failed due to: " + cause);
                    }
                }

                //Now let's validate received bodies, if they was provided
                if (!cachalotEntrails.outMessages.isEmpty()) {
                    Collection<String> expected = cachalotEntrails.outMessages;
                    revealWomb("Expected responses size {}, actual responses size {}", expected.size(), digested.size());
                    Assert.assertEquals("Responses count doesn't match", expected.size(), digested.size());
                    Assert.assertThat(digested, hasItems(expected.toArray(new String[expected.size()])));
                }
            }
        });

        //Sync cause we don't need parallel here
        Optional.ofNullable(jdbcCachalotEntrails).ifPresent(cachalotEntrails -> {
            for (JdbcValidationRule<?> validationRule : cachalotEntrails.terminalState) {
                if (!validationRule.validate()) {
                    revealWomb("Validation rule violated {}", validationRule);
                    Assert.fail();
                } else {
                    revealWomb("Validation rule checked");
                }
            }
        });
    }

    @SuppressWarnings("MismatchedQueryAndUpdateOfCollection")
    protected final class JdbcCachalotEntrails {

        private final JdbcTemplate jdbcTemplate;
        private final Collection<Supplier<? extends String>> initialState = new ArrayList<>();
        private final Collection<JdbcValidationRule<?>> terminalState = new ArrayList<>();

        private JdbcCachalotEntrails(final DataSource dataSource) {
            notNull(dataSource, "DataSource must be specified");
            jdbcTemplate = new JdbcTemplate(dataSource, true);
            revealWomb("JdbcTemplate initialized with {}", dataSource);
        }

        public JdbcCachalotEntrails beforeFeed(Supplier<? extends String> initializer) {
            notNull(initializer, "Given initializer must not be null");
            initialState.add(initializer);
            revealWomb("Initializer added {}", initializer);
            return this;
        }

        public JdbcCachalotEntrails beforeFeed(Collection<Supplier<? extends String>> initializers) {
            notNull(initializers, "Given initializers must not be null");
            notEmpty(initializers, "Given initializers must not be null");
            initialState.addAll(initializers);
            revealWomb("Initializers added {}", initializers);
            return this;
        }

        public JdbcCachalotEntrails afterFeed(JdbcValidationRule<?> verificator) {
            notNull(verificator, "Given verificator must not be null");
            terminalState.add(verificator);
            revealWomb("Verificator added {}", verificator);
            return this;
        }

        public JdbcCachalotEntrails afterFeed(Collection<JdbcValidationRule<?>> verificators) {
            notNull(verificators, "Given verificators must not be null");
            notEmpty(verificators, "Given verificators must not be null");
            terminalState.addAll(verificators);
            revealWomb("Verificators added {}", verificators);
            return this;
        }

        public CachalotEntrails ingest() {
            return CachalotEntrails.this;
        }

    }

    @SuppressWarnings("MismatchedQueryAndUpdateOfCollection")
    protected final class JmsCachalotEntrails {

        private final JmsTemplate jmsTemplate;
        //We should create one receiver per queue; It's possible to receive multiple messages from the same queue, if
        //queue name was provided multiple times.
        private final List<JmsTemplate> receivers = new ArrayList<>();
        private String inQueue;
        private String inMessage;
        private final List<String> outQueues = new ArrayList<>();
        private final Collection<String> outMessages = new ArrayList<>();
        private final Map<String, ? super Object> headers = new HashMap<>();
        private boolean expectingResponse = true;
        private long timeout = Long.MAX_VALUE;

        private JmsCachalotEntrails(final ConnectionFactory factory) {
            notNull(factory, "Provided connection factory must not be null");
            jmsTemplate = new JmsTemplate(factory);
            revealWomb("JmsTemplate initialized with {}", factory);
        }

        private void validateState(String callFrom) {
            notNull(jmsTemplate, "Illegal call #" + callFrom + " before CachalotEntrails#usingJms");
        }

        /**
         * @param queue target queue to send message.
         * @return self.
         */
        public JmsCachalotEntrails sendTo(String queue) {
            validateState("sendTo");
            notNull(queue, "Send queue must be specified");
            this.inQueue = queue;
            revealWomb("In queue set {}", queue);
            return this;
        }

        /**
         * @param queue message queue to receive message from. This queue will be added to response queue collection.
         * By default assumed, that each queue produce one message. I.e. if you want to receive multiple messages
         * from one queue, you can call this method multiple times, or call #receiveFrom(Collection<String> outQueues).
         * This method call is not idempotent: it's changing state of underlying infrastructure.
         * @return self.
         */
        public JmsCachalotEntrails receiveFrom(String queue) {
            validateState("receiveFrom");
            notNull(queue, "Receive queue must be specified");
            outQueues.add(queue);
            revealWomb("Out queue set {}", queue);
            return this;
        }

        /**
         * Same as #receiveFrom(String outQueue), but for multiple queues at once.
         * @return self.
         */
        public JmsCachalotEntrails receiveFrom(Collection<String> queues) {
            validateState("receiveFrom");
            notNull(queues, "Receive queues must be specified");
            notEmpty(queues, "Receive queues must be specified");
            outQueues.addAll(queues);
            revealWomb("Out queues set {}", queues);
            return this;
        }

        /**
         * Append headers to jms message.
         * @param headers to append.
         * @return self.
         */
        public JmsCachalotEntrails withHeaders(Map<String, ?> headers) {
            validateState("withHeaders");
            notNull(headers, "Headers must be specified");
            this.headers.putAll(headers);
            revealWomb("Headers added {}", headers);
            return this;
        }

        /**
         * Append header to jms message.
         * @param header to append.
         * @param value to append.
         * @return self.
         */
        public JmsCachalotEntrails withHeader(String header, Object value) {
            validateState("withHeader");
            notNull(header, "Header name must be specified");
            notNull(value, "Header value must be specified");
            headers.put(header, value);
            revealWomb("Header added {}: {}", header, value);
            return this;
        }

        /**
         * Indicates in-only interaction. Test flow won't be waiting for response.
         * @return self.
         */
        public JmsCachalotEntrails withoutResponse() {
            validateState("withoutResponse");
            expectingResponse = false;
            //noinspection ConstantConditions
            revealWomb("Awaiting response set to {}", expectingResponse);
            return this;
        }

        /**
         * @param message to send. It could be any string text.
         * @return self.
         */
        public JmsCachalotEntrails withSpecifiedInput(String message) {
            validateState("withSpecifiedInput");
            notNull(message, "Input must be specified, if you call #withSpecifiedInput");
            this.inMessage = message;
            revealWomb("In message {}", message);
            return this;
        }

        /**
         * If provided, received messages will be compared with the body. If it won't be found, test will be considered
         * as failed.
         * @param message to compare.
         * @return self.
         */
        public JmsCachalotEntrails withExpectedResponse(String message) {
            validateState("withExpectedResponse");
            notNull(message, "Output must be specified, if you call #withExpectedResponse");
            outMessages.add(message);
            revealWomb("Out message {}", message);
            return this;
        }

        /**
         * Same as #withExpectedResponse(String message), but all messages will be compared with responses.
         * @return self.
         */
        public JmsCachalotEntrails withExpectedResponse(Collection<String> messages) {
            validateState("withExpectedResponse");
            notNull(messages, "Output must be specified, if you call #withExpectedResponse");
            notEmpty(messages, "Output must be specified, if you call #withExpectedResponse");
            outMessages.addAll(messages);
            revealWomb("Out messages {}", messages);
            return this;
        }

        /**
         * @param millis timeout for each message to be received.
         * @return self.
         */
        public JmsCachalotEntrails waitNotMoreThen(long millis) {
            validateState("waitNotMoreThen");
            timeout = millis;
            revealWomb("Timeout set to {} millis", millis);
            return this;
        }

        /**
         * Complete the subsystem (jms) configuration and returns to main config.
         * @return {@link CachalotEntrails} as main config.
         */
        public CachalotEntrails ingest() {
            if (jmsTemplate != null) {
                notNull(inQueue, "Send queue must be specified");
                if (expectingResponse) {
                    notNull(outQueues, "Receive queues must be specified");
                    notEmpty(outQueues, "Receive queues must be specified");
                    IntStream.range(0, outQueues.size()).forEach(i ->
                            receivers.add(new JmsTemplate(jmsTemplate.getConnectionFactory())));
                    revealWomb("Receivers added. Count: {}", receivers.size());
                }
            }
            return CachalotEntrails.this;
        }
    }

}
