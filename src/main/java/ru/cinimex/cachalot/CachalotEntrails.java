package ru.cinimex.cachalot;

import java.util.ArrayList;
import java.util.Collection;
import java.util.Map;
import java.util.Optional;
import java.util.concurrent.CompletionService;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.concurrent.ExecutorCompletionService;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.function.Supplier;
import javax.jms.ConnectionFactory;
import javax.jms.Message;
import javax.jms.TextMessage;
import javax.sql.DataSource;

import org.junit.Assert;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.springframework.jdbc.core.JdbcTemplate;
import org.springframework.jms.core.JmsTemplate;
import org.springframework.scheduling.concurrent.CustomizableThreadFactory;
import org.springframework.test.context.junit4.SpringRunner;

import lombok.AccessLevel;
import lombok.NoArgsConstructor;
import lombok.NonNull;
import lombok.extern.slf4j.Slf4j;
import ru.cinimex.cachalot.validation.GenericValidationRule;
import ru.cinimex.cachalot.validation.JdbcValidationRule;
import ru.cinimex.cachalot.validation.ValidationRule;

import static org.hamcrest.collection.IsCollectionWithSize.hasSize;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.fail;
import static org.springframework.jms.support.destination.JmsDestinationAccessor.RECEIVE_TIMEOUT_NO_WAIT;
import static org.springframework.util.Assert.isTrue;
import static org.springframework.util.Assert.notEmpty;
import static org.springframework.util.Assert.notNull;

@Slf4j
@RunWith(SpringRunner.class)
@SuppressWarnings({"unused", "FieldCanBeLocal", "WeakerAccess"})
@NoArgsConstructor(access = AccessLevel.PROTECTED)
public abstract class CachalotEntrails {

    private JmsCachalotEntrails jmsCachalotEntrails;
    private JdbcCachalotEntrails jdbcCachalotEntrails;
    private CompletionService<JmsCachalotEntrails.JmsExpectation> cachalotTummy;
    private final Collection<JmsCachalotEntrails.JmsExpectation> digested = new ArrayList<>();
    private boolean traceOn;

    /**
     * Configure your test flow using {@link CachalotEntrails} dsl.
     *
     * @throws Exception in case you wanna say something.
     */
    protected abstract void feed() throws Exception;

    /**
     * Local logs will contain all information about configuration and processing.
     * The output can be quite complex.
     *
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
     *
     * @param factory is {@link ConnectionFactory}
     * @return nested config as {@link JmsCachalotEntrails}
     */
    public JmsCachalotEntrails usingJms(final ConnectionFactory factory) {
        jmsCachalotEntrails = new JmsCachalotEntrails(factory);
        return jmsCachalotEntrails;
    }

    /**
     * Indicates, that your test use database for manipulating data before/after execution.
     *
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

    /**
     * Main execution logic.
     *
     * @throws Exception if something wrong happens.
     */
    @Test
    public void deepSwim() throws Exception {
        revealWomb("Prepare to deep swim");
        feed();
        revealWomb("Cachalot feeded");

        // sync cause we don't need parallel here
        // it's jdbc preConditions run.
        Optional.ofNullable(jdbcCachalotEntrails).ifPresent(cachalotEntrails -> {
            for (Supplier<? extends String> supplier : cachalotEntrails.preConditions) {
                String query = supplier.get();
                revealWomb("Calling {}", query);
                cachalotEntrails.template.execute(query);
            }
        });

        // sync cause we don't need parallel here
        // it's jms preConditions run.
        Optional.ofNullable(jmsCachalotEntrails).ifPresent(cachalotEntrails -> {
            // check if client requested ravage
            if (cachalotEntrails.shouldRavage) {
                revealWomb("Queues ravage requested");
                // clear input queue first
                cachalotEntrails.template.setReceiveTimeout(RECEIVE_TIMEOUT_NO_WAIT);
                int inputQueueMessagesCount = 0;
                while (jmsCachalotEntrails.template.receive(cachalotEntrails.inQueue) != null) {
                    revealWomb("Cleared {} from input queue", ++inputQueueMessagesCount);
                }

                // clear all out queues
                Collection<JmsCachalotEntrails.JmsExpectation> expectations = jmsCachalotEntrails.expectations;
                for (JmsCachalotEntrails.JmsExpectation expectation : expectations) {
                    expectation.template.setReceiveTimeout(RECEIVE_TIMEOUT_NO_WAIT);
                    int outputQueueMessagesCount = 0;
                    while (expectation.template.receive(expectation.queue) != null) {
                        revealWomb("Cleared {} from {} queue", ++inputQueueMessagesCount, expectation.queue);
                    }
                }
                revealWomb("Queues ravaged");
            }
        });

        // sync cause we don't need parallel here
        // it's jms sending.
        Optional.ofNullable(jmsCachalotEntrails).ifPresent(cachalotEntrails -> {
            revealWomb("Prepare to send {} into {}", cachalotEntrails.inMessage, cachalotEntrails.inQueue);
            cachalotEntrails.template.send(cachalotEntrails.inQueue, session -> {
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

        final Collection<Future<JmsCachalotEntrails.JmsExpectation>> calls = new CopyOnWriteArrayList<>();

        // multithreaded jms response consuming.
        // cause we could receive more than one response for one request.
        Optional.ofNullable(jmsCachalotEntrails).ifPresent(cachalotEntrails -> {
            if (cachalotEntrails.expectingResponse) {
                long timeout = cachalotEntrails.timeout;

                CustomizableThreadFactory cachalotWatcher = new CustomizableThreadFactory("CachalotWatcher");
                ExecutorService executor = Executors.newCachedThreadPool(cachalotWatcher);

                try {
                    // possibility to uncontrolled growth.
                    cachalotTummy = new ExecutorCompletionService<>(executor);

                    Collection<JmsCachalotEntrails.JmsExpectation> expectations = cachalotEntrails.expectations;

                    expectations.forEach(expectation -> calls.add(cachalotTummy.submit(() -> {
                        revealWomb("Calling response from {} with timeout {} millis", expectation.queue, timeout);
                        expectation.template.setReceiveTimeout(timeout);
                        Message message = expectation.template.receive(expectation.queue);
                        //Avoid null check, but works only fro text message for now.
                        if (message instanceof TextMessage) {
                            revealWomb("Received text message");
                            expectation.actual = ((TextMessage) message).getText();
                            return expectation;
                        }
                        revealWomb("Received unknown type jms message");
                        return null;
                    })));

                } finally {
                    executor.shutdown();
                }

                Future<JmsCachalotEntrails.JmsExpectation> call;
                while (calls.size() > 0) {
                    try {
                        // block until a callable completes
                        call = cachalotTummy.take();
                        revealWomb("Received completed future: {}", call);
                        calls.remove(call);
                        // get expectation, if the Callable was able to create it.
                        JmsCachalotEntrails.JmsExpectation expectation = call.get();
                        if (expectation == null) {
                            fail("Message was not received in configured timeout: " + timeout + " millis");
                        }
                        revealWomb("Received message:\n{}", expectation.actual);
                        digested.add(expectation);
                    } catch (Exception e) {
                        Throwable cause = e.getCause();
                        log.error("Message receiving failed due to: " + cause, e);

                        for (Future<JmsCachalotEntrails.JmsExpectation> future : calls) {
                            // try to cancel all pending tasks.
                            future.cancel(true);
                        }
                        // fail
                        fail("Message receiving failed due to: " + cause);
                    }
                }
                for (JmsCachalotEntrails.JmsExpectation expectation : digested) {
                    if (expectation.expected != null) {
                        assertEquals("Expected and actual not match!", expectation.expected, expectation.actual);
                    }
                    expectation.validationRules.forEach(rule -> {
                        String error = "Test failed!\nMessage: " + expectation.actual + "\nrule: " + rule;
                        isTrue(rule.validate(expectation.actual), error);
                    });
                }
            }
        });

        // single threaded cause we don't need parallel here
        // jdbc post-conditions here
        Optional.ofNullable(jdbcCachalotEntrails).ifPresent(cachalotEntrails -> {
            for (JdbcValidationRule<?> validationRule : cachalotEntrails.postConditions) {
                long begin = System.currentTimeMillis();
                validationRule.setTemplate(cachalotEntrails.template);
                boolean validated = false;
                // so, validation rule must eventually completes with success or error.
                // eventually means that it could be async operations in tested system,
                // so we need to wait the time lag to make the correct check.
                do {
                    if (validationRule.validate(null)) {
                        validated = true;
                    }
                    // wait for a while.
                    try {
                        Thread.sleep(500);
                    } catch (InterruptedException ignored) {
                        ignored.printStackTrace();
                    }
                } while (!validated && begin + cachalotEntrails.timeout > System.currentTimeMillis());

                if (!validated) {
                    revealWomb("Validation rule violated {}", validationRule);
                    fail();
                } else {
                    revealWomb("Validation rule checked");
                }
            }
        });
    }

    @SuppressWarnings("MismatchedQueryAndUpdateOfCollection")
    protected final class JdbcCachalotEntrails {

        private final JdbcTemplate template;
        private final Collection<Supplier<? extends String>> preConditions = new ArrayList<>();
        private final Collection<JdbcValidationRule<?>> postConditions = new ArrayList<>();
        private long timeout = 0;

        private JdbcCachalotEntrails(final DataSource dataSource) {
            notNull(dataSource, "DataSource must be specified");
            template = new JdbcTemplate(dataSource, true);
            revealWomb("JdbcTemplate initialized with {}", dataSource);
        }

        /**
         * Query will be used before test execution for initial state manipulating.
         * It could be implemented as simple lambda: () -> "UPDATE MY_TABLE SET PROPERTY = 'AB' WHERE PROPERTY = 'BA'".
         * This method is not idempotent, i.e. each call will add statement to execute.
         *
         * @param query is statement supplier to process.
         * @return self.
         */
        public JdbcCachalotEntrails beforeFeed(Supplier<? extends String> query) {
            notNull(query, "Given query must not be null");
            preConditions.add(query);
            revealWomb("Query added {}", query);
            return this;
        }

        /**
         * Same as #beforeFeed(Supplier<? extends String> query), but for multiple statements.
         *
         * @param queries are statement suppliers to process.
         * @return self.
         */
        public JdbcCachalotEntrails beforeFeed(Collection<Supplier<? extends String>> queries) {
            notNull(queries, "Given queries must not be null");
            notEmpty(queries, "Given queries must not be null");
            preConditions.addAll(queries);
            revealWomb("Queries added {}", queries);
            return this;
        }

        /**
         * Validate database state after test run.
         * This method is not idempotent, i.e. each call will add a rule to validate.
         * If rule validation fail, then test will be considered as failed.
         *
         * @param rule is {@link JdbcValidationRule} to check.
         * @return self.
         */
        public JdbcCachalotEntrails afterFeed(JdbcValidationRule<?> rule) {
            notNull(rule, "Given rule must not be null");
            postConditions.add(rule);
            revealWomb("Rule added {}", rule);
            return this;
        }

        /**
         * Same as #afterFeed(JdbcValidationRule<?> rule), but for multiple rules at once.
         *
         * @param rules are {@link JdbcValidationRule} to check.
         * @return self.
         */
        public JdbcCachalotEntrails afterFeed(Collection<JdbcValidationRule<?>> rules) {
            notNull(rules, "Given rules must not be null");
            notEmpty(rules, "Given rules must not be null");
            postConditions.addAll(rules);
            revealWomb("Rules added {}", rules);
            return this;
        }

        /**
         * @param millis timeout for rule to be validated. (It's could be async processing)
         *               If {@link ValidationRule} returns false even after timeout,
         *               test intended to be failed.
         * @return self.
         */
        public JdbcCachalotEntrails waitNotMoreThen(long millis) {
            timeout = millis;
            revealWomb("Timeout set to {} millis", millis);
            return this;
        }

        /**
         * Complete the subsystem (jdbc) configuration and returns to main config.
         *
         * @return {@link CachalotEntrails} as main config.
         */
        public CachalotEntrails ingest() {
            return CachalotEntrails.this;
        }

    }

    @SuppressWarnings("MismatchedQueryAndUpdateOfCollection")
    protected final class JmsCachalotEntrails {

        private final JmsTemplate template;
        private final Map<String, ? super Object> headers = new ConcurrentHashMap<>();
        private String inQueue;
        private String inMessage;
        private boolean expectingResponse = true;
        private boolean shouldRavage = false;
        private long timeout = JmsTemplate.RECEIVE_TIMEOUT_INDEFINITE_WAIT;
        private final Collection<JmsExpectation> expectations = new CopyOnWriteArrayList<>();

        private JmsCachalotEntrails(final ConnectionFactory factory) {
            notNull(factory, "Provided connection factory must not be null");
            template = new JmsTemplate(factory);
            revealWomb("JmsTemplate initialized with {}", factory);
        }

        private void validateState(String callFrom) {
            notNull(template, "Illegal call #" + callFrom + " before CachalotEntrails#usingJms");
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
         *              By default assumed, that each queue produce one message. I.e. if you want to receive multiple
         *              messages
         *              from one queue, you can call this method multiple times.
         *              This method call is not idempotent: it's changing state of underlying infrastructure.
         * @return {@link JmsExpectation} instance.
         */
        public JmsExpectation receiveFrom(String queue) {
            validateState("receiveFrom");
            notNull(queue, "Receive queue must be specified");
            JmsExpectation expectation = new JmsExpectation(queue);
            expectations.add(expectation);
            return expectation;
        }

        /**
         * Append headers to jms message.
         *
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
         *
         * @param header to append.
         * @param value  to append.
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
         *
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
         * If specified, the {@code input} and all {@code output} queues will be cleared before
         * test run. Clearing will be performed as {@link JmsTemplate#receive} with
         * {@link JmsTemplate#RECEIVE_TIMEOUT_NO_WAIT} timeout.
         *
         * @return self.
         */
        public JmsCachalotEntrails withRavage() {
            shouldRavage = true;
            return this;
        }

        /**
         * Complete the subsystem (jms) configuration and returns to main config.
         *
         * @return {@link CachalotEntrails} as main config.
         */
        public CachalotEntrails ingest() {
            if (template != null) {
                notNull(inQueue, "Send queue must be specified");

                if (!expectations.isEmpty()) {
                    String error = "Jms destinations present, at the same time response is not expected";
                    isTrue(expectingResponse, error);
                }
                if (expectingResponse) {
                    notNull(expectations, "Receive queues must be specified");
                    notEmpty(expectations, "Receive queues must be specified");
                    revealWomb("Receivers added. Count: {}", expectations.size());
                } else {
                    Assert.assertThat("Response not expected, but jms response queue was provided", expectations,
                            hasSize(0));
                }
            }
            return CachalotEntrails.this;
        }

        public class JmsExpectation {

            @NonNull
            private String queue;
            private String expected;
            private String actual;
            private JmsTemplate template;
            private Collection<ValidationRule<? super String>> validationRules = new ArrayList<>();

            private JmsExpectation(String queue) {
                this.queue = queue;
                this.template = new JmsTemplate(JmsCachalotEntrails.this.template.getConnectionFactory());
                revealWomb("Out queue set {}", queue);
            }

            /**
             * @param rule is {@link ValidationRule} instance. It could be {@link GenericValidationRule} or custom
             *             implementation of the rule. If provided, received message will be validated against this
             *             rule. If it returns false, test will be considered as failed.
             *             Note: this operation, like many others, is not idempotent. I.e. it changes the state of
             *             underlying infrastructure. You can add multiple rules for one message by calling
             *             {@link #addRule(ValidationRule)} multiple times.
             * @return self.
             */
            @SuppressWarnings("JavaDoc")
            public JmsExpectation addRule(ValidationRule<? super String> rule) {
                notNull(rule, "Given rule must not be null");
                validationRules.add(rule);
                revealWomb("Rule added {}", rule);
                return this;
            }

            /**
             * If provided, received messages will be compared with the body. If it won't be found, test will be
             * considered as failed.
             *
             * @param message to compare.
             * @return self.
             */
            public JmsExpectation withExpectedResponse(String message) {
                validateState("withExpectedResponse");
                notNull(message, "Output must be specified, if you call #withExpectedResponse");
                this.expected = message;
                revealWomb("Out message {}", message);
                return this;
            }

            public JmsCachalotEntrails expect() {
                return JmsCachalotEntrails.this;
            }
        }
    }

}
