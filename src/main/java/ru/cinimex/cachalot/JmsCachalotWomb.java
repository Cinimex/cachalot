package ru.cinimex.cachalot;

import java.util.ArrayList;
import java.util.Collection;
import java.util.Comparator;
import java.util.List;
import java.util.Map;
import java.util.concurrent.CompletionService;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.concurrent.ExecutorCompletionService;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.atomic.AtomicLong;
import javax.jms.ConnectionFactory;
import javax.jms.JMSException;
import javax.jms.Message;
import javax.jms.TextMessage;

import org.junit.Assert;
import org.springframework.jms.core.JmsTemplate;
import org.springframework.scheduling.concurrent.CustomizableThreadFactory;

import lombok.AccessLevel;
import lombok.Getter;

import static java.util.stream.Collectors.groupingBy;
import static org.hamcrest.collection.IsCollectionWithSize.hasSize;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.fail;
import static org.springframework.jms.support.destination.JmsDestinationAccessor.RECEIVE_TIMEOUT_NO_WAIT;
import static org.springframework.util.Assert.isTrue;
import static org.springframework.util.Assert.notEmpty;
import static org.springframework.util.Assert.notNull;
import static ru.cinimex.cachalot.Priority.*;

@Getter(AccessLevel.PACKAGE)
@SuppressWarnings({"unused"})
public class JmsCachalotWomb extends Womb {

    private final Cachalot parent;
    private final JmsTemplate template;
    private final AtomicLong index = new AtomicLong();
    private final AtomicLong offersIndex = new AtomicLong();
    private final Map<String, ? super Object> headers = new ConcurrentHashMap<>();
    private final Collection<JmsExpectation> digested = new ArrayList<>();
    private final Collection<JmsExpectation> expectations = new CopyOnWriteArrayList<>();
    private final Collection<JmsOffer> offers = new CopyOnWriteArrayList<>();

    private boolean shouldRavage = false;
    private boolean expectingResponse = true;
    private long timeout = JmsTemplate.RECEIVE_TIMEOUT_INDEFINITE_WAIT;
    private CompletionService<Collection<JmsExpectation>> cachalotTummy;

    JmsCachalotWomb(final Cachalot parent, final ConnectionFactory factory, final boolean traceOn) {
        super(JMS_DEFAULT_PRIORITY_START, JMS_DEFAULT_PRIORITY_END);
        notNull(factory, "Provided connection factory must not be null");
        this.parent = parent;
        this.traceOn = traceOn;
        this.template = new JmsTemplate(factory);
        revealWomb("JmsTemplate initialized with {}", factory);
    }

    private void validateState(String callFrom) {
        notNull(template, "Illegal call #" + callFrom + " before CachalotWomb#usingJms");
    }

    /**
     * @param queue target queue to send message.
     * @return self.
     */
    public JmsOffer sendTo(String queue) {
        validateState("sendTo");
        notNull(queue, "Send queue must be specified");
        JmsOffer offer = new JmsOffer(this, template, queue, offersIndex.getAndIncrement());
        offers.add(offer);
        revealWomb("In queue set {}", queue);
        return offer;
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
        long index = this.index.getAndIncrement();
        JmsExpectation expectation = new JmsExpectation(this, template.getConnectionFactory(), queue, index);
        expectations.add(expectation);
        return expectation;
    }

    /**
     * Append headers to jms message.
     *
     * @param headers to append.
     * @return self.
     */
    public JmsCachalotWomb withHeaders(Map<String, ?> headers) {
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
    public JmsCachalotWomb withHeader(String header, Object value) {
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
    public JmsCachalotWomb withoutResponse() {
        validateState("withoutResponse");
        expectingResponse = false;
        //noinspection ConstantConditions
        revealWomb("Awaiting response set to {}", expectingResponse);
        return this;
    }

    /**
     * @param millis timeout for each message to be received.
     * @return self.
     */
    public JmsCachalotWomb waitNotMoreThen(long millis) {
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
    public JmsCachalotWomb withRavage() {
        shouldRavage = true;
        return this;
    }

    @Override
    public JmsCachalotWomb withStartPriority(int priority) {
        super.withStartPriority(priority);
        return this;
    }

    @Override
    public JmsCachalotWomb withEndPriority(int priority) {
        super.withEndPriority(priority);
        return this;
    }

    /**
     * Complete the subsystem (jms) configuration and returns to main config.
     *
     * @return {@link Cachalot} as main config.
     */
    public Cachalot ingest() {
        if (template != null) {
            notEmpty(offers, "Send queue must be specified");

            if (!expectations.isEmpty()) {
                String error = "Jms destinations present, at the same time response is not expected";
                isTrue(expectingResponse, error);
            }
            if (expectingResponse) {
                notNull(expectations, "Receive queues must be specified");
                notEmpty(expectations, "Receive queues must be specified");
                revealWomb("Receivers added. Count: {}", expectations.size());
            } else {
                Assert.assertThat("Response not expected, but jms response queue was provided", expectations, hasSize(0));
            }
        }
        return parent;
    }

    @Override
    void before() throws Exception {
        // sync cause we don't need parallel here
        // it's jms preconditions run.
        // check if client requested ravage
        if (shouldRavage) {
            revealWomb("Queues ravage requested");
            // clear input queues first
            template.setReceiveTimeout(RECEIVE_TIMEOUT_NO_WAIT);
            int inputQueueMessagesCount = 0;
            for (JmsOffer offer : offers) {
                int outputQueueMessagesCount = 0;
                while (offer.getTemplate().receive(offer.getQueue()) != null) {
                    revealWomb("Cleared {} message(s) from {} input queue", ++inputQueueMessagesCount, offer.getQueue());
                }
            }

            // clear all out queues
            for (JmsExpectation expectation : expectations) {
                expectation.getTemplate().setReceiveTimeout(RECEIVE_TIMEOUT_NO_WAIT);
                int outputQueueMessagesCount = 0;
                while (expectation.getTemplate().receive(expectation.getQueue()) != null) {
                    revealWomb("Cleared {} message(s) from {} queue", ++outputQueueMessagesCount, expectation.getQueue());
                }
            }
            revealWomb("Queues ravaged");
        }

        offers.forEach(offer -> {
            // it's jms sending.
            revealWomb("Prepare to send {} into {}", offer.getMessages(), offer.getQueue());
            offer.getMessages().forEach(inMessage -> {
                offer.getTemplate().send(offer.getQueue(), session -> {
                    TextMessage message = session.createTextMessage();
                    if (inMessage != null) {
                        message.setText(inMessage);
                    }
                    for (Map.Entry<String, ? super Object> header : headers.entrySet()) {
                        message.setObjectProperty(header.getKey(), header.getValue());
                    }
                    revealWomb("Message {} created", message);
                    return message;
                });
                revealWomb("Message successfully sent into {}", offer.getQueue());
            });

        });
    }

    @Override
    void after() throws Exception {
        // multithreaded jms response consuming.
        // cause we could receive more than one response for one request.
        if (!expectingResponse) {
            // no need for processing
            return;
        }

        final Collection<Future<Collection<JmsExpectation>>> calls = new CopyOnWriteArrayList<>();
        final CustomizableThreadFactory cachalotWatcher = new CustomizableThreadFactory("CachalotWatcher");
        final ExecutorService executor = Executors.newCachedThreadPool(cachalotWatcher);

        try {
            // possibility to uncontrolled growth.
            cachalotTummy = new ExecutorCompletionService<>(executor);

            // first of all group expectations by queue name.
            // We need to receive messages sequentially from the same queue.
            // Multithreaded only for different queues.
            Map<String, List<JmsExpectation>> byQueue = expectations.stream().collect(groupingBy(JmsExpectation::getQueue));
            revealWomb("Prepared queues: {}", byQueue.keySet());

            byQueue.forEach((key, expectationsByQueue) -> {
                revealWomb("Processing {} queue", key);
                calls.add(cachalotTummy.submit(() -> {
                    expectationsByQueue.stream().sorted(Comparator.comparing(JmsExpectation::getId)).forEach(expectation -> {

                        revealWomb("Calling response from {} with timeout {} millis", expectation.getQueue(), timeout);
                        expectation.getTemplate().setReceiveTimeout(timeout);
                        Message message = expectation.getTemplate().receive(key);
                        //Avoid null check, but works only fro text message for now.
                        if (message instanceof TextMessage) {
                            revealWomb("Received text message");
                            try {
                                expectation.setActual(((TextMessage) message).getText());
                            } catch (JMSException e) {
                                e.printStackTrace();
                                fail("Unexpected exception during message processing: " + e);
                            }
                        } else {
                            revealWomb("Received unknown type jms message {}", message);
                        }
                    });
                    return expectationsByQueue;
                }));
            });

        } finally {
            executor.shutdown();
        }

        Future<Collection<JmsExpectation>> call;
        while (!calls.isEmpty()) {
            try {
                // retrieve next completed future
                call = cachalotTummy.take();
                revealWomb("Received completed future: {}", call);
                calls.remove(call);
                // get expectation, if the Callable was able to create it.
                Collection<JmsExpectation> expectations = call.get();
                if (expectations == null || expectations.isEmpty()) {
                    fail("Message was not received in configured timeout: " + timeout + " millis");
                }
                expectations.forEach(expectation -> {
                    revealWomb("Received message:\n{}", expectation.getActual());
                    digested.add(expectation);
                });
            } catch (Exception e) {
                Throwable cause = e.getCause();
                revealWomb("Message receiving failed due to: " + cause, e);

                for (Future<Collection<JmsExpectation>> future : calls) {
                    // try to cancel all pending tasks.
                    future.cancel(true);
                }
                // fail
                fail("Message receiving failed due to: " + cause);
            }
        }
        for (JmsExpectation expectation : digested) {
            if (expectation.getExpected() != null) {
                assertEquals("Expected and actual not match!", expectation.getExpected(), expectation.getActual());
            }
            expectation.getRules().forEach(rule -> {
                String error = "Test failed!\nMessage: " + expectation.getActual() + "\nrule: " + rule;
                isTrue(rule.validate(expectation.getActual()), error);
            });
        }
    }

}

