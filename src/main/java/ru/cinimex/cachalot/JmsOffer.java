package ru.cinimex.cachalot;

import java.util.ArrayList;
import java.util.Collection;
import java.util.Objects;

import org.springframework.jms.core.JmsTemplate;

import lombok.AccessLevel;
import lombok.Getter;
import lombok.NonNull;

import static org.springframework.util.Assert.notEmpty;
import static org.springframework.util.Assert.notNull;

@SuppressWarnings({"unused", "MismatchedQueryAndUpdateOfCollection"})
@Getter(AccessLevel.PACKAGE)
public class JmsOffer extends Womb {

    private final long id;
    private final String queue;
    private final JmsTemplate template;
    private final JmsCachalotWomb parent;
    private final Collection<String> messages = new ArrayList<>();

    JmsOffer(JmsCachalotWomb parent, JmsTemplate template, @NonNull String queue, long id) {
        this.id = id;
        this.queue = queue;
        this.parent = parent;
        this.template = template;
        revealWomb("Out queue set {}", queue);
    }

    /**
     * @param message to send. It could be any string text.
     * @return self.
     */
    public JmsOffer withSpecifiedInput(String message) {
        validateState("withSpecifiedInput");
        notNull(message, "Input must be specified, if you call #withSpecifiedInput");
        messages.add(message);
        revealWomb("In message {}", message);
        return this;
    }

    /**
     * @param messages to send. It could be any collection containing messages as plain text.
     * @return self.
     */
    public JmsOffer withSpecifiedInput(Collection<String> messages) {
        validateState("withSpecifiedInput");
        notNull(messages, "Input must be specified, if you call #withSpecifiedInput");
        notEmpty(messages, "Input must be specified, if you call #withSpecifiedInput");
        this.messages.addAll(messages);
        revealWomb("In messages {}", messages);
        return this;
    }

    public JmsCachalotWomb ingest() {
        return parent;
    }

    @Override
    public boolean equals(Object other) {
        if (this == other) {
            return true;
        }
        if (other == null || getClass() != other.getClass()) {
            return false;
        }
        JmsOffer that = (JmsOffer) other;
        return id == that.id;
    }

    @Override
    public int hashCode() {
        return Objects.hash(id);
    }

    @SuppressWarnings("SameParameterValue")
    private void validateState(String callFrom) {
        notNull(template, "Illegal call #" + callFrom + " before CachalotWomb#usingJms");
    }

}
