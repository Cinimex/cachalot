package ru.cinimex.cachalot;

import lombok.AccessLevel;
import lombok.Getter;

// TODO Javadoc
abstract class Maw extends Traceable {

    // priority of lifecycle execution
    @Getter(AccessLevel.PACKAGE)
    int startPriority;
    @Getter(AccessLevel.PACKAGE)
    int endPriority;

    @SuppressWarnings("WeakerAccess")
    Maw(final int startPriority, final int endPriority) {
        this.startPriority = startPriority;
        this.endPriority = endPriority;
    }

    Maw() {
        // lowest priority
        this(0, 0);
    }

    void before() throws Exception {
        // noop
    }

    void after() throws Exception {
        // noop
    }

    @SuppressWarnings("UnusedReturnValue")
    Maw withStartPriority(int priority) {
        if (priority < 0 || priority > 100) {
            throw new IllegalArgumentException("Priority should be in range 0-100");
        }
        this.startPriority = priority;
        return this;
    }

    @SuppressWarnings("UnusedReturnValue")
    Maw withEndPriority(int priority) {
        if (priority < 0 || priority > 100) {
            throw new IllegalArgumentException("Priority should be in range 0-100");
        }
        this.endPriority = priority;
        return this;
    }
}
