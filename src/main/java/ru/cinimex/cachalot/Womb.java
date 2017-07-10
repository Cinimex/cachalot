package ru.cinimex.cachalot;

import lombok.AccessLevel;
import lombok.Getter;
import lombok.Setter;
import lombok.extern.slf4j.Slf4j;

@Slf4j
abstract class Womb {

    @Setter(AccessLevel.PACKAGE)
    boolean traceOn;
    // priority of lifecycle execution
    @Getter(AccessLevel.PACKAGE)
    int startPriority;
    @Getter(AccessLevel.PACKAGE)
    int endPriority;

    @SuppressWarnings("WeakerAccess")
    Womb(final int startPriority, final int endPriority) {
        this.startPriority = startPriority;
        this.endPriority = endPriority;
    }

    Womb() {
        // lowest priority
        this(0, 0);
    }

    void revealWomb(String say, Object... what) {
        if (traceOn) {
            log.info(say, what);
        }
    }

    @SuppressWarnings("unused")
    void revealWomb(boolean traceOn, String say, Object... what) {
        this.traceOn = traceOn;
        revealWomb(say, what);
    }

    void before() throws Exception {
        // noop
    }

    void after() throws Exception {
        // noop
    }

    @SuppressWarnings("UnusedReturnValue")
    Womb withStartPriority(int priority) {
        if (priority < 0 || priority > 100) {
            throw new IllegalArgumentException("Priority should be in range 0-100");
        }
        this.startPriority = priority;
        return this;
    }

    @SuppressWarnings("UnusedReturnValue")
    Womb withEndPriority(int priority) {
        if (priority < 0 || priority > 100) {
            throw new IllegalArgumentException("Priority should be in range 0-100");
        }
        this.endPriority = priority;
        return this;
    }
}
