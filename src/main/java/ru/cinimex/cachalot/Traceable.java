package ru.cinimex.cachalot;

import lombok.AccessLevel;
import lombok.Setter;
import lombok.extern.slf4j.Slf4j;

@Slf4j
abstract class Traceable {

    @Setter(AccessLevel.PACKAGE)
    boolean traceOn;

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


}
