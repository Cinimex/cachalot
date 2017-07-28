package ru.cinimex.cachalot;

import java.util.ArrayList;
import java.util.Comparator;
import java.util.List;
import javax.jms.ConnectionFactory;
import javax.sql.DataSource;

import org.junit.Test;
import org.junit.runner.RunWith;
import org.springframework.test.context.junit4.SpringRunner;

import lombok.AccessLevel;
import lombok.NoArgsConstructor;
import lombok.extern.slf4j.Slf4j;

@Slf4j
@RunWith(SpringRunner.class)
@SuppressWarnings({"unused", "WeakerAccess"})
@NoArgsConstructor(access = AccessLevel.PROTECTED)
public abstract class Cachalot extends Womb {

    private final List<Womb> wombs = new ArrayList<>();
    private final Comparator<? super Womb> start = Comparator.comparing(Womb::getStartPriority).reversed();
    private final Comparator<? super Womb> end = Comparator.comparing(Womb::getEndPriority).reversed();

    /**
     * Configure your test flow using {@link Cachalot} dsl.
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
    public Cachalot enableDataTrace() {
        traceOn = true;
        return this;
    }

    /**
     * Indicates, that your test use jms as underlying system. Method accepts
     * {@link javax.jms.ConnectionFactory} as input and opens different scope of
     * jms related api calls.
     *
     * @param factory is {@link ConnectionFactory}
     * @return nested config as {@link JmsCachalotWomb}
     */
    public JmsCachalotWomb usingJms(final ConnectionFactory factory) {
        JmsCachalotWomb womb = new JmsCachalotWomb(this, factory, traceOn);
        wombs.add(womb);
        return womb;
    }

    /**
     * Indicates, that your test use database for manipulating data before/after execution.
     *
     * @param dataSource is {@link DataSource}
     * @return nested config as {@link JdbcCachalotWomb}
     */
    public JdbcCachalotWomb usingJdbc(final DataSource dataSource) {
        JdbcCachalotWomb womb = new JdbcCachalotWomb(this, dataSource, traceOn);
        wombs.add(womb);
        return womb;
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

        wombs.sort(start);
        wombs.forEach(this::before);

        wombs.sort(end);
        wombs.forEach(this::after);
    }

    private void before(Womb womb) {
        if (womb != null) {
            // set actual logging for that moment
            womb.setTraceOn(traceOn);
            try {
                womb.before();
            } catch (RuntimeException | AssertionError toThrowUp) {
                throw  toThrowUp;
            } catch (Exception e) {
                log.error("Unrecoverable Cachalot exception: ", e);
                throw new RuntimeException("Unrecoverable Cachalot exception", e);
            }
        }
    }

    private void after(Womb womb) {
        if (womb != null) {
            // set actual logging for that moment
            womb.setTraceOn(traceOn);
            try {
                womb.after();
            } catch (RuntimeException | AssertionError toThrowUp) {
                throw  toThrowUp;
            } catch (Exception e) {
                log.error("Unrecoverable Cachalot exception: ", e);
                throw new RuntimeException("Unrecoverable Cachalot exception", e);
            }
        }
    }

}
