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
public abstract class Cachalot extends Maw {

    private final List<Maw> maws = new ArrayList<>();
    private final Comparator<? super Maw> start = Comparator.comparing(Maw::getStartPriority).reversed();
    private final Comparator<? super Maw> end = Comparator.comparing(Maw::getEndPriority).reversed();

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
     * @return nested config as {@link JmsCachalotMaw}
     */
    public JmsCachalotMaw usingJms(final ConnectionFactory factory) {
        JmsCachalotMaw maw = new JmsCachalotMaw(this, factory, traceOn);
        maws.add(maw);
        return maw;
    }

    /**
     * Indicates, that your test use database for manipulating data before/after execution.
     *
     * @param dataSource is {@link DataSource}
     * @return nested config as {@link JdbcCachalotMaw}
     */
    public JdbcCachalotMaw usingJdbc(final DataSource dataSource) {
        JdbcCachalotMaw maw = new JdbcCachalotMaw(this, dataSource, traceOn);
        maws.add(maw);
        return maw;
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

        maws.sort(start);
        maws.forEach(this::before);

        maws.sort(end);
        maws.forEach(this::after);
    }

    private void before(Maw maw) {
        if (maw != null) {
            // set actual logging for that moment
            maw.setTraceOn(traceOn);
            try {
                maw.before();
            } catch (RuntimeException | AssertionError toThrowUp) {
                throw  toThrowUp;
            } catch (Exception e) {
                log.error("Unrecoverable Cachalot exception: ", e);
                throw new RuntimeException("Unrecoverable Cachalot exception", e);
            }
        }
    }

    private void after(Maw maw) {
        if (maw != null) {
            // set actual logging for that moment
            maw.setTraceOn(traceOn);
            try {
                maw.after();
            } catch (RuntimeException | AssertionError toThrowUp) {
                throw  toThrowUp;
            } catch (Exception e) {
                log.error("Unrecoverable Cachalot exception: ", e);
                throw new RuntimeException("Unrecoverable Cachalot exception", e);
            }
        }
    }

}
