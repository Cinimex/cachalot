package ru.cinimex.cachalot;

import java.util.ArrayList;
import java.util.Collection;
import java.util.function.Supplier;
import javax.sql.DataSource;

import org.springframework.jdbc.core.JdbcTemplate;

import lombok.extern.slf4j.Slf4j;
import ru.cinimex.cachalot.validation.JdbcValidationRule;

import static org.junit.Assert.fail;
import static org.springframework.util.Assert.notEmpty;
import static org.springframework.util.Assert.notNull;
import static ru.cinimex.cachalot.Priority.*;

@Slf4j
@SuppressWarnings({"unused"})
public class JdbcCachalotWomb extends Womb {

    private final JdbcTemplate template;
    private final Cachalot parent;
    private final Collection<Supplier<? extends String>> preConditions = new ArrayList<>();
    private final Collection<JdbcValidationRule<?>> postConditions = new ArrayList<>();
    private long timeout = 0;

    JdbcCachalotWomb(final Cachalot parent, final DataSource dataSource, final boolean traceOn) {
        // expecting to execute before - with high priority, end - with low
        super(JDBC_DEFAULT_PRIORITY_START, JDBC_DEFAULT_PRIORITY_END);
        notNull(dataSource, "DataSource must be specified");
        this.parent = parent;
        this.traceOn = traceOn;
        this.template = new JdbcTemplate(dataSource, true);
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
    public JdbcCachalotWomb beforeFeed(Supplier<? extends String> query) {
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
    public JdbcCachalotWomb beforeFeed(Collection<Supplier<? extends String>> queries) {
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
    public JdbcCachalotWomb afterFeed(JdbcValidationRule<?> rule) {
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
    public JdbcCachalotWomb afterFeed(Collection<JdbcValidationRule<?>> rules) {
        notNull(rules, "Given rules must not be null");
        notEmpty(rules, "Given rules must not be null");
        postConditions.addAll(rules);
        revealWomb("Rules added {}", rules);
        return this;
    }

    /**
     * @param millis timeout for rule to be validated. (It's could be async processing)
     *               If {@link java.util.function.Predicate} returns false even after timeout,
     *               test intended to be failed.
     * @return self.
     */
    public JdbcCachalotWomb waitNotMoreThen(long millis) {
        timeout = millis;
        revealWomb("Timeout set to {} millis", millis);
        return this;
    }

    @Override
    public JdbcCachalotWomb withStartPriority(int priority) {
        super.withStartPriority(priority);
        return this;
    }

    @Override
    public JdbcCachalotWomb withEndPriority(int priority) {
        super.withEndPriority(priority);
        return this;
    }

    /**
     * Complete the subsystem (jdbc) configuration and returns to main config.
     *
     * @return {@link Cachalot} as main config.
     */
    public Cachalot ingest() {
        return parent;
    }

    @Override
    void before() throws Exception {
        // sync cause we don't need parallel here
        // it's jdbc preconditions run.
        for (Supplier<? extends String> supplier : preConditions) {
            String query = supplier.get();
            revealWomb("Calling {}", query);
            template.execute(query);
        }
    }

    @Override
    void after() throws Exception {
        // single threaded cause we don't need parallel here
        // jdbc post-conditions here
        for (JdbcValidationRule<?> validationRule : postConditions) {
            long begin = System.currentTimeMillis();
            validationRule.setTemplate(template);
            boolean validated = false;
            // so, validation rule must eventually completes with success or error.
            // eventually means that it could be async operations in tested system,
            // so we need to wait the time lag to make the correct check.
            do {
                if (validationRule.test(null)) {
                    validated = true;
                }
                // wait for a while.
                try {
                    Thread.sleep(500);
                } catch (InterruptedException ignored) {
                    ignored.printStackTrace();
                }
            } while (!validated && begin + timeout > System.currentTimeMillis());

            if (!validated) {
                revealWomb("Validation rule violated {}", validationRule);
                fail();
            } else {
                revealWomb("Validation rule checked");
            }
        }
    }
}
