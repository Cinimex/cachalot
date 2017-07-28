package ru.cinimex.cachalot.validation;

import java.util.ArrayList;
import java.util.Collection;
import java.util.List;
import java.util.function.Predicate;

import org.springframework.jdbc.core.JdbcTemplate;
import org.springframework.jdbc.core.RowMapper;

import lombok.Setter;
import lombok.extern.slf4j.Slf4j;

import static org.springframework.util.Assert.notNull;

/**
 * Check to perform with database.
 *
 * @param <T> is entity type to check.
 */
@Slf4j
@SuppressWarnings("unused")
public class JdbcValidationRule<T> implements Predicate<T> {

    @Setter
    private JdbcTemplate template;
    private final RowMapper<T> mapper;
    private final String query;
    private final Collection<Predicate<T>> predicates = new ArrayList<>();

    /**
     * Validation rule constructor.
     *
     * @param mapper    is a {@link RowMapper} to transform entity.
     * @param predicate is a {@link Predicate}, that contains validation logic.
     * @param query     to execute for rule.
     */
    public JdbcValidationRule(RowMapper<T> mapper, Predicate<T> predicate, String query) {
        notNull(mapper, "Mapper must not be null");
        notNull(predicate, "Rule must not be null");
        notNull(query, "Query must not be null");
        this.mapper = mapper;
        this.query = query;
        this.predicates.add(predicate);
    }

    /**
     * Validation rule constructor.
     *
     * @param mapper     is a {@link RowMapper} to transform entity.
     * @param predicates are {@link Predicate}, that contains validation logic.
     * @param query      to execute for rule.
     */
    public JdbcValidationRule(RowMapper<T> mapper, Collection<Predicate<T>> predicates, String query) {
        notNull(mapper, "Mapper must not be null");
        notNull(predicates, "Rule must not be null");
        notNull(query, "Query must not be null");
        this.mapper = mapper;
        this.query = query;
        this.predicates.addAll(predicates);
    }

    /**
     * Perform validation logic.
     *
     * @return true if validation succeed, false otherwise.
     */
    @Override
    public boolean test(T noopItem) {
        notNull(template, "Template must not be null");
        List<T> items = template.query(query, mapper);
        if (items.isEmpty()) {
            log.debug("Query return no items, can't process any check");
            return false;
        }
        for (T item : items) {
            for (Predicate<T> predicate : predicates) {
                if (!predicate.test(item)) {
                    log.debug("Validating {}", item);
                    log.error("Rule violation for predicate: {} and query: {}", predicate, query);
                    return false;
                }
            }
        }
        return true;
    }

    @Override
    public String toString() {
        final StringBuilder sb = new StringBuilder("CachalotJdbcValidationRule [\n");
        sb.append("  mapper = ").append(mapper).append("\n");
        sb.append("  query = '").append(query).append('\n');
        sb.append("  predicates = ").append(predicates).append("\n");
        sb.append(']');
        return sb.toString();
    }
}
