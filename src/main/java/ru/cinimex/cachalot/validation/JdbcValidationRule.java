package ru.cinimex.cachalot.validation;

import java.util.ArrayList;
import java.util.Collection;
import java.util.List;
import java.util.function.Predicate;

import org.springframework.jdbc.core.JdbcTemplate;
import org.springframework.jdbc.core.RowMapper;

import lombok.extern.slf4j.Slf4j;

import static org.springframework.util.Assert.notNull;

/**
 * Check to perform with database.
 * @param <T> is entity type to check.
 */
@Slf4j
@SuppressWarnings("unused")
public class JdbcValidationRule<T> implements ValidationRule<T>{

    private final JdbcTemplate template;
    private final RowMapper<T> mapper;
    private final String query;
    private final Collection<Predicate<T>> rules = new ArrayList<>();

    /**
     * Validation rule constructor.
     * @param template is a {@link JdbcTemplate} to use.
     * @param mapper is a {@link RowMapper} to transform entity.
     * @param rule is a {@link Predicate}, that contains validation logic.
     * @param query to execute for rule.
     */
    public JdbcValidationRule(JdbcTemplate template, RowMapper<T> mapper, Predicate<T> rule, String query) {
        notNull(template, "Template must not be null");
        notNull(mapper, "Mapper must not be null");
        notNull(rule, "Rule must not be null");
        notNull(query, "Query must not be null");
        this.template = template;
        this.mapper = mapper;
        this.query = query;
        this.rules.add(rule);
    }

    /**
     * Validation rule constructor.
     * @param template is a {@link JdbcTemplate} to use.
     * @param mapper is a {@link RowMapper} to transform entity.
     * @param rules are {@link Predicate}, that contains validation logic.
     * @param query to execute for rule.
     */
    public JdbcValidationRule(JdbcTemplate template, RowMapper<T> mapper, Collection<Predicate<T>> rules, String query) {
        notNull(template, "Template must not be null");
        notNull(mapper, "Mapper must not be null");
        notNull(rules, "Rule must not be null");
        notNull(query, "Query must not be null");
        this.template = template;
        this.mapper = mapper;
        this.query = query;
        this.rules.addAll(rules);
    }

    /**
     * Perform validation logic.
     * @return true if validation succeed, false otherwise.
     */
    @Override
    public boolean validate(T t) {
        List<T> items = template.query(this.query, mapper);
        log.debug("Returned items: {}", items);
        if (items.isEmpty()) {
            log.warn("No items found");
            return false;
        }
        for (T item : items) {
            log.debug("Validating {}", item);
            for (Predicate<T> rule : rules) {
                if (!rule.test(item)) {
                    log.error("Rule violation occurred: {}: {}", rule, item);
                    return false;
                }
            }
        }
        return true;
    }

    @Override
    public String toString() {
        final StringBuilder sb = new StringBuilder("CachalotJdbcValidationRule [\n");
        sb.append("  template = ").append(template).append("\n");
        sb.append("  mapper = ").append(mapper).append("\n");
        sb.append("  query = '").append(query).append('\n');
        sb.append("  rules = ").append(rules).append("\n");
        sb.append(']');
        return sb.toString();
    }
}
