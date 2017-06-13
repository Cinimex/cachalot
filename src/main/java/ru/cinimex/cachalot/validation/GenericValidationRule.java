package ru.cinimex.cachalot.validation;

import lombok.extern.slf4j.Slf4j;

import java.util.ArrayList;
import java.util.Collection;
import java.util.function.Predicate;

/**
 * Free condition validation rule.
 *
 * @param <T> is entity type to check.
 */
@Slf4j
public class GenericValidationRule<T> implements ValidationRule<T> {

    private final Collection<Predicate<T>> rules = new ArrayList<>();

    /**
     * @param rule is a {@link Predicate}, that contains validation logic.
     */
    public GenericValidationRule(Predicate<T> rule) {
        this.rules.add(rule);
    }

    /**
     * @param rules are {@link Predicate}, that contains validation logic.
     */
    public GenericValidationRule(Collection<Predicate<T>> rules) {
        this.rules.addAll(rules);
    }

    /**
     * Perform validation logic.
     * @return true if validation succeed, false otherwise.
     */
    @Override
    public boolean validate(T t) {
        for (Predicate<T> rule : rules) {
            if (!(rule.test(t))) {
                return false;
            }
        }
        return true;
    }
}
