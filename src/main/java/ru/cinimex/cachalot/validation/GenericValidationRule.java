package ru.cinimex.cachalot.validation;

import lombok.extern.slf4j.Slf4j;

import java.util.ArrayList;
import java.util.Collection;
import java.util.function.Predicate;

import org.springframework.util.Assert;

/**
 * Free condition validation rule.
 *
 * @param <T> is entity type to check.
 */
@Slf4j
@SuppressWarnings("unused")
public class GenericValidationRule<T> implements ValidationRule<T> {

    private final Collection<Predicate<T>> predicates = new ArrayList<>();

    /**
     * @param predicate is a {@link Predicate}, that contains validation logic.
     */
    public GenericValidationRule(Predicate<T> predicate) {
        Assert.notNull(predicate, "Provided predicate must not be null!");
        this.predicates.add(predicate);
    }

    /**
     * @param predicates are {@link Predicate}, that contains validation logic.
     */
    public GenericValidationRule(Collection<Predicate<T>> predicates) {
        Assert.notNull(predicates, "Provided predicates must not be null!");
        Assert.notEmpty(predicates, "Provided predicates must not be empty!");
        this.predicates.addAll(predicates);
    }

    /**
     * Perform validation logic.
     * @return true if validation succeed, false otherwise.
     */
    @Override
    public boolean validate(T item) {
        for (Predicate<T> predicate : predicates) {
            if (!(predicate.test(item))) {
                log.error("Rule violation occurred: {}: {}", predicate, item);
                return false;
            }
        }
        return true;
    }
}
