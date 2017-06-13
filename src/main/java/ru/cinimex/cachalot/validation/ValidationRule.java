package ru.cinimex.cachalot.validation;

/**
 * Basic validation rule's interface.
 * @param <T> is entity type to check.
 */
public interface ValidationRule<T> {
    boolean validate(T t);
}
