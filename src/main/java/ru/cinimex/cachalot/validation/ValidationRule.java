package ru.cinimex.cachalot.validation;

/**
 * Basic validation rule interface.
 *
 * @param <T> is entity type to check.
 */
public interface ValidationRule<T> {

    boolean validate(T item);
}
