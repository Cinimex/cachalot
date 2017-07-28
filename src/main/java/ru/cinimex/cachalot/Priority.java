package ru.cinimex.cachalot;

@SuppressWarnings("unused")
// TODO Javadoc
public final class Priority {

    // no instances pls
    private Priority() {}

    public static final int PRIORITY_HIGH = 100;
    public static final int PRIORITY_MEDIUM = 50;
    public static final int PRIORITY_LOW = 0;

    static final int JDBC_DEFAULT_PRIORITY_START = 90;
    static final int JDBC_DEFAULT_PRIORITY_END = 10;
    static final int JMS_DEFAULT_PRIORITY_START = 70;
    static final int JMS_DEFAULT_PRIORITY_END = 30;

}
