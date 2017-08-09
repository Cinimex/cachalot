# Cachalot - a Java acceptance testing framework for complex backends
Cachalot is a testing framework created for backend-heavy spring-based Java applications.
Frequently, the only way to test complex server-side logic is via remote calls through JMS, or some other protocol. This is the situation Cachalot was designed for.

![cachalot](cachalot.png)                                                                                                                             
 
## When to use

When you have a complex logic, you want to verify via black-box tests.
 
 
## How to use 

 * Inherit and Override:  

```java
class MyAwesomeScenario extends ru.cinimex.cachalot.Cachalot {
    
    @Override
    protected void feed() throws Exception {
    }
}
```

 * Use dsl inside feed to describe your scenario (it's a bit like Camel):  

```java
class MyAwesomeScenario extends ru.cinimex.cachalot.Cachalot {
    
    @Override
    protected void feed() throws Exception {
        
        // all processing will be logged 
        enableDataTrace()
            .usingJms(factory)
            	// total timeout per queue during receiving (in ms)
                .waitNotMoreThen(7000)
                // all specified in/out queues will be cleared before test run
                .withRavage()
                // input queue (optional)
                .sendTo("INPUT_QUEUE")
                	// input text message (could be any text. Mandatory, if in queue specified)
                    .withSpecifiedInput("<Message></Message>")
                // complete sending    
                .ingest()
                // output queue (must present one|many out queues, if withoutResponse not called)
                .receiveFrom("OUTPUT_QUEUE")
                	// message to compare with output
                    .withExpectedResponse("<Message><Transformed/></Message>")
                    // validation rule, if equality check not satisfied
                    .addRule(s -> !s.isEmpty())
                // complete receiving        
                .ingest()
            // complete jms subsystem    
            .ingest()    
            .usingJdbc(dataSource)
            	// by default, jdbc #beforeFeed called first, before any other processing.
                // it takes a Supplier<String> to produce a valid jdbc statement. 
                // you can call #beforeFeed as many times as you need.
                .beforeFeed(() -> "TRUNCATE MY_TABLE CASCADE")
                // by default, jdbc #afterFeed called after all processing done, to validate state.
                // it takes a string RawMapper<T>, Predicate<T> and Supplier<String>
                .afterFeed(new JdbcValidationRule<>(myMapper, Objects::nonNull, "SELECT * FROM RESULT_TABLE"))
            .ingest();
    }
}
```
        
#### It's done. :+1:
 
Currently Cachalot supports 2 subsystems (Jms &#38; Jdbc).

Each subsystem have it's own lifecycle. If you need custom execution order you can specify:

```java
class MyAwesomeScenario extends ru.cinimex.cachalot.Cachalot {
    
    @Override
    protected void feed() throws Exception {
        usingJdbc(datasource)
            .withStartPriority(12) // priority of initial step
            .withEndPriority(99) // priority of completion step
            .beforeFeed(() -> "UPDATE XXX")
        .ingest();    
    }
}
```

Subsystem with highest priority will execute first.

 Where priority ∈ [0, 100], with 100 - is highest priority and 0 - lowest.
 
#### Dsl meaning:
 
##### Base

* enableDataTrace()  
    
        Local logs will contain all information about configuration and processing.
        The output can be quite complex.   

* usingJms(ConnectionFactory factory)

        Indicates, that your test use jms as underlying system. Method accepts 
        {@link javax.jms.ConnectionFactory} as input and opens different scope of 
        jms related api calls.

* usingJdbc(DataSource dataSource)  
       
        Indicates, that your test use database for manipulating data before/after or during execution.
        
##### Jms        
        
* withRavage()  
    
        All messages from specified in/out queues will be cleared before jms subsystem run.           
* sendTo(String inQueue)  
         
        Target queue to send message.
* withSpecifiedInput(String message)   
         
        Message to send. It could be any string text.
* receiveFrom(String outQueue)  
         
        Message queue to receive message from. This queue will be added to response queue collection.
        By default assumed, that each queue produce one message. I.e. if you want to receive multiple messages from one queue, you can call this method multiple times, or call #receiveFrom(Collection<String> outQueues).
        This method call is not idempotent: it's changing state of underlying infrastructure.
* withExpectedResponse(String message)
         
        If provided, received messages will be compared with given message. 
* withExpectedResponse(Collection&#60;String&#62; messages)
        
        Same as #withExpectedResponse(String message), but all messages will be compared with responses.
* waitNotMoreThen(long millis)  
        
        Timeout for each output queue to process.
* withoutResponse()  
        
        Indicates in-only interaction. Test flow won't be waiting for response.
* withHeader(String header, Object value)  
        
        Append header to jms message.
* withHeaders(Map&#60;String, ?&#62; headers)  
        
        Append headers to jms message.
* ingest()  

        Complete the subsystem (jms or jdbc or any...) configuration and returns to main config.
        
##### Jdbc

* beforeFeed(Supplier&#60;? extends String&#62; initializer)  
       
        Supplier will be used before test execution for initial state manipulating.
        It could be implemented as simple lambda: () -> "UPDATE MY_TABLE SET PROPERTY = 'AB' WHERE PROPERTY = 'BA'".
        This method is not idempotent, i.e. each call will add statement to execute.
* beforeFeed(Collection&#60;Supplier&#60;? extends String&#62;&#62; initializers)  
         
        Same as #beforeFeed(Supplier<? extends String> initializer), but for multiple statements.
* afterFeed(JdbcValidationRule&#60;?&#62; verificator)  

        Validate database state after test run.
        This method is not idempotent, i.e. each call will add a rule to validate.
        If rule validation fail, then test will be considered as failed. 
        Validation rule may looks like:

		new JdbcValidationRule<>((rs, rowNum) -> rs.getInt("count"),
                integer -> {
                    log.info("Expected {} / actual {}", expected.size(), integer);
                    return expected.size() == integer;
                },
                "SELECT count(*) FROM MY_TABLE WHERE id > 0 AND id < 100");
* afterFeed(Collection&#60;JdbcValidationRule&#60;?&#62;&#62; verificators)  
        
        Same as #afterFeed(JdbcValidationRule<?> verificator), but for multiple predicates at once.
          
#### Pull requests are always welcome :wink:
                 
