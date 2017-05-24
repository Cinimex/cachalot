 <h1>Cachalot</h1>
 
 Simple infrastructure for spring-based applications integration testing.
 
                                                                                                                                 
            #@     @,                                                                                                            
              @          @     .@/*@@                                                                                            
                @                   @                                                                                            
                  @(             .@                                                                                              
                    *@@      @@&                                                                                                 
                       .@       @(   &  @@                                                                                       
                        @                 @                                                                                      
                        @                  /@                                                                                    
                        @                    (@                                                                                  
                        @,                     ,@#                                                                               
                         @                        (@/                                                                            
                         @                           .@@,                                                                        
                         *,                               /@@#                                                                   
                          @                                   /@@                                                                
                          @                                       (@                                                             
                          .@                                          .@                                                         
                           @                                             %(*&                                                    
                            @                                                     .#@@@@@@@&.                                    
                             @                                                                /@@@@@&.                           
                              @                                                ,     *              @@@@@@@@                     
                               %                                              *        #               *     .@@                 
                                                                                              (#                 @&              
                                  ,                                                ,                           /,  @             
                                   @               (                           */               /,        /      *  @            
                                    *@@@%@  %         (@/,                                                .        / @           
                                 @             ,            @                                                      @ @           
                             @                  *        *(@                              .                     /    @           
                               *@*             @ %&         *                             (   .    #             * ( @           
                                           #@&@@@          &  %(                                          #          @           
                                                #@,               *             /         (   *  ./##&@@*  .    ,    @           
                                                   &@        . &                 ,      .      (  *        /  /  .  (%           
                                                      @&         @@&      ,                                         @            
                                                         @@        @@@%    . /// .,*(%&@., ,                 ,  ,  #             
                                                            @@@&     &@@@     (  ,(,     .  * ((///,.   ,  .  (   /,             
                                                                  ,@@@@@@@@@#      /  ,  ,                / ,  ( @               
                                                                                  %@@@@(      #.  *(  ,   ,     @                
                                                                                         @@@& %     .  (  *   @,                 
                                                                                             @@*  #,.#      @%                   
                                                                                                  &@@@@@@#                       
 
 <h3>Как кормить кашалота:</h3>
 
 1. Inherit:
```java
class MyAwesomeScenario extends CachalotEntrails {...}
```
 2. Override:
```java

@Override
protected void feed() throws Exception {...}

```
 3. Use dsl inside feed to describe your scenario (it's a bit like Camel):
```java
enableDataTrace()
    .usingJms(factory)
        .sendTo("INPUT_QUEUE")
        .withSpecifiedInput("<Message></Message>")
        .receiveFrom("OUTPUT_QUEUE")
        .withExpectedResponse("<Message><Transformed/></Message>")
        .waitNotMoreThen(7000)
    .ingest()
    .withState(dataSource)
        .beforeFeed(() -> "TRUNCATE MY_TABLE CASCADE")
        .afterFeed(rule)
    .ingest();
```
        
 
 4. It's done.
 
 <h4>Dsl meaning:</h4>
 
* enableDataTrace()  
    
        local logs will contain all information about configuration and processing.
        The output can be quite complex.   
* usingJms(ConnectionFactory factory)

        Indicates, that your test use jms as underlying system. Method accepts 
        {@link javax.jms.ConnectionFactory} as input and opens different scope of 
        jms related api calls.
* sendTo(String inQueue)  
         
        Target queue to send message.
* withSpecifiedInput(String message)   
         
        Message to send. It could be any string text.
* receiveFrom(String outQueue)  
         
        Message queue to receive message from. This queue will be added to response queue collection.
        By default assumed, that each queue produce one message. I.e. if you want to receive multiple messages
        from one queue, you can call this method multiple times, or call #receiveFrom(Collection<String> outQueues).
        This method call is not idempotent: it's changing state of underlying infrastructure.
* receiveFrom(Collection<String> outQueues)  

        Same as #receiveFrom(String outQueue), but for multiple queues at once.
* withExpectedResponse(String message)
         
        If provided, received messages will be compared with the body. If it won't be found, test will be considered
        as failed.
* withExpectedResponse(Collection<String> messages)
        
        Same as #withExpectedResponse(String message), but all messages will be compared with responses.
* waitNotMoreThen(long millis)  
        
        Timeout for each message to be received.
* withoutResponse()  
        
        Indicates in-only interaction. Test flow won't be waiting for response.
* withHeader(String header, Object value)  
        
        Append header to jms message.
* withHeaders(Map<String, ?> headers)  
        
        Append headers to jms message.
* ingest()  

        Complete the subsystem (jms or jdbc or any...) configuration and returns to main config.
* withState(DataSource dataSource)  
       
        Indicates, that your test use database for manipulating data before/after execution.
* beforeFeed(Supplier<? extends String> initializer)  
       
        Initializer will be used before test execution for initial state manipulating.
        It could be implemented as simple lambda: () -> "UPDATE MY_TABLE SET PROPERTY = 'AB' WHERE PROPERTY = 'BA'".
        This method is not idempotent, i.e. each call will add statement to execute.
* beforeFeed(Collection<Supplier<? extends String>> initializers)  
         
        Same as #beforeFeed(Supplier<? extends String> initializer), but for multiple statements.
* afterFeed(JdbcValidationRule<?> verificator)  

        Validate database state after test run.
        This method is not idempotent, i.e. each call will add a rule to validate.
        It rule validation fail, then test will be considered as failed.
* afterFeed(Collection<JdbcValidationRule<?>> verificators)  
        
        Same as #afterFeed(JdbcValidationRule<?> verificator), but for multiple rules at once.
          
          
