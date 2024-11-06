package com.hmdp.config;

import org.springframework.amqp.core.*;
import org.springframework.beans.factory.annotation.Qualifier;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;

import java.util.HashMap;

@Configuration
public class TTLQueueConfig {

    // 普通交换机名称
    public static final String X_EXCHANGE = "X";
    // 死信交换机名称
    public static final String Y_DEAD_LETTER_EXCHANGE = "Y";
    // 普通队列名称
    public static final String QUEUE_A = "QA";
    // 死信队列名称
    public static final String DEAD_LETTER_QUEUE = "QD";

    @Bean("xExchange")
    public DirectExchange xExchange(){
        return new DirectExchange(X_EXCHANGE);
    }

    @Bean("yExchange")
    public DirectExchange yExchange(){
        return new DirectExchange(Y_DEAD_LETTER_EXCHANGE);
    }

    @Bean("queueA")
    public Queue queueA(){
        final HashMap<String,Object> arguments = new HashMap<>();
        arguments.put("x-dead-letter-exchange", Y_DEAD_LETTER_EXCHANGE);
        arguments.put("x-dead-letter-routing-key", "YD");
        arguments.put("x-message-ttl", 60000);
        return QueueBuilder.durable(QUEUE_A).withArguments(arguments).build();
    }

    @Bean("queueD")
    public Queue queueD(){
        return QueueBuilder.durable(DEAD_LETTER_QUEUE).build();
    }

    @Bean
    public Binding queueABindingX(@Qualifier("queueA")Queue queueA, @Qualifier("xExchange") DirectExchange xExchange){
        return BindingBuilder.bind(queueA).to(xExchange).with("XA");
    }

    @Bean
    public Binding queueDBindingY(@Qualifier("queueD")Queue queueD, @Qualifier("yExchange") DirectExchange yExchange){
        return BindingBuilder.bind(queueD).to(yExchange).with("YD");
    }

}
