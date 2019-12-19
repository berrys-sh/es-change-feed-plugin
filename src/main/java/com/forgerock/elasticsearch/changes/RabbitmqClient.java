/*
 * To change this license header, choose License Headers in Project Properties.
 * To change this template file, choose Tools | Templates
 * and open the template in the editor.
 */
package com.forgerock.elasticsearch.changes;

import com.rabbitmq.client.*;
import java.io.IOException;
import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.TimeoutException;
import org.json.JSONObject;

/**
 *
 * @author berrys
 */
public class RabbitmqClient {

    private static final String TASK_QUEUE_NAME = "task_queue";
    String qExchangeName = TASK_QUEUE_NAME + "-exchange";
    String dlxQueueName = TASK_QUEUE_NAME + "-dlx";
    String dlxQExchangeName = dlxQueueName + "-exchange";
    ConnectionFactory factory = null;
    Connection connection = null;
    Channel channel = null;

    public RabbitmqClient() {
        this.factory = new ConnectionFactory();
        this.factory.setHost("rabbitmq");
    }

    private boolean reNewConnectionIfClose() throws IOException, TimeoutException {
        boolean isConnected = false;
        try {
            if (this.connection == null || !connection.isOpen()) {
                this.connection = factory.newConnection();
                if (this.connection.isOpen()) {
                    this.channel = this.connection.createChannel();
                    Map<String, Object> args = new HashMap<>();
                    args.put("x-dead-letter-exchange", dlxQExchangeName);
                    args.put("x-dead-letter-routing-key", "");
                    this.channel.exchangeDeclare(qExchangeName, "direct", true);
                    this.channel.queueDeclare(TASK_QUEUE_NAME, true, false, false, args);
                    this.channel.queueBind(TASK_QUEUE_NAME, qExchangeName, "", null);
                    isConnected =  true;
                }
            } else {
                isConnected = true;
            }
        } catch (Exception e) {
            if (this.channel != null && this.channel.isOpen()) {
                this.channel.close();
            }
            if (this.connection != null && this.connection.isOpen()) {
                this.connection.close();
            }

            this.channel = null;
            this.connection = null;
            System.out.println("Error in reNewConnectionIfClose :" +  e);
        } finally {
            return isConnected;
        }
    }

    public void enqueue(String message) throws IOException, TimeoutException {
        if (this.reNewConnectionIfClose()) {
        channel.basicPublish("", TASK_QUEUE_NAME,
                MessageProperties.PERSISTENT_TEXT_PLAIN,
                message.getBytes());
        System.out.println(" [x] Sent '" + message + "'");
        }
    }

}
