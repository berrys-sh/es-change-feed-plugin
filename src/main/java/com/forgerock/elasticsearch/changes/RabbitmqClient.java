/*
 * To change this license header, choose License Headers in Project Properties.
 * To change this template file, choose Tools | Templates
 * and open the template in the editor.
 */
package com.forgerock.elasticsearch.changes;

import com.rabbitmq.client.*;
import java.io.IOException;
import java.net.SocketPermission;
import java.security.PrivilegedAction;
import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.TimeoutException;
import java.util.logging.Level;
import javax.management.MBeanPermission;
import javax.management.MBeanServerPermission;
import javax.management.MBeanTrustPermission;
import org.apache.logging.log4j.Logger;
import org.elasticsearch.common.SuppressForbidden;
import org.elasticsearch.common.logging.Loggers;
import org.json.JSONObject;

/**
 *
 * @author berrys
 */
public class RabbitmqClient {

    private final static ConfigurationManager CONFIG = ConfigurationManager.getInstance();
    private static final String TASK_QUEUE_NAME = CONFIG.getRabbitmqQueueName();
    String qExchangeName = TASK_QUEUE_NAME + "-exchange";
    String dlxQueueName = TASK_QUEUE_NAME + "_" + CONFIG.getRabbitmqQueuedlxTTL() + "-dlx";
    String dlxQExchangeName = dlxQueueName + "_" + CONFIG.getRabbitmqQueuedlxTTL() + "-exchange";
    String dlxWaitIndexQueueName = TASK_QUEUE_NAME + "_wait_index_" + CONFIG.getRabbitmqQueueWaitIndexDlxTTL() + "-dlx";
    String dlxWaitIndexQExchangeName = dlxQueueName + "_wait_index_" + CONFIG.getRabbitmqQueueWaitIndexDlxTTL() + "-exchange";
    ConnectionFactory factory = null;
    Connection connection = null;
    Channel channel = null;
    private final Logger log = Loggers.getLogger(RabbitmqClient.class, "Changes Feed");

    public RabbitmqClient() {
        log.info("Connecting to rabbitmq: host " + CONFIG.getRabbitmqHost() + " port : " + CONFIG.getRabbitmqPort());
        this.factory = new ConnectionFactory();
        this.factory.setHost(CONFIG.getRabbitmqHost());
        this.factory.setPort(CONFIG.getRabbitmqPort());
        if (!CONFIG.getRabbitmqUsername().isEmpty()) {
            this.factory.setUsername(CONFIG.getRabbitmqUsername());
        }
        if (!CONFIG.getRabbitmqPassword().isEmpty()) {

            this.factory.setPassword(CONFIG.getRabbitmqPassword());
        }

    }

    private static final java.security.AccessControlContext RESTRICTED_CONTEXT = new java.security.AccessControlContext(
            new java.security.ProtectionDomain[]{
                new java.security.ProtectionDomain(null, getRestrictedPermissions())
            }
    );

    @SuppressForbidden(reason = "adds access for bean creation")
    static java.security.PermissionCollection getRestrictedPermissions() {
        java.security.Permissions perms = new java.security.Permissions();
        perms.add(new SocketPermission("*", "accept,connect,resolve"));
        perms.add(new MBeanServerPermission("*"));
        perms.add(new MBeanPermission("*", "*"));
        perms.add(new MBeanTrustPermission("*"));
        perms.add(new RuntimePermission("*"));
//       perms.add(new SocketPermission("redis:6379", "connect,resolve"));
//        perms.add(new SocketPermission("172.17.0.2:6379", "connect,resolve"));
//        perms.add(new AllPermission());

        return perms;
    }

    private boolean reNewConnectionIfClose() throws IOException, TimeoutException {
        boolean isConnected = false;
        try {
            if (this.connection == null || !connection.isOpen()) {
                this.connection = java.security.AccessController.doPrivileged((PrivilegedAction<Connection>) ()
                        -> {
                    try {
                        return this.factory.newConnection();
                    } catch (IOException ex) {
                        java.util.logging.Logger.getLogger(RabbitmqClient.class.getName()).log(Level.SEVERE, null, ex);
                        return null;
                    } catch (TimeoutException ex) {
                        java.util.logging.Logger.getLogger(RabbitmqClient.class.getName()).log(Level.SEVERE, null, ex);
                        return null;

                    }
                },
                        RESTRICTED_CONTEXT);
                if (this.connection.isOpen()) {
                    this.channel = this.connection.createChannel();
                    Map<String, Object> qArgs = new HashMap<>();
                    qArgs.put("x-dead-letter-exchange", dlxQExchangeName);
                    qArgs.put("x-dead-letter-routing-key", "");
                    this.channel.exchangeDeclare(qExchangeName, "direct", true);
                    this.channel.queueDeclare(TASK_QUEUE_NAME, true, false, false, qArgs);
                    this.channel.queueBind(TASK_QUEUE_NAME, qExchangeName, "", null);

                    Map<String, Object> qDlxArgs = new HashMap<>();
                    qDlxArgs.put("x-dead-letter-exchange", qExchangeName);
                    qDlxArgs.put("x-dead-letter-routing-key", "");
                    qDlxArgs.put("x-message-ttl", CONFIG.getRabbitmqQueueWaitIndexDlxTTL() * 1000);
                    this.channel.exchangeDeclare(dlxWaitIndexQExchangeName, "direct", true);
                    this.channel.queueDeclare(dlxWaitIndexQueueName, true, false, false, qDlxArgs);
                    this.channel.queueBind(dlxWaitIndexQueueName, dlxWaitIndexQExchangeName, "", null);
                    isConnected = true;
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
            log.error("Error in reNewConnectionIfClose :" + e);
        } finally {
            return isConnected;
        }
    }

    public void enqueue(String message) throws IOException, TimeoutException {
        if (this.reNewConnectionIfClose()) {
            channel.basicPublish("", dlxWaitIndexQueueName,
                    MessageProperties.PERSISTENT_TEXT_PLAIN,
                    message.getBytes());
            log.debug(" [x] Sent '" + message + "'");
        }
    }

}
