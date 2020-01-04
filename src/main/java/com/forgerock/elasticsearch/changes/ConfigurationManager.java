/*
 * To change this license header, choose License Headers in Project Properties.
 * To change this template file, choose Tools | Templates
 * and open the template in the editor.
 */
package com.forgerock.elasticsearch.changes;

import java.io.FileInputStream;
import java.io.FileNotFoundException;
import java.io.FilePermission;
import java.io.InputStream;
import java.nio.file.Paths;
import java.security.PrivilegedAction;
import java.util.HashMap;
import java.util.Properties;
import java.util.PropertyPermission;
import org.apache.logging.log4j.Logger;
import org.elasticsearch.SpecialPermission;
import org.elasticsearch.common.SuppressForbidden;
import org.elasticsearch.common.logging.Loggers;

/**
 *
 * @author berrys
 */
public class ConfigurationManager {

    private static final HashMap<String, String> CONFIG = new HashMap<String, String>();
    private final Logger log = Loggers.getLogger(ConfigurationManager.class);
    private static ConfigurationManager single_instance = null;
    private static final String SETTING_PORT = "changes.port";
    private static final String SETTING_REDIS_PORT = "changes.redis.port";
    private static final String SETTING_REDIS_HOST = "changes.redis.host";
    private static final String SETTING_REDIS_ISFIRST_TLL = "changes.redis.isfirst.ttl";
    private static final String SETTING_RABBITMQ_PORT = "changes.rabbitmq.port";
    private static final String SETTING_RABBITMQ_HOST = "changes.rabbitmq.host";
    private static final String SETTING_RABBITMQ_USERNAME = "changes.rabbitmq.username";
    private static final String SETTING_RABBITMQ_PASSWORD = "changes.rabbitmq.password";
    private static final String SETTING_RABBITMQ_QUEUE_NAME = "changes.rabbitmq.queue.name";
    private static final String SETTING_RABBITMQ_QUEUEDLX_TTL = "changes.rabbitmq.queuedlx.ttl";
    private static final String SETTING_RABBITMQ_QUEUEWAITINDEXDLX_TTL = "changes.rabbitmq.queuewaitindex.dlx.ttl";
    private static final String SETTING_DISABLE = "changes.disable";
    private static final String SETTING_LISTEN_SOURCE = "changes.listenSource";
    private static final String SETTING_VERSION = "changes.version";

    private ConfigurationManager() {
        SecurityManager sm = System.getSecurityManager();
        if (sm != null) {
            // unprivileged code such as scripts do not have SpecialPermission
            sm.checkPermission(new SpecialPermission());
        }
        try {
            InputStream input = java.security.AccessController.doPrivileged((PrivilegedAction<InputStream>) new PrivilegedAction<InputStream>() {
                @Override
                public InputStream run() {
                    try {
                        log.info("Current path (elasticsearch directory)= " + Paths.get(".").toAbsolutePath().normalize().toString());
                        return new FileInputStream(Paths.get(".").toAbsolutePath().normalize().toString() + "/plugins/es-change-feed-plugin/plugin-descriptor.properties");
                    } catch (FileNotFoundException ex) {
                        log.error("FileInputStream error: " + ex);
                        return null;
                    }
                }
            },
                    RESTRICTED_CONTEXT);
            Properties prop = new Properties();
            if (input == null) {
                throw new Exception("Sorry, unable to find plugin-descriptor.properties");
            }
            // load a properties file

            prop.load(input);
            ConfigurationManager.CONFIG.put(SETTING_PORT, prop.getProperty(SETTING_PORT));
            ConfigurationManager.CONFIG.put(SETTING_REDIS_HOST, prop.getProperty(SETTING_REDIS_HOST));
            ConfigurationManager.CONFIG.put(SETTING_REDIS_PORT, prop.getProperty(SETTING_REDIS_PORT));
            ConfigurationManager.CONFIG.put(SETTING_REDIS_ISFIRST_TLL, prop.getProperty(SETTING_REDIS_ISFIRST_TLL));
            ConfigurationManager.CONFIG.put(SETTING_RABBITMQ_HOST, prop.getProperty(SETTING_RABBITMQ_HOST));
            ConfigurationManager.CONFIG.put(SETTING_RABBITMQ_PORT, prop.getProperty(SETTING_RABBITMQ_PORT));
            ConfigurationManager.CONFIG.put(SETTING_RABBITMQ_USERNAME, prop.getProperty(SETTING_RABBITMQ_USERNAME));
            ConfigurationManager.CONFIG.put(SETTING_RABBITMQ_PASSWORD, prop.getProperty(SETTING_RABBITMQ_PASSWORD));
            ConfigurationManager.CONFIG.put(SETTING_RABBITMQ_QUEUE_NAME, prop.getProperty(SETTING_RABBITMQ_QUEUE_NAME));
            ConfigurationManager.CONFIG.put(SETTING_RABBITMQ_QUEUEDLX_TTL, prop.getProperty(SETTING_RABBITMQ_QUEUEDLX_TTL));
            ConfigurationManager.CONFIG.put(SETTING_RABBITMQ_QUEUEWAITINDEXDLX_TTL, prop.getProperty(SETTING_RABBITMQ_QUEUEWAITINDEXDLX_TTL));
            ConfigurationManager.CONFIG.put(SETTING_LISTEN_SOURCE, prop.getProperty(SETTING_LISTEN_SOURCE));
            ConfigurationManager.CONFIG.put(SETTING_DISABLE, prop.getProperty(SETTING_DISABLE));
            ConfigurationManager.CONFIG.put(SETTING_VERSION, prop.getProperty(SETTING_VERSION));

            log.info("************************ es-change-feed-plugin configuration  ******************************");
            ConfigurationManager.CONFIG.entrySet().forEach(entry -> {
                log.info(entry.getKey() + " = " + entry.getValue());
            });
            log.info("********************************************************************************************");

        } catch (Exception ex) {
            log.error("Error load configuration : " + ex);
        }
    }

    public static ConfigurationManager getInstance() {
        if (single_instance == null) {
            single_instance = new ConfigurationManager();
        }

        return single_instance;
    }

    private static final java.security.AccessControlContext RESTRICTED_CONTEXT = new java.security.AccessControlContext(
            new java.security.ProtectionDomain[]{
                new java.security.ProtectionDomain(null, getRestrictedPermissions())
            }
    );

    @SuppressForbidden(reason = "adds access for bean creation")
    static java.security.PermissionCollection getRestrictedPermissions() {
        java.security.Permissions perms = new java.security.Permissions();
        perms.add(new FilePermission("*", "read"));
        perms.add(new PropertyPermission("user.dir", "read"));
        perms.add(new FilePermission("<<ALL FILES>>", "read"));
        return perms;
    }

    public boolean getIsDisable() {
        return Boolean.parseBoolean(ConfigurationManager.CONFIG.get(SETTING_DISABLE));
    }

    public int getSocketPort() {
        return Integer.parseInt(ConfigurationManager.CONFIG.get(SETTING_PORT));
    }

    public int getRedisPort() {
        return Integer.parseInt(ConfigurationManager.CONFIG.get(SETTING_REDIS_PORT));
    }

    public String getRedisHost() {
        return ConfigurationManager.CONFIG.get(SETTING_REDIS_HOST);
    }

    public int getRedisIsFirstTTL() {
        return Integer.parseInt(ConfigurationManager.CONFIG.get(SETTING_REDIS_ISFIRST_TLL));
    }

    public int getRabbitmqPort() {
        return Integer.parseInt(ConfigurationManager.CONFIG.get(SETTING_RABBITMQ_PORT));
    }

    public String getRabbitmqHost() {
        return ConfigurationManager.CONFIG.get(SETTING_RABBITMQ_HOST);
    }

    public String getRabbitmqUsername() {
        return ConfigurationManager.CONFIG.get(SETTING_RABBITMQ_USERNAME);
    }

    public String getRabbitmqPassword() {
        return ConfigurationManager.CONFIG.get(SETTING_RABBITMQ_PASSWORD);
    }

    public String getRabbitmqQueueName() {
        return ConfigurationManager.CONFIG.get(SETTING_RABBITMQ_QUEUE_NAME);
    }

    public int getRabbitmqQueuedlxTTL() {
        return Integer.parseInt(ConfigurationManager.CONFIG.get(SETTING_RABBITMQ_QUEUEDLX_TTL));
    }

    public int getRabbitmqQueueWaitIndexDlxTTL() {
        return Integer.parseInt(ConfigurationManager.CONFIG.get(SETTING_RABBITMQ_QUEUEWAITINDEXDLX_TTL));
    }

    public String getListenSource() {
        return ConfigurationManager.CONFIG.get(SETTING_LISTEN_SOURCE);
    }
}
