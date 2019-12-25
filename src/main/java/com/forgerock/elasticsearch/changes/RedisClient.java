/*
 * To change this license header, choose License Headers in Project Properties.
 * To change this template file, choose Tools | Templates
 * and open the template in the editor.
 */
package com.forgerock.elasticsearch.changes;

import java.security.PrivilegedAction;
import javax.management.MBeanPermission;
import javax.management.MBeanServerPermission;
import javax.management.MBeanTrustPermission;
import org.apache.logging.log4j.Logger;
import org.elasticsearch.common.logging.Loggers;
import redis.clients.jedis.Jedis;
import redis.clients.jedis.JedisPool;
import org.elasticsearch.SpecialPermission;
import org.elasticsearch.common.SuppressForbidden;
import redis.clients.jedis.exceptions.JedisException;

/**
 *
 * @author berrys
 */
public class RedisClient {

    private final Logger log = Loggers.getLogger(RedisClient.class);
    private JedisPool pool = null;
    private final static ConfigurationManager CONFIG = ConfigurationManager.getInstance();

    private static final java.security.AccessControlContext RESTRICTED_CONTEXT = new java.security.AccessControlContext(
            new java.security.ProtectionDomain[]{
                new java.security.ProtectionDomain(null, getRestrictedPermissions())
            }
    );

    @SuppressForbidden(reason = "adds access for bean creation")
    static java.security.PermissionCollection getRestrictedPermissions() {
        java.security.Permissions perms = new java.security.Permissions();
        perms.add(new MBeanServerPermission("*"));
        perms.add(new MBeanPermission("*", "*"));
        perms.add(new MBeanTrustPermission("*"));
        perms.add(new RuntimePermission("*"));
        return perms;
    }

    public RedisClient() {
        SecurityManager sm = System.getSecurityManager();
        if (sm != null) {
            // unprivileged code such as scripts do not have SpecialPermission
            sm.checkPermission(new SpecialPermission());
        }
        log.info("Connecting to redis: host " + CONFIG.getRedisHost() + " port : " + CONFIG.getRedisPort());

        this.pool = java.security.AccessController.doPrivileged((PrivilegedAction<JedisPool>) ()
                -> new JedisPool(CONFIG.getRedisHost(), CONFIG.getRedisPort()),
                RESTRICTED_CONTEXT);

    }

    public boolean isFirst(String key, int ttl) {
        boolean result = false;
        Jedis jedis = null;
        try {
            jedis = pool.getResource();
            if (jedis.setnx(key, "TRUE") == 1) {
                jedis.expire(key, ttl);
                result = true;
            } else {
                result = false;
            }
        } catch (JedisException e) {
            log.info("isFirst connection exception");
            throw e;
        } catch (Exception e) {
            log.info("isFirst error ");
            throw e;
        } finally {
            if (null != jedis) {
                jedis.close();
            }
        }
        return result;
    }

}
