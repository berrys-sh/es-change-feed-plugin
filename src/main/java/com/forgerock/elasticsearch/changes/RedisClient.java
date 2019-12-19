/*
 * To change this license header, choose License Headers in Project Properties.
 * To change this template file, choose Tools | Templates
 * and open the template in the editor.
 */
package com.forgerock.elasticsearch.changes;

import org.apache.logging.log4j.Logger;
import org.elasticsearch.common.logging.Loggers;
import redis.clients.jedis.Jedis;
import redis.clients.jedis.JedisPool;

/**
 *
 * @author berrys
 */

public class RedisClient {
    private final Logger log = Loggers.getLogger(WebSocketEndpoint.class);
    private JedisPool pool = null;

    public RedisClient() {
        this.pool = new JedisPool("redis", 6379);

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
        } catch (Exception e) {
            log.info("isFirst error :" + e);
            throw e;
        } finally {
            if (null != jedis) {
                jedis.close();
            }
        }
        return result;
    }

 

}