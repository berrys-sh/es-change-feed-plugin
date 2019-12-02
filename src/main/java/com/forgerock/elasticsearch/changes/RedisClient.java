/*
 * To change this license header, choose License Headers in Project Properties.
 * To change this template file, choose Tools | Templates
 * and open the template in the editor.
 */
package com.forgerock.elasticsearch.changes;

import org.apache.logging.log4j.Logger;
import org.elasticsearch.common.logging.Loggers;
import redis.clients.jedis.Jedis;

/**
 *
 * @author berrys
 */
public class RedisClient {

    private final Logger log = Loggers.getLogger(WebSocketEndpoint.class);

    private Jedis jedis;

    public RedisClient() {
        jedis = new Jedis("redis", 6379);
    }

    public void pushBl(String message) {
        jedis.lpush("queue#tasks", message);

    }

}
