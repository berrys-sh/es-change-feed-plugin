package com.forgerock.elasticsearch.changes;

import org.apache.logging.log4j.Logger;
import org.elasticsearch.common.io.stream.BytesStreamOutput;
import org.elasticsearch.common.logging.Loggers;
import org.elasticsearch.common.xcontent.XContentBuilder;
import org.elasticsearch.common.xcontent.XContentType;
import org.elasticsearch.common.xcontent.json.JsonXContent;
import org.elasticsearch.index.engine.Engine;
import org.elasticsearch.index.shard.IndexingOperationListener;
import org.elasticsearch.index.shard.ShardId;
import org.joda.time.DateTime;

import java.io.IOException;
import java.util.Set;
import org.apache.commons.codec.digest.DigestUtils;

/**
 * Date: 04/05/2017 Time: 16:54
 */
public class WebSocketIndexListener implements IndexingOperationListener {

    private final Logger log = Loggers.getLogger(WebSocketIndexListener.class);
    private final Set<Source> sources;
    private final WebSocketRegister register;
    private final RedisClient redisClient;
    private final RabbitmqClient rabbitmqClient;
    private final static ConfigurationManager CONFIG = ConfigurationManager.getInstance();

    WebSocketIndexListener(Set<Source> sources, WebSocketRegister register, RedisClient redisClient, RabbitmqClient rabbitmqClient) {
        this.sources = sources;
        this.register = register;
        this.redisClient = redisClient;
        this.rabbitmqClient = rabbitmqClient;
    }

    @Override
    public void postIndex(ShardId shardId, Engine.Index index, Engine.IndexResult result) {

        ChangeEvent change = new ChangeEvent(
                shardId.getIndex().getName(),
                index.type(),
                index.id(),
                new DateTime(),
                result.isCreated() ? ChangeEvent.Operation.CREATE : ChangeEvent.Operation.INDEX,
                result.getVersion(),
                index.source()
        );

        addChange(change);
    }

    @Override
    public void postDelete(ShardId shardId, Engine.Delete delete, Engine.DeleteResult result) {

        ChangeEvent change = new ChangeEvent(
                shardId.getIndex().getName(),
                delete.type(),
                delete.id(),
                new DateTime(),
                ChangeEvent.Operation.DELETE,
                result.getVersion(),
                null
        );

        addChange(change);
    }

    private static boolean filter(String index, String type, String id, Source source) {
        if (source.getIndices() != null && !source.getIndices().contains(index)) {
            return false;
        }

        if (source.getTypes() != null && !source.getTypes().contains(type)) {
            return false;
        }

        if (source.getIds() != null && !source.getIds().contains(id)) {
            return false;
        }

        return true;
    }

    static boolean filter(ChangeEvent change, Set<Source> sources) {
        for (Source source : sources) {
            if (filter(change.getIndex(), change.getType(), change.getId(), source)) {
                return true;
            }
        }

        return false;
    }

    private void addChange(ChangeEvent change) {

        if (!filter(change, sources)) {
            return;
        }

        String message;
        String message4hash;

        try {
            XContentBuilder builder = new XContentBuilder(JsonXContent.jsonXContent, new BytesStreamOutput());
            builder.startObject()
                    .field("_index", change.getIndex())
                    .field("_type", change.getType())
                    .field("_id", change.getId())
                    .field("_timestamp", change.getTimestamp())
                    .field("_version", change.getVersion())
                    .field("_operation", change.getOperation().toString());
            if (change.getSource() != null) {
                builder.rawField("_source", change.getSource(), XContentType.JSON);
            }
            builder.endObject();

            message = builder.string();

            XContentBuilder builderHash = new XContentBuilder(JsonXContent.jsonXContent, new BytesStreamOutput());
            builderHash.startObject()
                    .field("_index", change.getIndex())
                    .field("_type", change.getType())
                    .field("_id", change.getId())
                    .field("_version", change.getVersion())
                    .field("_operation", change.getOperation().toString());
            if (change.getSource() != null) {
                builderHash.rawField("_source", change.getSource(), XContentType.JSON);
            }
            builderHash.endObject();

            message4hash = builderHash.string();
        } catch (IOException e) {
            log.error("Failed to write JSON", e);
            return;
        }
        String messageMd5 = DigestUtils.md5Hex(message4hash);
        try {
            if (this.redisClient.isFirst(messageMd5, CONFIG.getRedisIsFirstTTL())) {
                this.rabbitmqClient.enqueue(message);
            }
        } catch (Exception e) {
            log.error("Error enqueue msg", e);
        }
        for (WebSocketEndpoint listener : register.getListeners()) {
            try {
                listener.sendMessage(message);
            } catch (Exception e) {
                log.error("Failed to send message", e);
            }

        }

    }
}
