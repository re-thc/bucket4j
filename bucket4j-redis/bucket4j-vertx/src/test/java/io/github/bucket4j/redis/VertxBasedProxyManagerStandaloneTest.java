package io.github.bucket4j.redis;

import io.github.bucket4j.distributed.serialization.Mapper;
import io.github.bucket4j.redis.vertx.Bucket4jVertx;
import io.github.bucket4j.tck.AbstractDistributedBucketTest;
import io.github.bucket4j.tck.ProxyManagerSpec;
import io.vertx.core.Vertx;
import io.vertx.redis.client.Redis;
import io.vertx.redis.client.RedisOptions;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.BeforeAll;
import org.testcontainers.containers.GenericContainer;

import java.nio.charset.StandardCharsets;
import java.util.Arrays;
import java.util.UUID;

public class VertxBasedProxyManagerStandaloneTest extends AbstractDistributedBucketTest {

    private static GenericContainer container;
    private static Vertx vertx;
    private static Redis redis;

    @BeforeAll
    public static void setup() {
        container = startRedisContainer();
        vertx = Vertx.vertx();
        redis = createVertxClient(container, vertx);

        specs = Arrays.asList(
            new ProxyManagerSpec<>(
                "VertxBasedProxyManager_ByteArrayKey",
                () -> UUID.randomUUID().toString().getBytes(StandardCharsets.UTF_8),
                () -> Bucket4jVertx.casBasedBuilder(redis)
            ).checkExpiration(),
            new ProxyManagerSpec<>(
                "VertxBasedProxyManager_StringKey",
                () -> UUID.randomUUID().toString(),
                () -> Bucket4jVertx.casBasedBuilder(redis).keyMapper(Mapper.STRING)
            ).checkExpiration()
        );
    }

    @AfterAll
    public static void shutdown() {
        try {
            try {
                if (redis != null) {
                    redis.close().toCompletionStage().toCompletableFuture().join();
                }
            } finally {
                if (vertx != null) {
                    vertx.close().toCompletionStage().toCompletableFuture().join();
                }
            }
        } finally {
            if (container != null) {
                container.close();
            }
        }
    }

    private static GenericContainer startRedisContainer() {
        GenericContainer genericContainer = new GenericContainer("redis:4.0.11")
            .withExposedPorts(6379);
        genericContainer.start();
        return genericContainer;
    }

    private static Redis createVertxClient(GenericContainer container, Vertx vertx) {
        String redisUrl = "redis://" + container.getHost() + ":" + container.getMappedPort(6379);
        RedisOptions redisOptions = new RedisOptions().setConnectionString(redisUrl);
        return Redis.createClient(vertx, redisOptions);
    }

}
