package io.github.bucket4j.redis;

import io.github.bucket4j.distributed.serialization.Mapper;
import io.github.bucket4j.redis.vertx.Bucket4jVertx;
import io.github.bucket4j.tck.AbstractDistributedBucketTest;
import io.github.bucket4j.tck.ProxyManagerSpec;
import io.vertx.core.Vertx;
import io.vertx.redis.client.Redis;
import io.vertx.redis.client.RedisClientType;
import io.vertx.redis.client.RedisOptions;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.BeforeAll;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.testcontainers.containers.GenericContainer;

import java.nio.charset.StandardCharsets;
import java.util.Arrays;
import java.util.UUID;

public class VertxBasedProxyManagerClusterTest extends AbstractDistributedBucketTest {

    private static final Logger logger = LoggerFactory.getLogger(VertxBasedProxyManagerClusterTest.class);
    private static final Integer[] CONTAINER_CLUSTER_PORTS = {7000, 7001, 7002, 7003, 7004, 7005};
    private static final int CLUSTER_START_TIMEOUT_SECONDS = 200;

    private static GenericContainer container;
    private static Vertx vertx;
    private static Redis redis;

    @BeforeAll
    public static void setup() {
        container = startRedisContainer();
        vertx = Vertx.vertx();
        redis = createVertxClusterClient(container, vertx);

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

    private static Redis createVertxClusterClient(GenericContainer container, Vertx vertx) {
        RedisOptions options = new RedisOptions().setType(RedisClientType.CLUSTER);
        for (Integer containerPort : CONTAINER_CLUSTER_PORTS) {
            String endpoint = "redis://" + container.getHost() + ":" + container.getMappedPort(containerPort);
            options.addConnectionString(endpoint);
        }
        return Redis.createClient(vertx, options);
    }

    private static GenericContainer startRedisContainer() {
        GenericContainer genericContainer = new GenericContainer("grokzen/redis-cluster:6.0.7");
        genericContainer.withExposedPorts(CONTAINER_CLUSTER_PORTS);
        genericContainer.start();

        for (int i = 0; i < CLUSTER_START_TIMEOUT_SECONDS; i++) {
            try {
                String clusterInfo = genericContainer.execInContainer("redis-cli", "-p", "7000", "cluster", "info").getStdout();
                if (clusterInfo.contains("cluster_state:ok")) {
                    return genericContainer;
                }
            } catch (Exception e) {
                logger.error("Failed to check Redis Cluster availability: {}", e.getMessage(), e);
            }

            try {
                Thread.sleep(1000);
            } catch (InterruptedException e) {
                throw new RuntimeException(e);
            }
        }

        throw new IllegalStateException("Cluster was not assembled in " + CLUSTER_START_TIMEOUT_SECONDS + " seconds");
    }

}
