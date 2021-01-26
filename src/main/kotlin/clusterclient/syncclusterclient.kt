package clusterclient

import io.lettuce.core.RedisFuture
import io.lettuce.core.RedisURI
import io.lettuce.core.ScoredValue
import io.lettuce.core.cluster.RedisClusterClient
import io.lettuce.core.cluster.api.StatefulRedisClusterConnection
import io.lettuce.core.cluster.api.async.RedisAdvancedClusterAsyncCommands
import io.lettuce.core.codec.ByteArrayCodec
import io.lettuce.core.support.ConnectionPoolSupport
import org.apache.commons.pool2.impl.GenericObjectPool
import org.apache.commons.pool2.impl.GenericObjectPoolConfig
import java.nio.ByteBuffer
import java.time.Duration
import java.util.*

private const val FIRST_ELEMENT_INDEX: Long = 0
private const val LAST_ELEMENT_INDEX: Long = -1

class LRedisSyncClusterClient(
    host: String,
    port: Int,
    blockWhenExhausted: Boolean,
    maxTotal: Int,
    maxWaitMillis: Long,
    timeout: Int = 500
) {

    private var pool: GenericObjectPool<StatefulRedisClusterConnection<ByteArray, ByteArray>>

    init {
        val clusterClient = RedisClusterClient.create(RedisURI.create(host, port))
        clusterClient.defaultTimeout = Duration.ofMillis(timeout.toLong())

        val objectPoolConfig = GenericObjectPoolConfig()

        objectPoolConfig.maxTotal = maxTotal
        objectPoolConfig.maxWaitMillis = maxWaitMillis
        objectPoolConfig.blockWhenExhausted = blockWhenExhausted

        val connection = clusterClient.connect(ByteArrayCodec())

        pool = ConnectionPoolSupport
            .createGenericObjectPool({ connection }, objectPoolConfig)
    }

    fun addToSet(key: String, values: List<String>) {
        pool.borrowObject().use { connection ->
            connection.sync().sadd(getBytes(key), *values.map { getBytes(it) }.toTypedArray())
        }
    }

    fun addToSortedSet(key: String, score: Long, values: List<String>) {
        val scoredValues: MutableList<ScoredValue<ByteArray>> = mutableListOf()

        values.forEach { value ->
            scoredValues.add(
                ScoredValue.fromNullable(score.toDouble(), getBytes(value))
            )
        }

        pool.borrowObject().use { connection ->
            connection.sync().zadd(getBytes(key), *scoredValues.toTypedArray())
        }
    }

    fun delete(key: String) {
        pool.borrowObject().use { connection ->
            connection.sync().del(getBytes(key))
        }
    }


    fun removeFromSortedSet(key: String, value: String) {
        pool.borrowObject().use { connection ->
            connection.sync().zrem(getBytes(key), getBytes(value))
        }
    }

    fun getAllSortedSetMembers(key: String): Set<String> {
        pool.borrowObject().use { connection ->
            return toStringSet(
                connection.sync().zrevrange(
                    getBytes(key),
                    FIRST_ELEMENT_INDEX,
                    LAST_ELEMENT_INDEX
                ).toMutableSet()
            )
        }
    }

    fun countSortedSetMembers(key: String): Long {
        pool.borrowObject().use { connection ->
            return connection.sync().zcard(getBytes(key))
        }
    }

    fun getSetMembers(key: String): Set<String> {
        pool.borrowObject().use { connection ->
            return toStringSet(connection.sync().smembers(getBytes(key)))
        }
    }

    fun removeMinimumScoredElementsFromSortedSet(key: String, numberOfMembers: Int): Set<String> {
        pool.borrowObject().use { connection ->
            return toStringSet(connection.sync().zpopmin(getBytes(key), numberOfMembers.toLong()).map { it.value }
                .toMutableSet())
        }
    }

    fun popFromSet(key: String, numberOfMembers: Int): Set<String> {
        pool.borrowObject().use { connection ->
            return toStringSet(connection.sync().spop(getBytes(key), numberOfMembers.toLong()))
        }
    }

    fun removeFromSet(key: String, memberToRemove: String) {
        pool.borrowObject().use { connection ->
            connection.sync().srem(getBytes(key), getBytes(memberToRemove))
        }
    }

    private fun getBytes(key: String): ByteArray {
        return try {
            bytesFromUUID(UUID.fromString(key))
        } catch (e: IllegalArgumentException) {
            key.toByteArray()
        }
    }

    private fun toStringSet(mutableSet: MutableSet<ByteArray>): Set<String> {
        return mutableSet.map {
            try {
                String(getBytes(uuidFromBytes(it).toString()))
            } catch (e: Exception) {
                String(it)
            }
        }.toSet()
    }

    private fun bytesFromUUID(uuid: UUID): ByteArray {
        val bb: ByteBuffer = ByteBuffer.wrap(ByteArray(16))
        bb.putLong(uuid.mostSignificantBits)
        bb.putLong(uuid.leastSignificantBits)
        return bb.array()
    }

    private fun uuidFromBytes(bytes: ByteArray): UUID? {
        val byteBuffer = ByteBuffer.wrap(bytes)
        val firstLong = byteBuffer.long
        val secondLong = byteBuffer.long
        return UUID(firstLong, secondLong)
    }
}