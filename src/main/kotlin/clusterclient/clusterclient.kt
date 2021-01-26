package clusterclient

import io.lettuce.core.RedisURI
import io.lettuce.core.cluster.RedisClusterClient
import io.lettuce.core.cluster.api.StatefulRedisClusterConnection
import io.lettuce.core.cluster.api.async.RedisAdvancedClusterAsyncCommands
import io.lettuce.core.codec.ByteArrayCodec
import io.lettuce.core.support.AsyncConnectionPoolSupport
import io.lettuce.core.support.BoundedPoolConfig
import org.apache.commons.pool2.impl.GenericObjectPoolConfig
import java.time.Duration
import java.util.*
import java.util.concurrent.CompletableFuture
import java.util.concurrent.Future
import kotlin.collections.ArrayList


private const val FIRST_ELEMENT_INDEX: Long = 0
private const val LAST_ELEMENT_INDEX: Long = -1

class LRedisClusterClient(
    host: String,
    port: Int,
    blockWhenExhausted: Boolean,
    maxTotal: Int,
    maxWaitMillis: Long,
    timeout: Int = 500
) {

    lateinit var connection: StatefulRedisClusterConnection<String, String>
    var futures : ArrayList<Future<*>> = ArrayList()

    init {
        val clusterClient = RedisClusterClient.create(RedisURI.create(host, port))
        clusterClient.defaultTimeout = Duration.ofMillis(timeout.toLong())
        clusterClient.refreshPartitions()

        val objectPoolConfig = GenericObjectPoolConfig()

        objectPoolConfig.maxTotal = maxTotal
        objectPoolConfig.maxWaitMillis = maxWaitMillis
        objectPoolConfig.blockWhenExhausted = blockWhenExhausted

        val poolFuture  =
            AsyncConnectionPoolSupport.createBoundedObjectPoolAsync(
                { clusterClient.connectAsync(ByteArrayCodec()) },
                BoundedPoolConfig.builder()
                    .maxTotal(maxTotal)
                    .build()
            ).toCompletableFuture().join()

        println("connection created ${poolFuture}")
        val setResult: CompletableFuture<String> = poolFuture.acquire().thenCompose { connection ->
            val async: RedisAdvancedClusterAsyncCommands<ByteArray, ByteArray> = connection.async()
            async.
            async.set("key1".toByteArray(),"value2".toByteArray() ).whenComplete {s: String?, throwable: Throwable? -> println("completed1")}
            async.set("key2".toByteArray(),"value2".toByteArray() ).whenComplete {s: String?, throwable: Throwable? -> println("completed2")}
            async.set("key3".toByteArray(),"value2".toByteArray() ).whenComplete {s: String?, throwable: Throwable? -> println("completed3")}
            async.set("key4".toByteArray(),"value2".toByteArray() ).whenComplete {s: String?, throwable: Throwable? -> println("completed4")}
            async.set("key5".toByteArray(),"value2".toByteArray() ).whenComplete {s: String?, throwable: Throwable? -> println("completed5")}
            async.set("key6".toByteArray(),"value2".toByteArray() ).whenComplete {s: String?, throwable: Throwable? -> println("completed6")}
            async.set("key7".toByteArray(),"value2".toByteArray() ).whenComplete {s: String?, throwable: Throwable? -> println("completed7")}
            async.set("key8".toByteArray(),"value2".toByteArray() ).whenComplete {s: String?, throwable: Throwable? -> println("completed8")}
            async.set("key9".toByteArray(),"value2".toByteArray() )
                .whenComplete { s: String?, throwable: Throwable? ->
                    println("Closing connection : ${s} | ${throwable.toString()}")
                    /*poolFuture.release(
                        connection
                    )*/
                }
        }
         setResult.join()
    }


    /*fun addToSortedSet(key: String, score: Long, values: List<String>) {
        val scoredValues: MutableList<ScoredValue<ByteArray>> = mutableListOf()

        values.forEach { value ->
            scoredValues.add(
                ScoredValue.fromNullable(score.toDouble(), getBytes(value))
            )
        }

        futures.add(commands.zadd(getBytes(key), *scoredValues.toTypedArray()))
    }

    fun addToSet(key: String, values: List<String>) {
        futures.add(commands.sadd(getBytes(key), *values.map { getBytes(it) }.toTypedArray()))
    }

    fun flushCommands() {
        commands.flushCommands()
        LettuceFutures.awaitAll(5, TimeUnit.NANOSECONDS,*futures.toTypedArray())
    }

    fun closeConnection() {
        connection.close()
    }

    fun delete(key: String) {
        pool.borrowObject().use { connection ->
            connection.sync().del(getBytes(key))
        }
    }


    fun getSetMembers(key: String): Set<String> {
        pool.borrowObject().use { connection ->
            return toStringSet(connection.sync().smembers(getBytes(key)))
        }
    }

    // 2
    fun removeMinimumScoredElementsFromSortedSet(key: String, numberOfMembers: Int): Set<String> {
        pool.borrowObject().use { connection ->
            return toStringSet(connection.sync().zpopmin(getBytes(key), numberOfMembers.toLong()).map { it.value }
                .toMutableSet())
        }
    }

    // 3
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
    }*/
}