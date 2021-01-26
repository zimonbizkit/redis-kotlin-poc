import clusterclient.LRedisClusterClient
import clusterclient.LRedisSyncClusterClient
import io.lettuce.core.LettuceFutures
import io.lettuce.core.RedisFuture
import io.lettuce.core.ScoredValue
import java.io.OutputStream
import java.util.concurrent.CompletableFuture
import java.util.concurrent.TimeUnit
import java.util.function.BiFunction
import java.util.logging.*
import kotlin.system.exitProcess
import kotlin.system.measureTimeMillis

private const val MAX_MATCH_COUNT = 100
fun main(args: Array<String>) {
    println("Hello World!")

    //logger.level = Level.WARNING
    val redisClient = LRedisClusterClient(
        "0.0.0.0",
        7000,
        false,
        200,
        500
    )


    /*println(" [ASYNC]--------->>>>> adding elapsed  ${time} milliseconds ")


    val timeE = measureTimeMillis {
        basicEviction(redisClient, potato)
    }*/


    /*val timeS = measureTimeMillis {
        syncRedisClient.addToSortedSet("edu2", System.nanoTime(), potato.values)
        potato.values.forEach { syncRedisClient.addToSet(it,listOf("edu2")) }
    }
    println(" [SYNC]--------->>>>> AddToSortedSetElapsed ${timeS} milliseconds ")*/

    //redisClient.closeConnection()
    exitProcess(0)


}
/*
private fun basicEviction(redisClient: LRedisClusterClient, potato: potato) {
     redisClient.commands.zcard(potato.name.toByteArray())
        .thenApply { res ->
            if (res - MAX_MATCH_COUNT > 0) {
                redisClient.commands.zpopmin(potato.name.toByteArray(), res).thenApply {
                    it.forEach { sv -> redisClient.commands.spop(sv.value) }
                }
            }
        }
    redisClient.flushCommands()

}


private fun basicInsertion(redisClient: LRedisClusterClient, potato: potato) {
    redisClient.addToSortedSet(potato.name, System.nanoTime(), potato.values)
    potato.values.chunked(50000).forEach {
        it.forEach {
            redisClient.addToSet(it, listOf(potato.name))
        }
    }
    redisClient.flushCommands()
}
*/
data class potato(val name: String, val values: List<String>)