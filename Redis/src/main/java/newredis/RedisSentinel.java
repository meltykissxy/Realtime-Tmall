package newredis;

import utils.RedisUtil;
import redis.clients.jedis.Jedis;
import redis.clients.jedis.Tuple;

import java.util.Map;
import java.util.Set;

public class RedisSentinel {
    public static void main(String[] args) {
        Jedis jedis = RedisUtil.getJedisFromSentinel();
        jedis.set("k1000","v1000");
        Set<String> keyset = jedis.keys("*");
        for (String key : keyset) {
            System.out.println(key);
        }
        Map<String, String> userMap = jedis.hgetAll("user:0101");
        for (Map.Entry<String, String> entry : userMap.entrySet()) {
            System.out.println(entry.getKey() + ":" + entry.getValue());
        }

        Set<Tuple> topTuple = jedis.zrevrangeWithScores("article:topn", 0, 2);
        for (Tuple tuple : topTuple) {
            System.out.println(tuple.getElement() + ":" + tuple.getScore());
        }

        System.out.println(jedis.ping());
        jedis.close();
    }
}
