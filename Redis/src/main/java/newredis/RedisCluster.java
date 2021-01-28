package newredis;

import utils.RedisUtil;
import redis.clients.jedis.JedisCluster;

public class RedisCluster {
    public static void main(String[] args) {
        JedisCluster jedisCluster = RedisUtil.getJedisCluster();
        jedisCluster.set("k1", "v1");
        // 关闭整个连接池
        jedisCluster.close();
        // 关闭之后，你就不能用了。所以别轻易关闭
    }
}
