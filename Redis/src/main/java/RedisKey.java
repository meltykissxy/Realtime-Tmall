import utils.RedisUtil;
import org.junit.jupiter.api.Test;
import redis.clients.jedis.Jedis;

public class RedisKey {
    @Test
    public void run01() {
        Jedis jedis = RedisUtil.getJedis();
        // KISS 简单的赋值，注意这里可以返回成功还是失败
        System.out.println(jedis.set("k1", "lss"));
        System.out.println(jedis.set("k2", "ly"));
        System.out.println(jedis.set("k3", "yjy"));
        System.out.println(jedis.set("a1", "dh"));
        System.out.println(jedis.set("b2", "chj"));
        // KISS 简单的获取key对应的值
        System.out.println(jedis.get("k1"));
        // KISS 获取所有key
        System.out.println(jedis.keys("*").toString());
        System.out.println(jedis.keys("k*").toString());
        System.out.println(jedis.keys("*1").toString());
        jedis.close();
    }
}
