import org.junit.jupiter.api.Test;
import redis.clients.jedis.Jedis;
import utils.RedisUtil;

public class RedisKiss {
    public static void main(String[] args) {
        Jedis jedis = RedisUtil.getJedis();
        //System.out.println(jedis.keys("*").toString());
        for (String str : jedis.smembers("dau:2021-01-28")) {
            System.out.println(str);
        }
        jedis.close();
    }

    // KISS Hello
    @Test
    public void getAllKeys() {
        Jedis jedis = RedisUtil.getJedis();
        //System.out.println(jedis.keys("*").toString());
        for (String str : jedis.keys("*")) {
            System.out.println(str);
        }
        jedis.close();
    }
}
