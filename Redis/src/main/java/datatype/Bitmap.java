package datatype;

import utils.RedisUtil;
import redis.clients.jedis.BitOP;
import redis.clients.jedis.Jedis;

public class Bitmap {
    public static void main(String[] args) {
        Jedis jedis = RedisUtil.getJedis();
        jedis.del("sign_week_01");
        jedis.del("sign_week_02");

        jedis.setbit("sign_week_01", 0, "1");
        jedis.setbit("sign_week_01", 1, "0");
        jedis.setbit("sign_week_01", 2, "1");
        jedis.setbit("sign_week_01", 3, "1");
        jedis.setbit("sign_week_01", 4, "0");
        jedis.setbit("sign_week_01", 5, "1");
        jedis.setbit("sign_week_01", 6, "1");

        jedis.setbit("sign_week_02", 0, "1");
        jedis.setbit("sign_week_02", 1, "1");
        jedis.setbit("sign_week_02", 2, "1");
        jedis.setbit("sign_week_02", 3, "1");
        jedis.setbit("sign_week_02", 4, "0");
        jedis.setbit("sign_week_02", 5, "0");
        jedis.setbit("sign_week_02", 6, "1");

        System.out.println(jedis.bitcount("sign", 0, -1));

        jedis.bitop(BitOP.AND, "AND_01_02", "sign_week_01", "sign_week_02");

        jedis.close();
    }
}
