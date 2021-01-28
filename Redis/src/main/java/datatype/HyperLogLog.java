package datatype;

import utils.RedisUtil;import redis.clients.jedis.Jedis;

public class HyperLogLog {
    public static void main(String[] args) {
        Jedis jedis = RedisUtil.getJedis();
        jedis.del("crawled:20171124");
        jedis.del("crawled:20171125");
        jedis.del("crawled:20171126");
        jedis.del("crawled:20171124_25_26");

        jedis.pfadd("crawled:20171124", "http://www.google.com/");
        jedis.pfadd("crawled:20171124", "http://www.redislabs.com/");
        jedis.pfadd("crawled:20171124", "http://www.redis.io/");
        jedis.pfadd("crawled:20171125", "http://www.redisearch.io/");
        jedis.pfadd("crawled:20171125", "http://www.redis.io/");
        jedis.pfadd("crawled:20171126", "http://www.google.com/");
        jedis.pfadd("crawled:20171126", "http://www.redisearch.io/");

        System.out.println(jedis.pfcount("crawled:20171124"));
        System.out.println(jedis.pfcount("crawled:20171125"));
        System.out.println(jedis.pfcount("crawled:20171126"));

        jedis.pfmerge("crawled:20171124_25_26", "crawled:20171124", "crawled:20171125", "crawled:20171126");
        System.out.println(jedis.pfcount("crawled:20171124_25_26"));

        jedis.close();
    }
}
