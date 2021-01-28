import utils.RedisUtil;
import org.junit.jupiter.api.Test;
import redis.clients.jedis.BitOP;
import redis.clients.jedis.Jedis;
import redis.clients.jedis.Transaction;
import redis.clients.jedis.Tuple;

import java.util.Map;
import java.util.Set;

public class RedisTest {

    // KISS Jedis事务操作，重点
    @Test
    public void run14() {
        Jedis jedis = new Jedis("hadoop102", 6379);
        jedis.set("money", "100");
        jedis.set("out", "0");

        jedis.watch("money");

        //开启redis事务
        Transaction multi = jedis.multi();
        int a = 0;
        try {
            multi.decrBy("money", 20);
            multi.incrBy("out", 20);
        } catch (Exception e) {
            //事务回滚
            multi.discard();
            e.printStackTrace();
        }
        //redis事务提交
        multi.exec();

        System.out.println(jedis.get("money"));
        System.out.println(jedis.get("out"));

        jedis.close();
    }

    @Test
    public void run13() {
        Jedis jedis = new Jedis("hadoop102",6379);
        jedis.flushDB();
        jedis.setbit("user:001:week01", 0, "1");
        jedis.setbit("user:001:week01", 1, "0");
        jedis.setbit("user:001:week01", 2, "0");
        jedis.setbit("user:001:week01", 3, "1");
        jedis.setbit("user:001:week01", 4, "0");
        jedis.setbit("user:001:week01", 5, "1");
        jedis.setbit("user:001:week01", 6, "0");

        jedis.setbit("user:002:week01", 0, "0");
        jedis.setbit("user:002:week01", 1, "0");
        jedis.setbit("user:002:week01", 2, "1");
        jedis.setbit("user:002:week01", 3, "1");
        jedis.setbit("user:002:week01", 4, "1");
        jedis.setbit("user:002:week01", 5, "1");
        jedis.setbit("user:002:week01", 6, "1");

        jedis.bitop(BitOP.AND, "BitOP.AND01", "user:001:week01", "user:002:week01");
        jedis.bitop(BitOP.OR, "BitOP.OR01", "user:001:week01", "user:002:week01");
        jedis.bitop(BitOP.XOR, "BitOP.XOR01", "user:001:week01", "user:002:week01");

        jedis.close();
    }

    @Test
    public void run09() {
        Jedis jedis = new Jedis("hadoop102",6379);
        jedis.set("k12", "abcdefg");
        jedis.setrange("k12", 2, "ly");
        System.out.println(jedis.get("k12"));// ablyefg
        jedis.close();
    }

    @Test
    public void run08() {
        Jedis jedis = new Jedis("hadoop102",6379);
        jedis.set("i1", "13");
        jedis.incrBy("i1", 2);
        System.out.println(jedis.get("i1"));

        jedis.set("s1", "lss");
        jedis.append("s1", "lovely");
        jedis.strlen("s1");
        System.out.println(jedis.getrange("s1", 0, 3)); //右侧是闭区间，区别Java
        System.out.println(jedis.getrange("s1", 0, -1)); //右侧全部用-1
        jedis.close();
    }

    @Test
    public void run07() {
        Jedis jedis = new Jedis("hadoop102",6379);
        jedis.zadd("z1", 5, "lss");
        jedis.zadd("z1", 9, "yjy");
        jedis.zadd("z1", 3, "dxy");
        jedis.zadd("z1", 8, "ly");

        System.out.println(jedis.zcard("z1"));

        jedis.zadd("z1", 9, "yjy");
        System.out.println(jedis.zcard("z1"));

        jedis.zadd("z1", 10, "yjy");
        System.out.println(jedis.zcard("z1"));

        jedis.close();
    }

    @Test
    public void run06() {
        Jedis jedis = new Jedis("hadoop102",6379);
        jedis.hset("h1", "name", "lss");
        jedis.hset("h1", "age", "18");
        jedis.hset("h1", "gender", "girl");

        System.out.println(jedis.hgetAll("h1"));

        jedis.close();
    }

    @Test
    public void run05() {
        Jedis jedis = new Jedis("hadoop102",6379);
        jedis.sadd("s1", "lss", "yjy", "lxy", "dxy");
        System.out.println(jedis.smembers("s1"));
        jedis.close();
    }

    @Test
    public void run04() {
        Jedis jedis = new Jedis("hadoop102",6379);
        jedis.flushDB();
        jedis.set("k1", "v1");
        jedis.get("k1");
        // KISS 重命名key
        jedis.rename("k1", "k2");
        System.out.println(jedis.get("k2"));
        System.out.println(jedis.get("k1"));
        jedis.close();
    }

    @Test
    public void run03() {
        Jedis jedis = new Jedis("hadoop102",6379);
        // KISS 你看，其实你可以根据返回值1或0来判断之前有没有这个key，去重时就可以利用这个技巧哦
        System.out.println(jedis.setnx("s1", "18")); // 1
        System.out.println(jedis.setnx("s1", "16")); // 0
        jedis.close();
    }

    public static void main(String[] args) {
        Jedis jedis = RedisUtil.getJedis();
        jedis.set("k1000", "v1000");

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
