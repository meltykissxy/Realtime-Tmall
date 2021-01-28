package newredis;

import utils.RedisUtil;
import org.junit.jupiter.api.Test;
import redis.clients.jedis.Jedis;

// 初始化连接
public class RedisNew {
    // 直接new
    @Test
    public void run01() {
        Jedis jedis = new Jedis("hadoop102",6379);
        String pong = jedis.ping();
        System.out.println("连接成功："+pong);
        jedis.close();
    }

    // 连接池
    @Test
    public void run02() {
        Jedis jedis = RedisUtil.getJedis();
        String pong = jedis.ping();
        System.out.println("连接成功："+pong);
        jedis.close();
        // 所以为什么这里用close关闭呢？Jedis重写了close()，要是你是new的，它就直接关闭；要是从连接池得到的，它会返回给连接池
    }

    /**
     * 密码配置（protected-mode为no时，bind和requirepass都注释掉；protected-mode为yes时，bind和requirepass必须设一个）
     * 69行，bind 127.0.0.1
     * 88行，protected-mode no
     * 789行，requirepass <密码>
     * 如果你设置了密码
     * Jedis
     * Jedis jedis = new Jedis("hadoop102",6379);
     * jedis.auth(<密码>);
     * 命令行
     * 需要在Redis命令行中：AUTH <密码>
     */
    @Test
    public void run03() {
        Jedis jedis = RedisUtil.getJedis();
        jedis.auth("PASSWORD");
        String pong = jedis.ping();
        System.out.println("连接成功："+pong);
        jedis.close();
    }
}
