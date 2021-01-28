package utils;

import redis.clients.jedis.*;

import java.util.HashSet;
import java.util.Set;

public class RedisUtil {
    /**
     * Jedis连接池
     */
    private static JedisPool jedisPool = null;
    public static Jedis getJedis(){
        if(jedisPool == null){
            JedisPoolConfig jedisPoolConfig = new JedisPoolConfig();
            jedisPoolConfig.setMaxTotal(100);
            jedisPoolConfig.setMinIdle(4);
            jedisPoolConfig.setMaxIdle(4);
            //资源耗尽时等待
            jedisPoolConfig.setBlockWhenExhausted(true);
            jedisPoolConfig.setMaxWaitMillis(5000);
            /**
             * 从池中去连接后要进行测试，因为连接池中的连接可能坏掉（还在，但不好用了）：
             * 1 服务器端重启过
             * 2 网断过
             * 3 服务器端维持空闲连接超时
             *
             * 如果不设置，经常因为外界因素造成莫名其妙的数据丢失问题，很奇葩。你只能管理自己，没法管理别人
             */
            jedisPoolConfig.setTestOnBorrow(true);
            /**
             * 空闲时偶尔测一下
             */
            jedisPoolConfig.setTestWhileIdle(true);
            jedisPool = new JedisPool(jedisPoolConfig, "hadoop102", 6379);
        }
        return jedisPool.getResource();
    }

    /**
     * 使用
     * Jedis jedis = RedisUtil.getJedis();
     * String pong = jedis.ping();
     * jedis.close();
     *
     * 所以为什么这里用close关闭呢？Jedis重写了close()，要是你是new的，它就直接关闭；要是从连接池得到的，它会返回给连接池
     */

    /**
     * Jedis哨兵池
     */
    private static JedisSentinelPool jedisSentinelPool = null;
    public static Jedis getJedisFromSentinel(){
        if(jedisSentinelPool ==null){
            //创建哨兵池
            Set<String> sentinels = new HashSet<>();
            // 这里是测试时的，如果你有多个节点都有哨兵，全部add
            // 注意这个端口号
            sentinels.add("192.168.11.101:26379");

            JedisPoolConfig jedisPoolConfig = new JedisPoolConfig();
            jedisPoolConfig.setMaxTotal(100);
            jedisPoolConfig.setMinIdle(20);
            jedisPoolConfig.setMaxIdle(30);
            //资源耗尽时等待
            jedisPoolConfig.setBlockWhenExhausted(true);
            jedisPoolConfig.setMaxWaitMillis(5000);
            //从池中去连接后要进行测试
            //导致连接池中的连接坏掉： 1 服务器端重启过 2 网断过 3 服务器端维持空闲连接超时
            jedisPoolConfig.setTestOnBorrow(true);

            jedisSentinelPool = new JedisSentinelPool("mymaster",sentinels,jedisPoolConfig);
        }
        return jedisSentinelPool.getResource();
    }

    /**
     * Redis集群模式
     */
    private static JedisCluster jedisCluster = null;
    public static JedisCluster getJedisCluster() {
        if (jedisCluster == null) {
            Set<HostAndPort> nodes = new HashSet<>();
            nodes.add(new HostAndPort("", 6390));
            nodes.add(new HostAndPort("", 6391));
            JedisPoolConfig jedisPoolConfig = new JedisPoolConfig();
            jedisPoolConfig.setMaxTotal(100);
            jedisPoolConfig.setMinIdle(4);
            jedisPoolConfig.setMaxIdle(4);
            //资源耗尽时等待
            jedisPoolConfig.setBlockWhenExhausted(true);
            jedisPoolConfig.setMaxWaitMillis(5000);
            /**
             * 从池中去连接后要进行测试，因为连接池中的连接可能坏掉（还在，但不好用了）：
             * 1 服务器端重启过
             * 2 网断过
             * 3 服务器端维持空闲连接超时
             *
             * 如果不设置，经常因为外界因素造成莫名其妙的数据丢失问题，很奇葩。你只能管理自己，没法管理别人
             */
            jedisPoolConfig.setTestOnBorrow(true);
            jedisCluster = new JedisCluster(nodes, jedisPoolConfig);
        }
        return jedisCluster;
    }
}
