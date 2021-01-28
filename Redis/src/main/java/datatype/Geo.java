package datatype;

import utils.RedisUtil;import org.junit.jupiter.api.Test;
import redis.clients.jedis.GeoRadiusResponse;
import redis.clients.jedis.GeoUnit;
import redis.clients.jedis.Jedis;
import redis.clients.jedis.params.GeoRadiusParam;

import java.util.List;

public class Geo {
    @Test
    public void run02() {
        Jedis jedis = RedisUtil.getJedis();
        jedis.del("cars");
        // The first argument is the set we’re adding to, the second is the longitude, third is the latitude and the fourth is the member name
        jedis.geoadd("cars", -115.17087, 36.12306, "my-car");
        // update my-car
        jedis.geoadd("cars", -115.17172, 36.12196, "my-car");
        // add a second car
        jedis.geoadd("cars", -115.171971, 36.120609, "robins-car");
        // Distance
        System.out.println(jedis.geodist("cars", "my-car", "robins-car", GeoUnit.M));

        List<GeoRadiusResponse> results1 = jedis.georadius("cars", -115.17258, 36.11996, 500, GeoUnit.M);
        for (GeoRadiusResponse result : results1) {
            System.out.println(result.getMemberByString());
        }

        List<GeoRadiusResponse> results2 = jedis.georadiusByMember("cars", "robins-car", 500, GeoUnit.M, GeoRadiusParam.geoRadiusParam().withDist());
        for (GeoRadiusResponse result : results2) {
            System.out.println(result.getMemberByString());
        }
        jedis.close();
    }

    @Test
    public void run01() {
        Jedis jedis = RedisUtil.getJedis();
        jedis.geoadd("china:city", 116.38, 39.90, "天安门");
        jedis.geoadd("china:city", 116.42, 39.93, "东城区");
        jedis.geoadd("china:city", 116.37, 39.92, "西城区");
        jedis.geoadd("china:city", 116.30, 39.95, "海淀区");
        jedis.geoadd("china:city", 116.35, 39.87, "宣武区");
        jedis.geoadd("china:city", 116.28, 39.85, "丰台区");

        System.out.println(jedis.geopos("china:city", "西城区", "东城区")); //[(116.37000113725662,39.919999041618105), (116.41999751329422,39.93000105131286)]

        System.out.println(jedis.geodist("china:city", "西城区", "东城区", GeoUnit.KM));

        List<GeoRadiusResponse> georadius = jedis.georadius("china:city", 116.30, 39.95, 10, GeoUnit.KM);
        for (GeoRadiusResponse geoRadiusResponse : georadius) {
            System.out.println(geoRadiusResponse.getMemberByString());
        }
        //宣武区
        //海淀区
        //天安门
        //西城区

        jedis.close();
    }
}
