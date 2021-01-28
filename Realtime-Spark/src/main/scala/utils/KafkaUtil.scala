package utils

import java.util.Properties

import org.apache.kafka.clients.consumer.ConsumerRecord
import org.apache.kafka.common.TopicPartition
import org.apache.kafka.common.serialization.StringDeserializer
import org.apache.spark.streaming.StreamingContext
import org.apache.spark.streaming.dstream.InputDStream
import org.apache.spark.streaming.kafka010.{ConsumerStrategies, KafkaUtils, LocationStrategies}

object KafkaUtil {
    // Kafka消费者配置
    val kafkaParam = collection.mutable.Map(
        "bootstrap.servers" -> PropertiesUtil.load().getProperty("kafka.broker.list"),//用于初始化链接到集群的地址
        "key.deserializer" -> classOf[StringDeserializer],
        "value.deserializer" -> classOf[StringDeserializer],
        //用于标识这个消费者属于哪个消费团体
        ConstantUtil.groupId -> "gmall2021_group",
        // KISS latest自动重置偏移量为最新的偏移量
        "auto.offset.reset" -> "latest",
        // KISS 如果是true，则这个消费者的偏移量会在后台自动提交,但是kafka宕机容易丢失数据
        // KISS 如果是false，会需要手动维护kafka偏移量
        "enable.auto.commit" -> (true: java.lang.Boolean)
    )

    /**
     * 下面是重载的方法
     */

    // 创建DStream，返回接收到的输入数据 从默认auto.offset位置读取数据
    def getKafkaStream(topic: String, ssc: StreamingContext, groupId: String)
    : InputDStream[ConsumerRecord[String, String]] = {
        kafkaParam(ConstantUtil.groupId) = groupId
        // Kafka和SparkStreaming的直连模式：Kafka4个分区对应SparkStreaming4个线程
        val dStream = KafkaUtils.createDirectStream[String, String](
            ssc,
            LocationStrategies.PreferConsistent, // 一般就选这个
            ConsumerStrategies.Subscribe[String, String](Array(topic), kafkaParam))
        dStream
    }

    // 创建DStream，返回接收到的输入数据 从指定偏移位置读取数据
    def getKafkaStream(topic: String, ssc: StreamingContext, offsets: Map[TopicPartition, Long], groupId: String)
    : InputDStream[ConsumerRecord[String, String]] = {
        kafkaParam(ConstantUtil.groupId) = groupId
        val dStream = KafkaUtils.createDirectStream[String, String](
            ssc,
            LocationStrategies.PreferConsistent, // 一般就选这个
            ConsumerStrategies.Subscribe[String, String](Array(topic), kafkaParam, offsets))
        dStream
    }
}
