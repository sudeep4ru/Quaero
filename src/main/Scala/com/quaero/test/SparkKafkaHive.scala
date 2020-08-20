package com.quaero.test

import org.apache.spark.rdd.RDD
import org.apache.spark.sql.SparkSession

import org.apache.spark.{ SparkConf, SparkContext, _}
import org.apache.spark.sql.{DataFrame, SQLContext, SparkSession}
import org.apache.kafka.common.serialization.StringDeserializer
import java.time.Instant
import org.apache.kafka.common.serialization.StringDeserializer
import org.apache.spark.streaming.kafka010.LocationStrategies.PreferConsistent
import org.apache.spark.streaming.kafka010._
import org.apache.spark.sql.functions.col


import org.apache.kafka.clients.admin.{AdminClient,NewTopic,AdminClientConfig}
import org.apache.kafka.clients.consumer.KafkaConsumer
import org.apache.kafka.common.TopicPartition

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Properties
import java.time.Instant
import collection.JavaConverters._
import collection.JavaConversions._
import scala.util.parsing.json._
import scala.util.Try

object SparkKafkaHive {

  import org.slf4j.LoggerFactory

  val APP_NAME = "SparkKafkaHive"
  //val LOG: Nothing = LoggerFactory.getLogger(SparkKafkaHive.getClass)
  def main(args:Array[String]): Unit = {

    val inputTopic = "customer"
    val kafkaBrokers = "localhost:9092"

    val conf = new SparkConf().setAppName(APP_NAME)
    val sc = new SparkContext(conf)
    val sparkSession = SparkSession.builder.config(conf)
                        .config("hive.metastore.uris","thrift://localhost:9093")
                        .enableHiveSupport()
                        .appName(APP_NAME).getOrCreate()
    val sqlContext = sparkSession.sqlContext
    import sparkSession.implicits._

    val gid = APP_NAME + Instant.now.getEpochSecond

    val kafkaParams = scala.collection.immutable.Map[String, Object](
      "bootstrap.servers" -> kafkaBrokers,
      "key.deserializer" -> classOf[StringDeserializer],
      "value.deserializer" -> classOf[StringDeserializer],
      "group.id" -> gid,
      "auto.offset.reset" -> "earliest",
      "enable.auto.commit" -> (false: java.lang.Boolean)
    ).asJava

    val KafkaConfig = new Properties()
    KafkaConfig.put(AdminClientConfig.BOOTSTRAP_SERVERS_CONFIG, kafkaBrokers)
    val adminClient = AdminClient.create(KafkaConfig)

    val consumer = new KafkaConsumer[String,String](kafkaParams)

    // Get list of all partitions of given Topic
    val topicPartitions = adminClient
      .describeTopics(List[String](inputTopic).asJava)
      .all().get().get(inputTopic).partitions()

    // Create Array of OffsetRange with topic, partition number, start & end offsets
    val offsetRanges = topicPartitions.asScala.map(x =>{
      val topicPartition = new TopicPartition(inputTopic, x.partition)
      val startOffset = consumer.beginningOffsets(List[TopicPartition](topicPartition))
        .values().asScala.toList.get(0)
      val stopOffset = consumer.endOffsets(List[TopicPartition](topicPartition))
        .values().asScala.toList.get(0)
      OffsetRange(topicPartition,
        startOffset,
        stopOffset)
    }).toArray

    // Create RDD from provided topic & offset details
    val messagesRDD = KafkaUtils.createRDD[String, String](sc, kafkaParams,
      offsetRanges, PreferConsistent)

    // Convert to DataFrame with columns "customer_id","product","category","ts"
    val customerDF = messagesRDD.map(message =>{
      val value = message.value().asInstanceOf[String]
      val key = message.key().asInstanceOf[String]
      val msgValues = JSON.parseFull(value).get.asInstanceOf[Map[String, String]]
      (msgValues("customer_id"),msgValues("attributes"),msgValues("timestamp").toLong)
    }).toDF("customer_id","attributes","timestamp")

    val customerEmailDf = sparkSession.sqlContext.sql("select * from email_mappings")

    val df_kafka = customerDF.as("kafka")
    val df_hive = customerEmailDf.as("hive")

    val joined_df = df_kafka.join(
      df_hive
      , col("kafka.customer_id") === col("hive.customer_id")
      , "inner")


    val final_result = joined_df.select(
      col("hive.email")
      , col("kafka.attributes"))

    final_result.show
    final_result
      .coalesce(1)
      .write.format("com.databricks.spark.csv")
      .option("header", "true")
      .save("customer_data.csv")

  }
}