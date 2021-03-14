package project6

import ca.dataedu.kafka.ProducerPlayground.producer

import java.util.Properties
import io.confluent.kafka.serializers.KafkaAvroSerializer
import org.apache.avro.generic.{GenericRecord, GenericRecordBuilder}
import io.confluent.kafka.schemaregistry.client.CachedSchemaRegistryClient
import org.apache.avro.generic.{GenericRecord, GenericRecordBuilder}
import org.apache.kafka.clients.consumer.{ConsumerConfig, ConsumerRecord}
import org.apache.kafka.common.serialization.StringDeserializer
import org.apache.spark.streaming.dstream.DStream
import org.apache.spark.streaming.kafka010.{ConsumerStrategies, KafkaUtils, LocationStrategies}
import org.apache.kafka.clients.producer.{KafkaProducer, ProducerConfig, ProducerRecord}
import org.apache.kafka.common.serialization.StringSerializer
import org.apache.spark.rdd.RDD

object SparkFinalSprint extends App with Main{

  val filename = "hdfs://172.16.129.58:8020/user/hive/warehouse/winter2020_jay.db/enriched_station_information/"

 // val enriched_station = sc.textFile(filename).map(Enrich_station.fromCsv).keyBy(_.shortname)
 // val srClient = new CachedSchemaRegistryClient("http://172.16.129.58:8081", 1)
  //val metadata = srClient.getSchemaMetadata("enriched_trip-jay", 1)
  //val movieSchema = srClient.getByID(metadata.getId)
  val kafkaConfig: Map[String, String] = Map[String, String](
    ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG -> "localhost:9092",
    ConsumerConfig.GROUP_ID_CONFIG -> "ja",
    ConsumerConfig.AUTO_OFFSET_RESET_CONFIG -> "earliest",
    ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG -> classOf[StringDeserializer].getName,
    ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG -> classOf[StringDeserializer].getName,
    ConsumerConfig.ENABLE_AUTO_COMMIT_CONFIG -> "false"
  )
  val topic = "trip"
  val inStream: DStream[ConsumerRecord[String, String]] = KafkaUtils.createDirectStream[String, String](
    ssc,
    LocationStrategies.PreferConsistent,
    ConsumerStrategies.Subscribe[String, String](List(topic), kafkaConfig))

 val outputTri = inStream.map(_.value()).foreachRDD(x=>{
   val enriched_trip=x.map(TripStream.fromCsv)
   enriched_trip.take(10).foreach(println)
 })

   /* val m = enriched_trip.collect().toList
    val avroMovies:List[GenericRecord] =m.map{ x =>
      val fields = x.split(",")
      new GenericRecordBuilder(movieSchema)
        .set("start_date", fields(0))
        .set("start_station_code", fields(1).toInt)
        .set("end_date", fields(2))
        .set("end_station_code",  fields(3).toInt)
        .set("duration_sec", fields(4).toInt)
        .set("is_member", fields(5).toInt)
        .set("system_id", fields(6))
        .set("timezone",  fields(7))
        .set("station_id", fields(8).toInt)
        .set("name", fields(9))
        .set("short_name", fields(10))
        .set("lat",  fields(11).toDouble)
        .set("lon", fields(12).toDouble)
        .set("capacity", fields(13).toInt)
        .build()
    }

    val producerProperties = new Properties()
    producerProperties.setProperty(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, "localhost:9092")
    producerProperties.setProperty(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG,
      classOf[StringSerializer].getName)
    producerProperties.setProperty(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG,
      classOf[KafkaAvroSerializer].getName)
    producerProperties.setProperty("schema.registry.url", "http://172.16.129.58:8081")

    val producer = new KafkaProducer[String, GenericRecord](producerProperties)
    avroMovies.map(avroMessage => new ProducerRecord[String, GenericRecord]
    ("enriched_trip",avroMessage.get("station_id").toString, avroMessage)).foreach(producer.send)

  //  producer.flush() /} */
  ssc.start()
  ssc.awaitTermination()
  ssc.stop(stopSparkContext = true,stopGracefully = true)

}
