package com.proyect.kafka.producers;

import java.util.Properties;
import java.util.concurrent.ExecutionException;

import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.Producer;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class KafkaTopicProducer {
	
	public static final Logger log = LoggerFactory.getLogger(KafkaTopicProducer.class);
	
	public static void main(String[] args) {
		long startTime = System.currentTimeMillis();
		Properties props=new Properties();
		props.put("bootstrap.servers","localhost:9092"); //Broker de kafka al que nos vamos a conectar
		props.put("acks","all");
		props.put("key.serializer","org.apache.kafka.common.serialization.StringSerializer");
		props.put("value.serializer","org.apache.kafka.common.serialization.StringSerializer");
		
		try(Producer<String,String> producer = new KafkaProducer(props);){
			for(int i=0; i<1000; i++) {
				//.get() lo realiza de manera sincrona mientras que sin el get es asincrono donde todo lo envia sin esperar que uno termine para mandar los demas.
				producer.send(new ProducerRecord<String,String>("javotopic-topic", String.valueOf(i), "valor de topic")).get();
			}
			producer.flush();
		} catch (InterruptedException | ExecutionException e) {
			log.error("Message producer interrupted ",e);
		}
		log.info("Processing time ={} ms ", (System.currentTimeMillis() - startTime));
	}
}
