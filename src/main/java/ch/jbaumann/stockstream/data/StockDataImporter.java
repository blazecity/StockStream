package ch.jbaumann.stockstream.data;

import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.Producer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.serialization.StringSerializer;


import java.net.http.HttpClient;
import java.net.http.HttpRequest;
import java.net.http.HttpResponse;
import java.net.URI;
import java.util.Properties;

public class StockDataImporter {
	private String apiKey;
	private String brokerUrl;
	private String kafkaTopic;
	private Producer<String, String> kafkaProducer;

	public StockDataImporter(String brokerUrl, String apiKey, String kafkaTopic) {
		this.apiKey = apiKey;
		this.brokerUrl = brokerUrl;
		this.kafkaTopic = kafkaTopic;

		Properties config = new Properties();
		config.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, this.brokerUrl);
		config.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class);
		config.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, StringSerializer.class);

		this.kafkaProducer = new KafkaProducer<>(config);
	}

	public void sendDataToKafka(final String ticker) {
		StringBuilder stringBuilder = new StringBuilder();
		stringBuilder.append("https://finnhub.io/api/v1/quote?symbol=").append(ticker).append("&token=").append(this.apiKey);

		HttpClient client = HttpClient.newHttpClient();
		HttpRequest request = HttpRequest.newBuilder()
				.uri(URI.create(stringBuilder.toString()))
				.header("accept", "application/json")
				.build();

		System.out.println("Sending request");

		client.sendAsync(request, HttpResponse.BodyHandlers.ofString())
				.thenApply(HttpResponse::body)
				.thenAccept(payload -> writeToKafka(ticker, payload))
				.join();
	}

	private void writeToKafka(String ticker, String payload) {
		System.out.println("Now writing to Kafka");
		// key should be ticker + time
		ProducerRecord<String, String> priceRecord = new ProducerRecord<>(this.kafkaTopic, ticker, payload);
		StringBuilder stringBuilder = new StringBuilder();
		this.kafkaProducer.send(priceRecord);
		System.out.println(stringBuilder.append("Sent price data of ").append(ticker).append(" from time ").append(""));
	}
}
