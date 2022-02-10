package ch.jbaumann.stockstream.data;

import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.Producer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.clients.producer.ProducerRecord;


import java.net.http.HttpClient;
import java.net.http.HttpRequest;
import java.net.http.HttpResponse;
import java.net.URI;
import java.util.Properties;
import java.util.logging.Level;
import java.util.logging.Logger;

public class StockDataImporter {
	private String apiKey;
	private String brokerUrl;
	private String kafkaTopic;
	private Producer<String, String> kafkaProducer;

	private final static Logger LOGGER = Logger.getLogger(StockDataImporter.class.getName());

	public StockDataImporter(String apiKey, String brokerUrl, String kafkaTopic) {
		this.apiKey = apiKey;
		this.brokerUrl = brokerUrl;
		this.kafkaTopic = kafkaTopic;

		Properties config = new Properties();
		config.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, this.brokerUrl);

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

		client.sendAsync(request, HttpResponse.BodyHandlers.ofString())
				.thenApply(HttpResponse::body)
				.thenAccept(payload -> writeToKafka(ticker, payload))
				.join();
	}

	private void writeToKafka(String ticker, String payload) {
		// key should be ticker + time
		ProducerRecord<String, String> priceRecord = new ProducerRecord<>(this.kafkaTopic, ticker, payload);
		StringBuilder stringBuilder = new StringBuilder();
		LOGGER.setLevel(Level.INFO);
		this.kafkaProducer.send(priceRecord);
		LOGGER.info(stringBuilder.append("Sent price data of ").append(ticker).append(" from time ").append("").toString());
	}
}
