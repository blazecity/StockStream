package ch.jbaumann.stockstream.data;

public class Main {
	public static void main(String[] args) {

		if (args.length == 3) {
			String brokerUrl = args[0];
			String apiKey = args[1];
			String kafkaTopic = args[2];
			StockDataImporter sdi = new StockDataImporter(brokerUrl, apiKey, kafkaTopic);
			sdi.sendDataToKafka("AAPL");
		}
	}
}
