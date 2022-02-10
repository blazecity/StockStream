package ch.jbaumann.stockstream.data;

import java.util.Date;
import java.util.Timer;
import java.util.TimerTask;

public class Main {
	public static void main(String[] args) {
		if (args.length == 3) {
			String brokerUrl = args[0];
			String apiKey = args[1];
			String kafkaTopic = args[2];
			System.out.println(brokerUrl + apiKey + kafkaTopic);
			StockDataImporter sdi = new StockDataImporter(brokerUrl, apiKey, kafkaTopic);
			String[] tickerList = {
				"AMZN",
				"MSFT",
				"NEE",
				"JPM",
				"MRNA"
			};

			PeriodicStockDataLoader psdl = new PeriodicStockDataLoader(sdi, tickerList);
			Timer timer = new Timer(true);
			timer.scheduleAtFixedRate(psdl, 0, 900_000);
		}


	}
}
