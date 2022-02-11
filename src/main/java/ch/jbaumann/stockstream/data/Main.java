package ch.jbaumann.stockstream.data;

import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;

public class Main {
	public static void main(String[] args) {
		if (args.length == 4) {
			String brokerUrl = args[0];
			String apiKey = args[1];
			String kafkaTopic = args[2];
			int period = Integer.parseInt(args[3]);
			System.out.println(brokerUrl + apiKey + kafkaTopic);
			StockDataImporter sdi = new StockDataImporter(brokerUrl, apiKey, kafkaTopic);
			String[] tickerList = {
				"AMZN",
				"MSFT",
				"NEE",
				"JPM",
				"MRNA"
			};
			System.out.println("sdfasdfasf");

			ScheduledExecutorService executor = Executors.newScheduledThreadPool(1);
			PeriodicStockDataLoader psdl = new PeriodicStockDataLoader(sdi, tickerList);
			executor.scheduleWithFixedDelay(psdl, 0, period, TimeUnit.MINUTES);
		}
	}
}
