package ch.jbaumann.stockstream.data;

import java.util.TimerTask;

public class PeriodicStockDataLoader extends TimerTask {
	private StockDataImporter sdi;
	private String[] tickerList;

	public PeriodicStockDataLoader(StockDataImporter sdi, String[] tickerList) {
		this.delay = delay;
		this.sdi = sdi;
		this.tickerList = tickerList;
	}

	@Override
	public void run() {
		for (String ticker : tickerList) {
			sdi.sendDataToKafka(ticker);
		}
	}
}
