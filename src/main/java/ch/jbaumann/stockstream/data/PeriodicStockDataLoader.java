package ch.jbaumann.stockstream.data;

public class PeriodicStockDataLoader implements Runnable {
	private StockDataImporter sdi;
	private String[] tickerList;

	public PeriodicStockDataLoader(StockDataImporter sdi, String[] tickerList) {
		this.sdi = sdi;
		this.tickerList = tickerList;
	}

	@Override
	public void run() {
		System.out.println("=> Now running data load");
		for (String ticker : tickerList) {
			sdi.sendDataToKafka(ticker);
		}
	}
}
