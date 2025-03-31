from FinnHubData import FinnHubClientWrapper

finnhub_client: FinnHubClientWrapper = FinnHubClientWrapper()
finnhub_client.update_stock_symbols()
finnhub_client.update_quotes()