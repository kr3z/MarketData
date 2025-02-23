ALTER TABLE Exchange 
RENAME COLUMN `update_time` TO  `update_stamp`,
MODIFY COLUMN  `update_count` int(11) NOT NULL,
DROP COLUMN `hash`,
ADD COLUMN `create_stamp` datetime DEFAULT NULL;

UPDATE Exchange set create_stamp=update_stamp;
commit;

ALTER TABLE Exchange 
MODIFY COLUMN `update_stamp` datetime NOT NULL,
MODIFY COLUMN `create_stamp` datetime NOT NULL;

ALTER TABLE Symbol 
RENAME COLUMN `update_time` TO `update_stamp`,
RENAME COLUMN `FinnHubSymbol` TO `finnhub_symbol`;

ALTER TABLE Symbol 
ADD COLUMN `exchange_key` int(11) DEFAULT NULL AFTER `id`,
ADD COLUMN `isin` varchar(30) DEFAULT NULL AFTER `mic`,
ADD COLUMN `last_finnhub_quote_check` datetime DEFAULT NULL AFTER `finnhub_symbol`,
ADD COLUMN `last_yahoo_quote_check` date DEFAULT NULL AFTER `last_finnhub_quote_check`,
ADD COLUMN `create_stamp` datetime DEFAULT NULL AFTER `uid`,
RENAME COLUMN `displaySymbol` TO `display_symbol`,
RENAME COLUMN `shareClassFIGI` TO `share_class_figi`,
RENAME COLUMN `type` TO `symbol_type`,
MODIFY COLUMN `finnhub_symbol` tinyint(1) NOT NULL DEFAULT 0,
MODIFY COLUMN `update_stamp` datetime NOT NULL,
MODIFY COLUMN `update_count` int(11) NOT NULL,
DROP COLUMN `hash`;

UPDATE Symbol s INNER JOIN Exchange e on s.mic = e.mic
set s.exchange_key=e.id;

UPDATE Symbol set create_stamp=update_stamp;

commit;

ALTER TABLE Symbol 
MODIFY COLUMN `exchange_key` int(11) NOT NULL,
MODIFY COLUMN `create_stamp` datetime NOT NULL;

CREATE UNIQUE INDEX `ux_Symbol_symbol_exchange` ON Symbol(`symbol`,`exchange_key`);

ALTER TABLE `YahooHistoricalData` RENAME TO `YahooQuote`;
DROP INDEX `symbol_key_quote_date` on YahooQuote;
CREATE UNIQUE INDEX `ux_YahooQuote_symbol_date` on YahooQuote(`symbol_key`,`quote_date`);


UPDATE Symbol s 
INNER JOIN (
    SELECT symbol_key,max(quote_time) as last_quote
    FROM FinnHubQuote GROUP BY symbol_key
) latest_finnhub_quote on s.id = latest_finnhub_quote.symbol_key
set s.last_finnhub_quote_check=latest_finnhub_quote.last_quote;

UPDATE Symbol s 
INNER JOIN (
    SELECT symbol_key,max(quote_date) as last_quote
    FROM YahooQuote GROUP BY symbol_key
) latest_yahoo_quote on s.id = latest_yahoo_quote.symbol_key
set s.last_yahoo_quote_check=latest_yahoo_quote.last_quote;

commit;