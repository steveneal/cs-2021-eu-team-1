package com.cs.rfq.decorator.extractors;

import com.cs.rfq.decorator.Rfq;
import com.cs.rfq.decorator.TradeDataLoader;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.time.DateTimeException;
import java.util.Map;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;

public class TotalTradesWithEntityExtractorTest extends AbstractSparkUnitTest{
    private Rfq rfq;
    Dataset<Row> trades;
    AverageTradedPriceExtractor avgTradedPrice;
    TotalTradesWithEntityExtractor extractor;

    @BeforeEach
    public void setup() {
        rfq = new Rfq();
        rfq.setIsin("AT0000A0VRQ6");
        rfq.setEntityId(5561279226039690843L);

        String filePath = getClass().getResource("volume-traded-1.json").getPath();
        trades = new TradeDataLoader().loadTrades(session, filePath);
        extractor = new TotalTradesWithEntityExtractor();
    }


    @Test
    public void testTradesWithEntityToday() {

        extractor.setSince("2018-06-09");
        Map<RfqMetadataFieldNames, Object> map = extractor.extractMetaData(rfq, session, trades);
        Object result = map.get(RfqMetadataFieldNames.tradesWithEntityToday);

        assertEquals(3L, result);
    }

    @Test
    public void testTradesWithEntityLastWeek() {

        extractor.setSince("2018-06-10");
        Map<RfqMetadataFieldNames, Object> map = extractor.extractMetaData(rfq, session, trades);
        Object result = map.get(RfqMetadataFieldNames.tradesWithEntityPastWeek);

        assertEquals(5L, result);
    }

    @Test
    public void testTradesWithEntityLastYear() {

        extractor.setSince("2018-12-31");
        Map<RfqMetadataFieldNames, Object> map = extractor.extractMetaData(rfq, session, trades);
        Object result = map.get(RfqMetadataFieldNames.tradesWithEntityPastYear);

        assertEquals(5L, result);
    }


    @Test
    public void testSetSinceException() {
        TotalTradesWithEntityExtractor extractor = new TotalTradesWithEntityExtractor();
        assertThrows(DateTimeException.class, () -> extractor.setSince("2-1-1"));
    }
}
