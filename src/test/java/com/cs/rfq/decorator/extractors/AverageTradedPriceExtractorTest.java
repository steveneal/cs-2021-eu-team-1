package com.cs.rfq.decorator.extractors;

import com.cs.rfq.decorator.Rfq;
import com.cs.rfq.decorator.TradeDataLoader;
import jdk.nashorn.internal.runtime.regexp.joni.exception.ValueException;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import sun.awt.SunHints;

import java.time.DateTimeException;
import java.util.Map;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;


public class AverageTradedPriceExtractorTest extends AbstractSparkUnitTest {

    private Rfq rfq;
    Dataset<Row> trades;
    AverageTradedPriceExtractor avgTradedPrice;

    @BeforeEach
    public void setup() {
        rfq = new Rfq();
        rfq.setIsin("AT0000A0VRQ6");

        String filePath = getClass().getResource("volume-traded-3.json").getPath();
        trades = new TradeDataLoader().loadTrades(session, filePath);
        avgTradedPrice = new AverageTradedPriceExtractor();
    }


    @Test
    public void testAveragePriceForAllGivenInstrumentLastWeek() {

        avgTradedPrice.setSince("2021-08-01");
        Map<RfqMetadataFieldNames, Object> map = avgTradedPrice.extractMetaData(rfq, session, trades);
        Object result = map.get(RfqMetadataFieldNames.averageTradedPricePastWeek);

        assertEquals(139.857, result);
    }


    @Test
    public void testAveragePriceForInstrumentNotPreviouslyTraded() {
        avgTradedPrice.setSince("2021-08-01");
        rfq.setIsin("xx");
        Map<RfqMetadataFieldNames, Object> map = avgTradedPrice.extractMetaData(rfq, session, trades);
        Object result = map.get(RfqMetadataFieldNames.averageTradedPricePastWeek);

        assertEquals(0.0, result);
    }


    @Test
    public void testAveragePriceWhenOnlyOneRecordIsMatched() {
        avgTradedPrice.setSince("2021-08-11");

        Map<RfqMetadataFieldNames, Object> map = avgTradedPrice.extractMetaData(rfq, session, trades);
        Object result = map.get(RfqMetadataFieldNames.averageTradedPricePastWeek);

        assertEquals(139.648, result);
    }


    @Test
    public void testSetSinceException() {
        AverageTradedPriceExtractor avgTradedPrice = new AverageTradedPriceExtractor();
        assertThrows(DateTimeException.class, () -> avgTradedPrice.setSince("2-1-1"));
    }

}

