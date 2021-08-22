package com.cs.rfq.decorator.extractors;

import com.cs.rfq.decorator.Rfq;
import com.cs.rfq.decorator.TradeDataLoader;
import jdk.nashorn.internal.runtime.regexp.joni.exception.ValueException;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;


import java.util.Map;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;


public class TradeSideBiasExtractorTest extends AbstractSparkUnitTest {


    private Rfq rfq;
    Dataset<Row> trades;

    @BeforeEach
    public void setup() {
        rfq = new Rfq();
        rfq.setIsin("AT0000A0VRQ6");
        rfq.setEntityId(5561279226039690841L);

        String filePath = getClass().getResource("volume-traded-3.json").getPath();
        trades = new TradeDataLoader().loadTrades(session, filePath);
    }


    @Test
    public void test() {
        TradeSideBiasExtractor extractor = new TradeSideBiasExtractor();

        extractor.setSince("2018-08-01");
        Map<RfqMetadataFieldNames, Object> map = extractor.extractMetaData(rfq, session, trades);
        Object result = map.get(RfqMetadataFieldNames.buySellRatioPastWeek);

        assertEquals("500000/-1", result);
    }

    @Test
    public void testSetSinceException() {
        TradeSideBiasExtractor extractor = new TradeSideBiasExtractor();
        assertThrows(ValueException.class, () -> extractor.setSince("2-1-1"));
    }

}

