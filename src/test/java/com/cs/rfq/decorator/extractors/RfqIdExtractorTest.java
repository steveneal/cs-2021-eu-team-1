package com.cs.rfq.decorator.extractors;

import com.cs.rfq.decorator.Rfq;
import com.cs.rfq.decorator.TradeDataLoader;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.util.Map;

import static org.junit.jupiter.api.Assertions.assertEquals;

public class RfqIdExtractorTest extends AbstractSparkUnitTest {
    private Rfq rfq;
    Dataset<Row> trades;
    RfqIdExtractor extractor;

    @BeforeEach
    public void setup() {
        rfq = new Rfq();
        rfq.setIsin("AT0000A0VRQ6");
        rfq.setId("SomeID");

        String filePath = getClass().getResource("volume-traded-3.json").getPath();
        trades = new TradeDataLoader().loadTrades(session, filePath);
        extractor = new RfqIdExtractor();
    }

    @Test
    public void testIfIdMatches() {

        Map<RfqMetadataFieldNames, Object> map = extractor.extractMetaData(rfq, session, trades);
        Object result = map.get(RfqMetadataFieldNames.rfqId);

        assertEquals("SomeID", result);
    }
}
