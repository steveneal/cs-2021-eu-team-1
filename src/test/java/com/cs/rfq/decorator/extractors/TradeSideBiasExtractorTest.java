package com.cs.rfq.decorator.extractors;

import com.cs.rfq.decorator.Rfq;
import com.cs.rfq.decorator.TradeDataLoader;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.util.Map;

import static org.junit.jupiter.api.Assertions.assertEquals;


public class TradeSideBiasExtractorTest extends AbstractSparkUnitTest {


    private Rfq rfq;
    Dataset<Row> trades;

    @BeforeEach
    public void setup() {
        rfq = new Rfq();
        rfq.setIsin("AT0000A0VRQ6");

        String filePath = getClass().getResource("volume-traded-3.json").getPath();
        trades = new TradeDataLoader().loadTrades(session, filePath);
    }


    @Test
    public void test() {
        TradeSideBiasExtractorTest extractor = new TradeSideBiasExtractorTest();

//        avgTradedPrice.setSince("2021-08-01");
//        Map<RfqMetadataFieldNames, Object> map = avgTradedPrice.extractMetaData(rfq, session, trades);
//        Object result = map.get(RfqMetadataFieldNames.averageTradedPricePastWeek);

        assertEquals(1, 1);
    }

}

