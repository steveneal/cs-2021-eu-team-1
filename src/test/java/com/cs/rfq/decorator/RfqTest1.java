package com.cs.rfq.decorator;

import org.junit.jupiter.api.Test;

import static org.junit.jupiter.api.Assertions.*;
import static org.junit.jupiter.api.Assertions.assertFalse;

public class RfqTest1 {

    @Test
    public void testJSonFactoryMethod() {
        String validRfqJson = "{" +
                "'id': '678HTG', " +
                "'traderId': 678235678019827653, " +
                "'entityId': 986509265427651c874, " +
                "'instrumentId': 'AT0000A0VRQ6', " +
                "'qty': 50000, " +
                "'price': 2.14, " +
                "'side': 'B' " +
                "}";

        Rfq rfq = Rfq.fromJson(validRfqJson);

        assertAll(
                () -> assertEquals("678HTG", rfq.getId()),
                () -> assertEquals((Long) 678235678019827653L, rfq.getTraderId()),
                () -> assertEquals((Long) 986509265427651874L, rfq.getEntityId()),
                () -> assertEquals("AT0000A0VRQ6", rfq.getIsin()),
                () -> assertEquals((Long) 50000L, rfq.getQuantity()),
                () -> assertEquals((Double) 2.14, rfq.getPrice()),
                () -> assertEquals("B", rfq.getSide()),
                () -> assertTrue(rfq.isBuySide()),
                () -> assertFalse(rfq.isSellSide())
        );

    }
}
