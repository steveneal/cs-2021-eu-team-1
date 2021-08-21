package com.cs.rfq.decorator;

import org.junit.jupiter.api.Test;

import static org.junit.jupiter.api.Assertions.*;
import static org.junit.jupiter.api.Assertions.assertFalse;

public class RFQTest2 {
    @Test
    public void testJSonFactoryMethod() {
        String validRfqJson = "{" +
                "'id': 'testing456', " +
                "'traderId': 3351266293154445953, " +
                "'entityId': 937543875620125834, " +
                "'instrumentId': 'AT0000383864', " +
                "'qty': 250000, " +
                "'price': 1.58, " +
                "'side': 'S' " +
                "}";

        Rfq rfq = Rfq.fromJson(validRfqJson);

        assertAll(
                () -> assertNotEquals("123ABC", rfq.getId()),
                () -> assertEquals((Long) 3351266293154445953L, rfq.getTraderId()),
                () -> assertEquals((Long) 937543875620125834L, rfq.getEntityId()),
                () -> assertNotEquals("AT0000383833", rfq.getIsin()),
                () -> assertEquals((Long) 250000L, rfq.getQuantity()),
                () -> assertEquals((Double) 1.58, rfq.getPrice()),
                () -> assertEquals("S", rfq.getSide()),
                () -> assertFalse(rfq.isBuySide()),
                () -> assertTrue(rfq.isSellSide())
        );

    }
}
