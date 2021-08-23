package com.cs.rfq.decorator;

import com.google.gson.Gson;
import com.google.gson.reflect.TypeToken;

import com.google.gson.JsonObject;
import com.google.gson.JsonParser;
import com.google.gson.stream.JsonReader;


import java.io.Serializable;
import java.lang.reflect.Type;
import java.util.Map;

public class Rfq implements Serializable {
    private String id;
    private String isin;
    private Long traderId;
    private Long entityId;
    private Long quantity;
    private Double price;
    private String side;

    public static Rfq fromJson(String json) {
        //TODO: build a new RFQ setting all fields from data passed in the RFQ json message

        Rfq createRfqFromJson = new Rfq();
        
        JsonObject jsonObject = (new JsonParser()).parse(json).getAsJsonObject();

        createRfqFromJson.setId(jsonObject.has("id") ? jsonObject.get("id").getAsString() : null);
        createRfqFromJson.setEntityId(jsonObject.has("entityId") ? jsonObject.get("entityId").getAsLong() : null);
        createRfqFromJson.setIsin(jsonObject.has("instrumentId") ? jsonObject.get("instrumentId").getAsString() : null);
        createRfqFromJson.setQuantity(jsonObject.has("qty") ? jsonObject.get("qty").getAsLong() : null);
        createRfqFromJson.setPrice(jsonObject.has("price") ? jsonObject.get("price").getAsDouble() : null);
        createRfqFromJson.setSide(jsonObject.has("side") ? jsonObject.get("side").getAsString() : null);
        createRfqFromJson.setTraderId(jsonObject.has("traderId") ? jsonObject.get("traderId").getAsLong() : null);
        
        return createRfqFromJson;

    }

    @Override
    public String toString() {
        return "Rfq{" +
                "id='" + id + '\'' +
                ", isin='" + isin + '\'' +
                ", traderId=" + traderId +
                ", entityId=" + entityId +
                ", quantity=" + quantity +
                ", price=" + price +
                ", side=" + side +
                '}';
    }

    public boolean isBuySide() {
        return "B".equals(side);
    }

    public boolean isSellSide() {
        return "S".equals(side);
    }

    public String getId() {
        return id;
    }

    public void setId(String id) {
        this.id = id;
    }

    public String getIsin() {
        return isin;
    }

    public void setIsin(String isin) {
        this.isin = isin;
    }

    public Long getTraderId() {
        return traderId;
    }

    public void setTraderId(Long traderId) {
        this.traderId = traderId;
    }

    public Long getEntityId() {
        return entityId;
    }

    public void setEntityId(Long entityId) {
        this.entityId = entityId;
    }

    public Long getQuantity() {
        return quantity;
    }

    public void setQuantity(Long quantity) {
        this.quantity = quantity;
    }

    public Double getPrice() {
        return price;
    }

    public void setPrice(Double price) {
        this.price = price;
    }

    public String getSide() {
        return side;
    }

    public void setSide(String side) {
        this.side = side;
    }
}
