package com.cs.rfq.decorator.publishers;

import com.cs.rfq.decorator.extractors.RfqMetadataFieldNames;
import com.google.gson.GsonBuilder;

import java.io.IOException;
import java.io.PrintStream;
import java.net.Socket;
import java.net.UnknownHostException;
import java.util.Map;


/***
 * simple naive socket writer
 *
 */
public class MetaDataJsonSocketPublisher implements MetadataPublisher {
    @Override
    public void publishMetadata(Map<RfqMetadataFieldNames, Object> metadata) {
        try (Socket socket = new Socket("localhost", 9001)) {
            PrintStream oStream = new PrintStream(socket.getOutputStream());
            String s = new GsonBuilder().setPrettyPrinting().create().toJson(metadata);
            oStream.println(s);

            //log.info(String.format("Publishing metadata:%n%s", s));
        } catch (UnknownHostException e) {
            e.printStackTrace();
        } catch (IOException e) {
            e.printStackTrace();
        }
    }
}
