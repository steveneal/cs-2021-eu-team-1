package com.cs.rfq.decorator.publishers;

import com.cs.rfq.decorator.extractors.RfqMetadataFieldNames;

import java.io.Serializable;
import java.util.Map;

/**
 * Simple interface for different metadata publishers to implement
 */
public interface MetadataPublisher extends Serializable {
    void publishMetadata(Map<RfqMetadataFieldNames, Object> metadata);
}
