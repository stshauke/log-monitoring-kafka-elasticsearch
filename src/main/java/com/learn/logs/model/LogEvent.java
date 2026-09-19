package com.learn.logs.model;

import lombok.AllArgsConstructor;
import lombok.Data;
import lombok.NoArgsConstructor;
import org.springframework.data.annotation.Id;
import org.springframework.data.elasticsearch.annotations.Document;
import org.springframework.data.elasticsearch.annotations.Field;
import org.springframework.data.elasticsearch.annotations.FieldType;

import java.time.Instant;

@Data
@NoArgsConstructor
@AllArgsConstructor
@Document(indexName = "app-logs")
public class LogEvent {

    @Id
    private String id;

    @Field(type = FieldType.Keyword)
    private String service;

    @Field(type = FieldType.Keyword)
    private String endpoint;

    @Field(type = FieldType.Keyword)
    private String httpMethod;

    @Field(type = FieldType.Integer)
    private Integer statusCode;

    @Field(type = FieldType.Long)
    private Long responseTimeMs;

    @Field(type = FieldType.Keyword)
    private String level; // INFO, WARN, ERROR

    @Field(type = FieldType.Keyword)
    private String traceId;

    @Field(type = FieldType.Date)
    private Instant timestamp;
}
