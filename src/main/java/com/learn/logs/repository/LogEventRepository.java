package com.learn.logs.repository;

import com.learn.logs.model.LogEvent;
import org.springframework.data.elasticsearch.repository.ElasticsearchRepository;
import org.springframework.stereotype.Repository;

@Repository
public interface LogEventRepository extends ElasticsearchRepository<LogEvent, String> {
}
