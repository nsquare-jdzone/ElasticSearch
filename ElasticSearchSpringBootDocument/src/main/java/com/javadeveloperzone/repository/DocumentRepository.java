package com.javadeveloperzone.repository;

import java.util.List;

import com.javadeveloperzone.model.Document;
import org.springframework.data.elasticsearch.repository.ElasticsearchRepository;


public interface DocumentRepository extends ElasticsearchRepository<Document, String> {
	List<Document> findByDocTitleEndsWith(String name);
	List<Document> findByDocTitleStartsWith(String name);
	List<Document> findByDocTypeEndsWith(String name);
	List<Document> findByDocTypeStartsWith(String name);
}
