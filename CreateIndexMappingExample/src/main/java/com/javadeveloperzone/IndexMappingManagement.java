package com.javadeveloperzone;

import org.elasticsearch.action.ActionFuture;
import org.elasticsearch.action.admin.indices.create.CreateIndexRequest;
import org.elasticsearch.action.admin.indices.create.CreateIndexResponse;
import org.elasticsearch.action.admin.indices.mapping.get.GetFieldMappingsRequest;
import org.elasticsearch.action.admin.indices.mapping.get.GetFieldMappingsResponse;
import org.elasticsearch.action.admin.indices.mapping.get.GetMappingsRequest;
import org.elasticsearch.action.admin.indices.mapping.get.GetMappingsResponse;
import org.elasticsearch.action.admin.indices.mapping.put.PutMappingRequest;
import org.elasticsearch.action.support.master.AcknowledgedResponse;
import org.elasticsearch.client.transport.TransportClient;
import org.elasticsearch.cluster.metadata.MappingMetaData;
import org.elasticsearch.common.collect.ImmutableOpenMap;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.transport.TransportAddress;
import org.elasticsearch.common.xcontent.XContentBuilder;
import org.elasticsearch.common.xcontent.XContentFactory;
import org.elasticsearch.common.xcontent.XContentType;
import org.elasticsearch.transport.client.PreBuiltTransportClient;

import java.io.IOException;
import java.net.InetAddress;
import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.ExecutionException;


public class IndexMappingManagement {

    TransportClient client = null;
    String indexName,indexTypeName;

    public static void main(String[] args) {
        IndexMappingManagement manageSettings = new IndexMappingManagement();
        try {
            manageSettings.initESTransportClient(); //init transport client
//            manageSettings.createIndex();

//            manageSettings.putMappingJSONSoure();
//            manageSettings.putMappingNestedDataTypeJSONSoure();
//              manageSettings.putMappingUsingMap();
//            manageSettings.putMappingXContentBuilder();
//            manageSettings.getMapping();
//            manageSettings.getFieldMapping();

//            manageSettings.updateMapping();
//            manageSettings.getMapping();
            manageSettings.getFieldMapping();
            manageSettings.refreshIndices(); //refresh indices

        }catch (Exception e){
            e.printStackTrace();
        }finally {
            manageSettings.closeTransportClient(); //close transport client
        }
    }

    /*
    Method used to init Elastic Search Transprt client,
    Return true if it is succesfully intialized otherwise false
     */
    public boolean initESTransportClient()
    {
        try {
            // un-command this, if you have multiple node
            client = new PreBuiltTransportClient(Settings.EMPTY)
                    .addTransportAddress(
                            new TransportAddress(InetAddress.getByName("localhost"), 9300));

            return true;
        } catch (Exception ex) {
            //log.error("Exception occurred while getting Client : " + ex, ex);
            ex.printStackTrace();
            return false;
        }
    }

    public IndexMappingManagement(){
        indexName = "putmappingexample";
        indexTypeName = "blogs";
    }

    public void createIndex(){
        CreateIndexRequest request = new CreateIndexRequest(indexName);
        request.settings(Settings.builder()
                .put("index.max_inner_result_window", 250)
                .put("index.write.wait_for_active_shards", 1)
                .put("index.query.default_field", "paragraph")
                .put("index.number_of_shards", 3)
                .put("index.number_of_replicas", 2)
        );
        CreateIndexResponse createIndexResponse = client.admin().indices().create(request).actionGet();
        System.out.println("Index Created : "+createIndexResponse.index());
    }

    public void putMappingJSONSoure() throws ExecutionException, InterruptedException {

        PutMappingRequest putMappingRequest = new PutMappingRequest(indexName);
        putMappingRequest.type(indexTypeName)
                .source("{\n" +
                        "      \"properties\": {\n" +
                        "        \"blogId\": {\n" +
                        "          \"type\": \"integer\"\n" +
                        "        },\n" +
                        "        \"isGuestPost\": {\n" +
                        "          \"type\": \"boolean\"\n" +
                        "        },\n" +
                        "        \"voteCount\": {\n" +
                        "          \"type\": \"integer\"\n" +
                        "        },\n" +
                        "        \"createdAt\": {\n" +
                        "          \"type\": \"date\"\n" +
                        "        }\n" +
                        "      }\n" +
                        "}",XContentType.JSON);

        AcknowledgedResponse acknowledgedResponse = client.admin().indices().putMapping(putMappingRequest).get();
        System.out.println("Put Mapping response : " + acknowledgedResponse.isAcknowledged());

        getMapping();
    }

    public void putMappingNestedDataTypeJSONSoure() throws ExecutionException, InterruptedException {

        PutMappingRequest putMappingRequest = new PutMappingRequest(indexName);
        putMappingRequest.type(indexTypeName)
                .source("{\n" +
                        "      \"properties\": {\n" +
                        "        \"blogId\": {\n" +
                        "          \"type\": \"integer\"\n" +
                        "        },\n" +
                        "\t\t\"rewards\": {\n" +
                        "          \"type\": \"nested\",\n" +
                        "          \"properties\": {\n" +
                        "            \"username\": {\n" +
                        "              \"type\": \"keyword\"\n" +
                        "            },\n" +
                        "            \"comments\": {\n" +
                        "              \"type\": \"text\"\n" +
                        "            }\n" +
                        "          }\n" +
                        "        }\n" +
                        "      }\n" +
                        "}",XContentType.JSON);

        AcknowledgedResponse acknowledgedResponse = client.admin().indices().putMapping(putMappingRequest).get();
        System.out.println("Put Mapping response : " + acknowledgedResponse.isAcknowledged());

        getMapping();
    }



    public void putMappingMap() {

    }

    public void putMappingXContentBuilder() throws IOException, ExecutionException, InterruptedException {
        PutMappingRequest putMappingRequest = new PutMappingRequest(indexName);

        XContentBuilder builder = XContentFactory.jsonBuilder();
        builder.startObject();
        {
            builder.startObject("properties");
            {
                builder.startObject("blogId");{
                    builder.field("type", "integer");
                }
                builder.endObject();
                builder.startObject("isGuestPost");{
                    builder.field("type", "boolean");
                }
                builder.endObject();
                builder.startObject("voteCount");{
                    builder.field("type", "integer");
                }
                builder.endObject();
            }
            builder.endObject();
        }
        builder.endObject();
        putMappingRequest.type(indexTypeName)
                .source(builder);

        AcknowledgedResponse acknowledgedResponse = client.admin().indices().putMapping(putMappingRequest).get();
        System.out.println("Put Mapping response : " + acknowledgedResponse.isAcknowledged());

    }

    public void putMappingUsingMap() throws ExecutionException, InterruptedException {
        PutMappingRequest putMappingRequest = new PutMappingRequest(indexName);
        Map<String, Object> properties = new HashMap<>();
        Map<String, Object> blogId = new HashMap<>(); //create HashMap for each field
        blogId.put("type", "integer");
        properties.put("blogId", blogId);

        Map<String, Object> isGuestPost = new HashMap<>();
        isGuestPost.put("type", "boolean");
        properties.put("isGuestPost", isGuestPost);

        Map<String, Object> voteCount = new HashMap<>();
        voteCount.put("type", "integer");
        properties.put("voteCount", voteCount);

        Map<String, Object> mappingMap = new HashMap<>();
        mappingMap.put("properties", properties);

        putMappingRequest.type(indexTypeName).source(mappingMap);
        AcknowledgedResponse acknowledgedResponse = client.admin().indices().putMapping(putMappingRequest).get();
        System.out.println("Put Mapping response : " + acknowledgedResponse.isAcknowledged());

    }

    public void getMapping() {
        GetMappingsRequest getMappingsRequest = new GetMappingsRequest();
        getMappingsRequest.indices(indexName);
        GetMappingsResponse getMappingsResponse = client.admin().indices().getMappings(getMappingsRequest).actionGet();
        ImmutableOpenMap<String, MappingMetaData> mapping = getMappingsResponse.mappings().get(indexName);
        Map<String, Object> sourceAsMap = mapping.get(indexTypeName).getSourceAsMap();

        sourceAsMap.entrySet().forEach((entry ->{
            System.out.println("Name : "+entry.getKey());
            if(entry.getValue() instanceof Map){

                ((Map<String,Object>)entry.getValue()).entrySet().forEach(s-> System.out.println(s.getKey()+"=>"+s.getValue()));
            }
        }));
    }

    public void getFieldMapping() throws ExecutionException, InterruptedException {

        GetFieldMappingsRequest request = new GetFieldMappingsRequest();
        request.indices(indexName);
        request.fields("blogId");
        GetFieldMappingsResponse getFieldMappingsResponse = client.admin().indices().getFieldMappings(request).get();
        GetFieldMappingsResponse.FieldMappingMetaData blogId = getFieldMappingsResponse.mappings().get(indexName).get(indexTypeName).get("blogId");
        System.out.println("Field Full Name : "+blogId.fullName());
        System.out.println("Field Hash Code : "+blogId.hashCode());

    }

    public void updateMapping() {
    }

    public void refreshIndices(){

        client.admin().indices()
                .prepareRefresh(indexName)
                .get(); //Refresh before search, so you will get latest indices result
    }

    public void closeTransportClient(){
        if(client!=null){
            client.close();
        }
    }
}
