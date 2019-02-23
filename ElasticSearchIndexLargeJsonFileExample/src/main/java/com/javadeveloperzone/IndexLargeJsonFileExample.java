package com.javadeveloperzone;

import com.google.gson.Gson;
import com.google.gson.GsonBuilder;
import com.google.gson.stream.JsonReader;
import org.elasticsearch.action.bulk.BulkItemResponse;
import org.elasticsearch.action.bulk.BulkRequestBuilder;
import org.elasticsearch.action.bulk.BulkResponse;
import org.elasticsearch.action.search.SearchResponse;
import org.elasticsearch.client.transport.TransportClient;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.transport.TransportAddress;
import org.elasticsearch.common.xcontent.XContentBuilder;
import org.elasticsearch.transport.client.PreBuiltTransportClient;

import java.io.*;
import java.net.InetAddress;
import java.nio.charset.StandardCharsets;
import java.util.Iterator;
import java.util.concurrent.ExecutionException;

import static org.elasticsearch.common.xcontent.XContentFactory.jsonBuilder;


public class IndexLargeJsonFileExample {


    String indexName,indexTypeName;
    TransportClient client = null;

    public static final String DOC_LANGUAGE = "docLanguage";
    public static final String PARENT_DOC_ID = "parentDocId";
    public static final String DOC_TITLE = "docTitle";
    public static final String IS_PARENT = "isParent";
    public static final String DOC_AUTHOR = "docAuthor";
    public static final String DOC_TYPE = "docType";


    public static void main(String[] args) {
        IndexLargeJsonFileExample esExample = new IndexLargeJsonFileExample();
        try {
            esExample.initEStransportClinet(); //init transport client

            esExample.JsonBulkImport(); //index multiple  document

            esExample.refreshIndices(); //refresh indices

            esExample.search(); //search indexed document
        }catch (Exception e){
            e.printStackTrace();
        }finally {
            esExample.closeTransportClient(); //close transport client
        }
    }


    public IndexLargeJsonFileExample(){
        indexName = "document";
        indexTypeName = "bulkindexing";
    }

    /*
    Method used to init Elastic Search Transprt client,
    Return true if it is succesfully intialized otherwise false
     */
    public boolean initEStransportClinet()
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


    public void JsonBulkImport() throws IOException, ExecutionException, InterruptedException {

        BulkRequestBuilder bulkRequest = client.prepareBulk();

        File jsonFilePath = new File("H:\\Work\\Data\\sample.json");
        int count=0,noOfBatch=1;

        //initialize jsonReader class by passing reader
        JsonReader jsonReader = new JsonReader(
                new InputStreamReader(
                        new FileInputStream(jsonFilePath), StandardCharsets.UTF_8));

        Gson gson = new GsonBuilder().create();

        jsonReader.beginArray(); //start of json array
        int numberOfRecords = 1;
        while (jsonReader.hasNext()){ //next json array element
            Document document = gson.fromJson(jsonReader, Document.class);
            //do something real
            try {
                XContentBuilder xContentBuilder = jsonBuilder()
                        .startObject()
                        .field(DOC_TYPE, document.getDocType())
                        .field(DOC_AUTHOR, document.getDocAuthor())
                        .field(DOC_TITLE, document.getDocTitle())
                        .field(IS_PARENT, document.isParent())
                        .field(PARENT_DOC_ID, document.getParentDocId())
                        .field(DOC_LANGUAGE, document.getDocLanguage())
                        .endObject();

                bulkRequest.add(client.prepareIndex(indexName, indexTypeName, String.valueOf(numberOfRecords))
                        .setSource(xContentBuilder));

                if (count==50_000) {
                    addDocumentToESCluser(bulkRequest, noOfBatch, count);
                    noOfBatch++;
                    count = 0;
                }
            }catch (Exception e) {
                e.printStackTrace();
                //skip records if wrong date in input file
            }
            numberOfRecords++;
            count++;
        }
        jsonReader.endArray();
        if(count!=0){ //add remaining documents to ES
            addDocumentToESCluser(bulkRequest,noOfBatch,count);
        }
        System.out.println("Total Document Indexed : "+numberOfRecords);
    }

    public void addDocumentToESCluser(BulkRequestBuilder bulkRequest,int noOfBatch,int count){

        if(count==0){
            //org.elasticsearch.action.ActionRequestValidationException: Validation Failed: 1: no requests added;
            return;
        }
        BulkResponse bulkResponse = bulkRequest.execute().actionGet();
        if (bulkResponse.hasFailures()) {
            System.out.println("Bulk Indexing failed for Batch : "+noOfBatch);

            // process failures by iterating through each bulk response item
            int numberOfDocFailed = 0;
            Iterator<BulkItemResponse> iterator = bulkResponse.iterator();
            while (iterator.hasNext()){
                BulkItemResponse response = iterator.next();
                if(response.isFailed()){
                    //System.out.println("Failed Id : "+response.getId());
                    numberOfDocFailed++;
                }
            }
            System.out.println("Out of "+count+" documents, "+numberOfDocFailed+" documents failed");
            System.out.println(bulkResponse.buildFailureMessage());
        }else{
            System.out.println("Bulk Indexing Completed for batch : "+noOfBatch);
        }
    }


    public void refreshIndices(){
        client.admin().indices()
                .prepareRefresh(indexName)
                .get(); //Refresh before search, so you will get latest indices result
    }

    public void search(){

        SearchResponse response = client.prepareSearch(indexName)
                .setTypes(indexTypeName)
                .get();
        //MatchAllDocQuery
        System.out.println("Total Hits : "+response.getHits().getTotalHits());
        System.out.println(response);
    }

    public void closeTransportClient(){
        if(client!=null){
            client.close();
        }
    }
}
