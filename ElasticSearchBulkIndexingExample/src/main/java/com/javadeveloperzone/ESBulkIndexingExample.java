package com.javadeveloperzone;

import org.elasticsearch.action.admin.indices.create.CreateIndexResponse;
import org.elasticsearch.action.admin.indices.mapping.put.PutMappingResponse;
import org.elasticsearch.action.bulk.BulkItemResponse;
import org.elasticsearch.action.bulk.BulkRequestBuilder;
import org.elasticsearch.action.bulk.BulkResponse;
import org.elasticsearch.action.search.SearchResponse;
import org.elasticsearch.client.transport.TransportClient;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.transport.TransportAddress;
import org.elasticsearch.common.xcontent.XContentBuilder;
import org.elasticsearch.transport.client.PreBuiltTransportClient;

import java.io.BufferedReader;
import java.io.File;
import java.io.FileReader;
import java.io.IOException;
import java.net.InetAddress;
import java.util.Iterator;

import java.util.concurrent.ExecutionException;

import static org.elasticsearch.common.xcontent.XContentFactory.jsonBuilder;

/**
 * Created by JavaDeveloperZone on 6/16/2018.
 */
public class ESBulkIndexingExample {


    String indexName,indexTypeName;
    TransportClient client = null;

    public static void main(String[] args) {
        ESBulkIndexingExample esExample = new ESBulkIndexingExample();
        try {
            esExample.initEStransportClinet(); //init transport client

            esExample.CSVbulkImport(true); //index multiple  document

            esExample.refreshIndices();

            esExample.search(); //search indexed document
        }catch (Exception e){
            e.printStackTrace();
        }finally {
            esExample.closeTransportClient(); //close transport client
        }
    }


    public ESBulkIndexingExample(){
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
                    .addTransportAddress(new TransportAddress(InetAddress.getByName("localhost"), 9300));

            return true;
        } catch (Exception ex) {
            //log.error("Exception occurred while getting Client : " + ex, ex);
            ex.printStackTrace();
            return false;
        }
    }


    public void CSVbulkImport(boolean isHeaderIncluded) throws IOException, ExecutionException, InterruptedException {

        BulkRequestBuilder bulkRequest = client.prepareBulk();

        File file = new File("G:\\study\\Blogs\\Elastic Search\\Bulk Indexing\\Input.csv");
        BufferedReader bufferedReader = new BufferedReader(new FileReader(file));
        String line = null;
        int count=0,noOfBatch=1;
        if(bufferedReader!=null && isHeaderIncluded){
            bufferedReader.readLine();//ignore first line
        }
        while ((line = bufferedReader.readLine())!=null){

            if(line.trim().length()==0){
                continue;
            }
            String data [] = line.split(",");
            if(data.length==5){

                try {
                    XContentBuilder xContentBuilder = jsonBuilder()
                            .startObject()
                            .field("docType", data[1])
                            .field("docTitle", data[2])
                            .field("docPage", data[3])
                            .endObject();

                    bulkRequest.add(client.prepareIndex(indexName, indexTypeName, data[0])
                            .setSource(xContentBuilder));

                    if ((count+1) % 500 == 0) {
                        count = 0;
                        addDocumentToESCluser(bulkRequest, noOfBatch, count);
                        noOfBatch++;
                    }
                }catch (Exception e) {
                    e.printStackTrace();
                    //skip records if wrong date in input file
                }
            }else{
                System.out.println("Invalid data : "+line);
            }
            count++;
        }
        bufferedReader.close();
        addDocumentToESCluser(bulkRequest,noOfBatch,count);


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
