package com.javadeveloperzone;

import org.elasticsearch.action.bulk.BulkItemResponse;
import org.elasticsearch.action.bulk.BulkRequestBuilder;
import org.elasticsearch.action.bulk.BulkResponse;
import org.elasticsearch.action.search.SearchResponse;
import org.elasticsearch.client.transport.TransportClient;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.transport.TransportAddress;
import org.elasticsearch.common.xcontent.XContentBuilder;
import org.elasticsearch.transport.client.PreBuiltTransportClient;

import java.io.IOException;
import java.net.InetAddress;
import java.sql.Connection;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.Iterator;
import java.util.concurrent.ExecutionException;

import static org.elasticsearch.common.xcontent.XContentFactory.jsonBuilder;

public class IndexDocumentDatabase {

    String indexName,indexTypeName;
    TransportClient client = null;
    DatabaseHelper databaseHelper = null;

    public static final String DOC_LANGUAGE = "docLanguage";
    public static final String DOC_TITLE = "docTitle";
    public static final String DOC_AUTHOR = "docAuthor";
    public static final String DOC_TYPE = "docType";
    public static final String DOC_ID = "docId";

    public static void main(String[] args) {
        IndexDocumentDatabase indexDocumentDatabase = new IndexDocumentDatabase();
        indexDocumentDatabase.index();
    }

    public void index(){

        try {
            initEStransportClinet(); //init transport client

            databaseHelper.openMySqlDbConnection(); //open MySQL database connection
            databaseBulkImport(); //fetch data from database and send to elastic search
            //using bulk import
            //update records
            databaseHelper.updateRecords();

            refreshIndices(); //refresh indices

            search(); //search indexed document
        }catch (Exception e){
            e.printStackTrace();
        }finally {
            databaseHelper.closeMySqlDbConnection();
            closeTransportClient(); //close transport client
        }
    }

    public IndexDocumentDatabase(){
        indexName = "indexdatabaseexamaple";
        indexTypeName = "indexdatabasemapping";
        databaseHelper = new DatabaseHelper();
    }

    /*
    Method used to init Elastic Search Transprt client,
    Return true if it is successfully initialized otherwise false
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


    public void databaseBulkImport() throws IOException, ExecutionException, InterruptedException, SQLException {


        BulkRequestBuilder bulkRequest = client.prepareBulk(); //prepare bulk request
        int count=0,noOfBatch=1;
        int numberOfRecords = 1;
        Connection connection = databaseHelper.getConnection();
        Statement statement = connection.createStatement();
        String query = "SELECT d.docId," +
                "d.docType," +
                "d.docTitle," +
                "d.docAuthor," +
                "d.docLanguage," +
                "d.numberOfPage " +
                "FROM document d where lastIndexDate>'2019-02-01 00:00:00'";
        ResultSet resultSet =
                statement.executeQuery(query);

        while (resultSet.next()){ //next json array element
            try {
                XContentBuilder xContentBuilder = jsonBuilder()
                        .startObject()
                        .field(DOC_TYPE, resultSet.getString(DOC_TYPE))
                        .field(DOC_AUTHOR, resultSet.getString(DOC_AUTHOR))
                        .field(DOC_TITLE, resultSet.getString(DOC_TITLE))
                        .field(DOC_LANGUAGE, resultSet.getString(DOC_LANGUAGE))
                        .endObject();

                bulkRequest.add(client.prepareIndex(indexName, indexTypeName, resultSet.getString(DOC_ID))
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
        if(count!=0){ //add remaining documents to ES
            addDocumentToESCluser(bulkRequest,noOfBatch,count);
        }
        resultSet.close();
        statement.close();
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
