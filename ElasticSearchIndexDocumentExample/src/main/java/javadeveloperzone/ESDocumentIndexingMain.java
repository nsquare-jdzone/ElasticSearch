package javadeveloperzone;

import javadeveloperzone.pojo.Document;
import org.elasticsearch.action.index.IndexResponse;
import org.elasticsearch.action.search.SearchResponse;
import org.elasticsearch.client.transport.TransportClient;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.transport.TransportAddress;
import org.elasticsearch.transport.client.PreBuiltTransportClient;
import java.net.InetAddress;
import java.util.Date;

import static org.elasticsearch.common.xcontent.XContentFactory.jsonBuilder;

/**
 * Created by JavaDeveloperZone on 6/16/2018.
 */
public class ESDocumentIndexingMain {

    TransportClient client = null;

    public static void main(String[] args) {
        ESDocumentIndexingMain esExample = new ESDocumentIndexingMain();
        try {
            esExample.initEStransportClinet(); //init transport client

            esExample.indexDocument(); //index one document

            esExample.refreshIndices(); //refresh indices

            esExample.search(); //search indexed document
        }catch (Exception e){
            e.printStackTrace();
        }finally {
            esExample.closeTransportClient(); //close transport client
        }


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

    private boolean indexDocument(Document document){
        try {
            IndexResponse response = client.prepareIndex("document", "document", String.valueOf(document.getDocId()))
                    .setSource(jsonBuilder()
                            .startObject()
                            .field("docTitle", document.getDocTitle())
                            .field("docPage", document.getPage())
                            .field("docType", document.getDocType())
                            .field("docModifiedDate",document.getModifiedDate())
                            .endObject()
                    )
                    .get();

            if(response!=null){
                System.out.println(response.toString());
            }else{
                return false;
            }
            return true;
        }catch (Exception e){
            return false;
        }
    }

    public void indexDocument(){
        Document document = new Document();
        document.setDocId(1);
        document.setDocTitle("Elastic Search Indexing Example");
        document.setDocType("PDF");
        document.setModifiedDate(new Date());
        document.setPage(2);
        boolean isIndexed = this.indexDocument(document);

        if(isIndexed){
            //further actions like change index status in DB,
            // send notification etc..
        }
    }

    public void refreshIndices(){
        client.admin().indices()
                .prepareRefresh("document")
                .get(); //Refresh before search, so you will get latest indices result
    }
    public void search(){

        SearchResponse response = client.prepareSearch("document")
                .setTypes("document")
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
