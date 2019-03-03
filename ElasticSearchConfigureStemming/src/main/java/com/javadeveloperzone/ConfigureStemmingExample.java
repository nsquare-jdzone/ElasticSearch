package com.javadeveloperzone;

import org.elasticsearch.action.admin.indices.create.CreateIndexRequest;
import org.elasticsearch.action.admin.indices.create.CreateIndexResponse;
import org.elasticsearch.action.admin.indices.settings.get.GetSettingsRequest;
import org.elasticsearch.action.admin.indices.settings.get.GetSettingsResponse;
import org.elasticsearch.client.transport.TransportClient;
import org.elasticsearch.common.Strings;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.transport.TransportAddress;
import org.elasticsearch.common.xcontent.XContentType;
import org.elasticsearch.transport.client.PreBuiltTransportClient;

import java.io.IOException;
import java.net.InetAddress;
import java.util.concurrent.ExecutionException;

import static org.elasticsearch.common.xcontent.XContentFactory.jsonBuilder;

public class ConfigureStemmingExample {

    TransportClient client = null;
    String indexName,indexTypeName;

    public ConfigureStemmingExample(){
        indexName = "englishstemsnalyzer";
        indexTypeName = "englishstemsnalyzer";
    }
    public static void main(String[] args) {
        ConfigureStemmingExample configureStemmingExample = new ConfigureStemmingExample();
        try {
            configureStemmingExample.initESTransportClient(); //init transport client

            configureStemmingExample.createSettingsWithEnglishStemAnalyzer();
//            configureStemmingExample.createSettingsWithAnalyzerJSONSource();
            configureStemmingExample.getSettingsWithAnalyzer();

        }catch (Exception e){
            e.printStackTrace();
        }finally {
            configureStemmingExample.closeTransportClient(); //close transport client
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

    public void createSettingsWithEnglishStemAnalyzer() throws ExecutionException, InterruptedException, IOException {
        CreateIndexRequest request = new CreateIndexRequest(indexName);
        request.settings(Settings.builder()
                .put("index.max_inner_result_window", 250)
                .put("index.write.wait_for_active_shards", 1)
                .put("index.query.default_field", "paragraph")
                .put("index.number_of_shards", 3)
                .put("index.number_of_replicas", 2)
                .loadFromSource(Strings.toString(jsonBuilder()
                        .startObject()
                           .startObject("analysis")
                                .startObject("filter")
                                    .startObject("english_stemmer")
                                    .field("type","stemmer")
                                    .field("name", "english")
                                    .endObject()
                                .endObject()
                                .startObject("analyzer")
                                    .startObject("EnglishStopWordAnalyzer")
                                        .field("tokenizer", "standard")
                                        .field("filter", new String[]{"lowercase","english_stemmer"})
                                    .endObject()
                                .endObject()
                            .endObject()
                        .endObject()), XContentType.JSON)
        );
        CreateIndexResponse createIndexResponse = client.admin().indices().create(request).get();
        System.out.println("Index : "+createIndexResponse.index()+" Created");
        getSettingsWithAnalyzer();
    }

    public void createSettingsWithAnalyzerJSONSource() throws ExecutionException, InterruptedException, IOException {
        CreateIndexRequest request = new CreateIndexRequest(indexName);


        String jsonSource = "{\n" +
                "        \"settings\":{\n" +
                "            \"index\": {\n" +
                "                \"analysis\": {\n" +
                "                    \"analyzer\": {\n" +
                "                        \"EnglishStopWordAnalyzer\": {\n" +
                "                            \"tokenizer\": \"standard\",\n" +
                "                            \"filter\" : [\n" +
                "                                \"lowercase\",\n" +
                "                                \"english_stemmer\"\n" +
                "                            ]\n" +
                "                        }\n" +
                "                    },\n" +
                "                    \"filter\" : {\n" +
                "                        \"english_stemmer\": {\n" +
                "                            \"type\": \"stemmer\",\n" +
                "                            \"name\": \"english\",\n" +
                "                        }\n" +
                "                    }\n" +
                "                }\n" +
                "            }\n" +
                "        }\n" +
                "    }";
        request.source(jsonSource,XContentType.JSON);
        CreateIndexResponse createIndexResponse = client.admin().indices().create(request).get();
        System.out.println("Index : "+createIndexResponse.index() + " Created.");
        getSettingsWithAnalyzer();
    }

    public void getSettingsWithAnalyzer() throws ExecutionException, InterruptedException {
        System.out.println("***************Get Settings with Analyzers *********************");
        GetSettingsRequest getSettingsRequest = new GetSettingsRequest();
        GetSettingsResponse indexResponse = client.admin().indices().getSettings(getSettingsRequest).get();
        Settings settings = indexResponse.getIndexToSettings().get(indexName);

        for(String key : settings.keySet()){
            System.out.println(key+" : "+settings.get(key));
        }
    }

    public void closeTransportClient(){
        if(client!=null){
            client.close();
        }
    }

}
