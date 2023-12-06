package com.example.youtubetrending;

import com.google.gson.Gson;
import com.google.gson.reflect.TypeToken;
import com.google.gson.stream.JsonReader;
import com.opencsv.CSVReader;
import com.opencsv.CSVReaderBuilder;
import com.opencsv.exceptions.CsvException;
import netscape.javascript.JSObject;
import org.json.JSONObject;

import java.io.FileNotFoundException;
import java.io.FileReader;
import java.io.IOException;
import java.lang.reflect.Type;
import java.nio.file.Files;
import java.nio.file.Paths;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

public class YouTubeTrendingApplication {

    public static void main(String[] args) throws IOException, CsvException {

        //input - paths to dataset
        String csvFilePath = "D:\\Fakultet\\Dataset\\USvideos.csv";
        String categoryJSON = "D:\\Fakultet\\Dataset\\US_category_id.json";

        //output - path to exported JSON
        String jsonFilePath = "C:\\Users\\Administrator\\Documents\\NoSQL Project JSON\\youtube_trending.json";

        convertToJson(csvFilePath,categoryJSON, jsonFilePath);

    }

    public static void convertToJson(String csvPath, String categoryJsonPath, String jsonPath) throws IOException, CsvException {
        CSVReader reader = new CSVReaderBuilder(new FileReader(csvPath)).build();
        List<String[]> lines = reader.readAll();

        String[] header = lines.get(0);
        lines.remove(0);



    }

    //video_id,trending_date,title,channel_title,category_id,publish_time,tags,views,likes,dislikes,comment_count,thumbnail_link,comments_disabled,ratings_disabled,video_error_or_removed,description

    //convert one line in one JSON object
    public JSONObject lineToObject(String[] line, String[] header){
        JSONObject trendingVideo = new JSONObject();

        for(int i = 0; i < line.length; i++){

        }

        return trendingVideo;
    }

    //get category JSON from categoryID
    public JSONObject categoryIdToObject(int categoryId, String categoryJsonPath) throws IOException {
        String json = new String(Files.readAllBytes(Paths.get(categoryJsonPath)));

        JSONObject categoryWithId = new JSONObject();

        Gson gson = new Gson();

        //convert whole JSON file to map
        Type type = new TypeToken<Map<String, Object>>(){}.getType();
        Map<String, Object> map = gson.fromJson(json, type);

        System.out.println(map);

        //take only the list of items
        List<?> list = (List<?>) map.get("items");

        //iterate trough all items
        for(Object obj : list){

            //create map for each object
            Map<String, Object> category = gson.fromJson(gson.toJson(obj), type);
            String currentKey = (String) category.get("id");

            //check id the ID is the one we need if yes create JSON object we need to return
            if(Integer.parseInt(currentKey) == categoryId){
                categoryWithId = new JSONObject(category);
            }
        }
        return categoryWithId;
    }
}
