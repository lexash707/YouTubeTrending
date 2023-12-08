package com.example.youtubetrending;

import com.google.gson.Gson;
import com.google.gson.reflect.TypeToken;
import com.opencsv.CSVReader;
import com.opencsv.CSVReaderBuilder;
import com.opencsv.exceptions.CsvException;
import org.json.JSONArray;
import org.json.JSONObject;

import java.io.FileReader;
import java.io.FileWriter;
import java.io.IOException;
import java.lang.reflect.Type;
import java.nio.file.Files;
import java.nio.file.Paths;
import java.sql.Timestamp;
import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.time.ZonedDateTime;
import java.time.format.DateTimeFormatter;
import java.util.Date;
import java.util.List;
import java.util.Map;

public class YouTubeTrendingApplication {

    public static void main(String[] args) throws IOException, CsvException, ParseException {

        //input - paths to dataset
        String csvFilePath = "D:\\Fakultet\\Dataset\\USvideos.csv";
        String categoryJSON = "D:\\Fakultet\\Dataset\\US_category_id.json";

        //output - path to exported JSON
        String jsonFilePath = "C:\\Users\\Administrator\\Documents\\NoSQL Project JSON\\youtube_trending.json";

        convertToJson(csvFilePath,categoryJSON, jsonFilePath);

    }

    public static void convertToJson(String csvPath, String categoryJsonPath, String jsonPath) throws IOException, CsvException, ParseException {
        CSVReader reader = new CSVReaderBuilder(new FileReader(csvPath)).build();
        List<String[]> lines = reader.readAll();

        //save the header and remove it from the list
        String[] header = lines.get(0);
        lines.remove(0);

        JSONArray videos = new JSONArray();

        for(String[] line : lines){
            JSONObject jsonObject = lineToObject(line, header, categoryJsonPath);
            videos.put(jsonObject);
        }

        FileWriter writer = new FileWriter(jsonPath);
        videos.write(writer);
        writer.flush();
        writer.close();

    }

    //video_id,trending_date,title,channel_title,category_id,publish_time,tags,views,likes,dislikes,comment_count,thumbnail_link,comments_disabled,ratings_disabled,video_error_or_removed,description

    //convert one line in one JSON object
    public static JSONObject lineToObject(String[] line, String[] header, String categoryJsonPath) throws IOException, ParseException {
        JSONObject trendingVideo = new JSONObject();

        for(int i = 0; i < line.length; i++){
            String key = header[i];
            String value = line[i];

            switch (key) {
                case "category_id" ->
                        trendingVideo.put("category", categoryIdToObject(Integer.parseInt(value), categoryJsonPath));
                case "tags" -> {
                    JSONArray tags = new JSONArray(parseTags(value));
                    trendingVideo.put(key, tags);
                }
                case "views", "likes", "dislikes", "comment_count" -> trendingVideo.put(key, Integer.parseInt(value));
                case "trending_date", "publish_time" -> {
                    Timestamp parsedDate = dateParser(value, key);
                    trendingVideo.put(key, parsedDate);
                }
                default -> trendingVideo.put(key, value);
            }
        }

        return trendingVideo;
    }

    //get category JSON from categoryID
    public static JSONObject categoryIdToObject(int categoryId, String categoryJsonPath) throws IOException {
        String json = new String(Files.readAllBytes(Paths.get(categoryJsonPath)));

        JSONObject categoryWithId = new JSONObject();

        Gson gson = new Gson();

        //convert whole JSON file to map
        Type type = new TypeToken<Map<String, Object>>(){}.getType();
        Map<String, Object> map = gson.fromJson(json, type);

        System.out.println(map);

        //take only the list of items
        List<?> list = (List<?>) map.get("items");

        //iterate through all items
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

    //make an array of tags
    public static String[] parseTags(String value){
        return value.split("\\|");
    }

    public static Timestamp dateParser(String dateString, String key) throws ParseException {
        SimpleDateFormat dateFormat;
        if(key.equals("trending_date")){
            dateFormat = new SimpleDateFormat("yy.dd.mm");
            Date parsedDate = dateFormat.parse(dateString);
            return new Timestamp(parsedDate.getTime());
        }
        else {
            DateTimeFormatter formatter = DateTimeFormatter.ISO_DATE_TIME;
            ZonedDateTime result = ZonedDateTime.parse(dateString, formatter);
            return Timestamp.from(result.toInstant());
        }

    }
}
