package rc;

import org.apache.http.HttpEntity;
import org.apache.http.HttpHost;
import org.apache.http.HttpResponse;
import org.apache.http.client.methods.HttpGet;
import org.apache.http.conn.params.ConnRoutePNames;
import org.apache.http.impl.client.DefaultHttpClient;
import org.json.simple.JSONArray;
import org.json.simple.JSONObject;
import org.json.simple.parser.JSONParser;
import org.json.simple.parser.ParseException;

import java.io.*;
import java.util.*;

public class App 
{
    public static void main( String[] args ) throws InterruptedException {

        //TreeSet<String> dishes = new TreeSet<String>();


        List<String> prefixes = getPrefixes();
        int index = 0;
//        for (String s : prefixes){
//            System.out.println(++index + ":" + s);
//        }

        HashMap<String, TreeSet<String>> typesAndDishes = new HashMap<String, TreeSet<String>>();
        for (String prefix : prefixes){

            //if (index < 10){
                String grubHubUrl = "https://api-gtm.grubhub.com/autocomplete?lat=37.75714874&lng=-122.42819214&locationMode=delivery&prefix=" + prefix + "&resultCount=100&resultTypeList=dishTerm";
                String jsonResponse = doRequest(grubHubUrl);
                System.out.println(prefix + ":   " + jsonResponse);
                jsonParse(jsonResponse, typesAndDishes);
//                Thread.sleep(500);
                index++;
            //}
        }
        //printDishes(typesAndDishes);
        saveDishes(typesAndDishes);
    }

    private static List<String> getPrefixes(){

        List<String> prefixes = new ArrayList<String>();
        String alphabet = "abcdefghijklmnopqrstuvwxyz";

        for (int i = 0; i < alphabet.length(); i++){
            String prefix =  String.valueOf(alphabet.charAt(i));
            prefixes.add(prefix);
        }

        for (int i = 0; i < alphabet.length(); i++){

            String prefix = String.valueOf(alphabet.charAt(i));

            for (int j = 0; j < alphabet.length(); j++) {

                prefix += String.valueOf(alphabet.charAt(j));
                prefixes.add(prefix);
                prefix = prefix.substring(0, prefix.length() - 1);
            }
        }

//        for (int i = 0; i < alphabet.length(); i++){
//
//            String prefix = String.valueOf(alphabet.charAt(i));
//
//            for (int j = 0; j < alphabet.length(); j++) {
//
//                prefix += String.valueOf(alphabet.charAt(j));
//
//                for (int k = 0; k < alphabet.length(); k++) {
//                    prefix += String.valueOf(alphabet.charAt(k));
//                    prefixes.add(prefix);
//                    prefix = prefix.substring(0, prefix.length() - 1);
//                }
//                prefix = prefix.substring(0, prefix.length() - 1);
//            }
//        }

        return prefixes;
    }

    private static void printDishes(HashMap<String, TreeSet<String>> typeAndDishes){

        int index = 0;

        for (String type : typeAndDishes.keySet()) {

            for (String dish : typeAndDishes.get(type)) {
                System.out.println(++index + ": " + type + " : " + dish.toString());
            }
        }
    }

    private static void saveDishes(HashMap<String, TreeSet<String>> typeAndDishes) {

        File fout = new File("dishes.txt");
        FileOutputStream fos = null;
        try {
            fos = new FileOutputStream(fout);
        } catch (FileNotFoundException e) {
            e.printStackTrace();
        }

        try {
            BufferedWriter bw = new BufferedWriter(new OutputStreamWriter(fos));
            int index = 0;
            for (String type : typeAndDishes.keySet()) {

                for (String dish : typeAndDishes.get(type)) {

                    System.out.println(++index + ": " + type + "," + dish);
                    bw.write(type + "," + dish);
                    bw.newLine();
                }
            }
            bw.close();
        } catch (IOException e) {
            e.printStackTrace();
        }
    }

    private static String doRequest(String urlStr){

        DefaultHttpClient httpclient = new DefaultHttpClient();
        //String urlStr = "https://api-gtm.grubhub.com/autocomplete?lat=37.75714874&lng=-122.42819214&locationMode=delivery&prefix=" + prefix + "&resultCount=100&resultTypeList=dishTerm";
        //String urlStr = "https://api-gtm.grubhub.com/autocomplete?lat=37.75714874&lng=-122.42819214&locationMode=delivery&prefix=ha&resultCount=8&resultTypeList=dishTerm";

        HttpGet httpget = new HttpGet(urlStr);
        httpget.addHeader("Content-Type", "application/json");
        httpget.addHeader("Authorization", "Bearer 08dd882b-f34d-4b4d-8fcd-e0dc704c3587");
        StringBuilder json = new StringBuilder();

        try {
            HttpResponse response = httpclient.execute(httpget);
            HttpEntity entity = response.getEntity();
            BufferedReader rd = new BufferedReader(new InputStreamReader(entity.getContent()));

            String line;
            while ((line = rd.readLine()) != null) {
                json.append(line);
            }

            //System.out.println(prefix + ":   " + json.toString());
        } catch (IOException e) {
            e.printStackTrace();
        }

        return json.toString();
    }

    //private static void jsonParse(String str, TreeSet<String> dishes){
        //str = "{\"age\":100, \"name\":\"mkyong.com\", \"messages\":[\"msg 1\",\"msg 2\",\"msg 3\"]}";
    private static void jsonParse(String str, HashMap<String, TreeSet<String>> typesAndDishes){
        //HashMap<String, TreeSet<String>> typesAndDishes = new HashMap<String, TreeSet<String>>();
        JSONParser parser = new JSONParser();

        try {
            Object obj = parser.parse(str);
            JSONObject jsonObject = (JSONObject) obj;
            JSONArray resultListArr = (JSONArray)jsonObject.get("result_list");
            JSONObject resultListObj = (JSONObject)resultListArr.get(0);
            String resultType = resultListObj.get("result_type").toString();



            //if (type.equalsIgnoreCase("dishTerm")){
                JSONArray compListArr = (JSONArray)resultListObj.get("completion_list");

                for (Object itemObj : compListArr){
                    JSONObject item = (JSONObject)itemObj;
                    //if (item.get("type").toString().equalsIgnoreCase("dish")){
                    String type = item.get("type").toString();
                    if (!typesAndDishes.containsKey(type)){
                        typesAndDishes.put(type, new TreeSet<String>());
                    }

                    TreeSet<String> dishes = typesAndDishes.get(type);
                    dishes.add(item.get("value").toString());
                    typesAndDishes.put(type, dishes);

                    //}
                }
            //}

        } catch (ParseException e) {
            e.printStackTrace();
        }
    }
}
