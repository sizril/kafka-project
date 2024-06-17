package com.kafka.connector;


import com.google.gson.Gson;
import com.google.gson.JsonObject;
import com.google.gson.JsonParser;

import java.io.BufferedReader;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.io.OutputStream;
import java.net.HttpURLConnection;
import java.net.URL;
import org.springframework.stereotype.Service;

// 커넥터 생성
@Service
public class CreateConnector {

    public void mkConnector(String connectorName, String processedTopic){

        try{
            InputStream inputStream = CreateConnector.class.getClassLoader().getResourceAsStream("base_connector_config.json");
            BufferedReader reader = new BufferedReader(new InputStreamReader(inputStream));
            JsonObject baseConfig = JsonParser.parseReader(reader).getAsJsonObject();
            reader.close();

            JsonObject connectorConfig = new JsonObject();
            connectorConfig.addProperty("name", connectorName);
            baseConfig.getAsJsonObject("config").addProperty("topics", processedTopic);
            connectorConfig.add("config",baseConfig.getAsJsonObject("config"));

            URL url = new URL("http://210.178.40.82:8083/connectors");
            HttpURLConnection conn = (HttpURLConnection) url.openConnection();
            conn.setRequestMethod("POST");
            conn.setRequestProperty("Content-Type", "application/json");
            conn.setDoOutput(true);

            OutputStream os = conn.getOutputStream();
            os.write(new Gson().toJson(connectorConfig).getBytes());
            os.flush();
            os.close();

            int responseCode = conn.getResponseCode();
            if(responseCode == HttpURLConnection.HTTP_CREATED){
                System.out.println("Connector Created");
            } else {
                BufferedReader in = new BufferedReader(new InputStreamReader(conn.getInputStream()));
                String inputLine;
                StringBuilder response = new StringBuilder();

                while ((inputLine = in.readLine()) != null) {
                    response.append(inputLine);
                }

                in.close();

                System.out.println("Failed to create connector: " + responseCode);
                System.out.println(response.toString());
            }

            conn.disconnect();

        } catch (Exception e){
            e.printStackTrace();
        }
    }
}
