package com.kafka.connector;

import java.io.BufferedReader;
import java.io.InputStreamReader;
import java.net.HttpURLConnection;
import java.net.URL;
import java.util.ArrayList;
import java.util.List;

import com.google.gson.Gson;
import org.springframework.stereotype.Service;

// 생성된 커넥터 목록 불러오기
@Service
public class ListConnectors {

    public List<String> getConnectors() {
        List<String> connectors = new ArrayList<>();

        try {
            URL url = new URL("http://210.178.40.82:8083/connectors");
            HttpURLConnection conn = (HttpURLConnection) url.openConnection();
            conn.setRequestMethod("GET");
            conn.setRequestProperty("Content-Type", "application/json");

            int responseCode = conn.getResponseCode();
            if (responseCode == HttpURLConnection.HTTP_OK) {
                BufferedReader in = new BufferedReader(new InputStreamReader(conn.getInputStream()));
                String inputLine;
                StringBuilder response = new StringBuilder();
                while ((inputLine = in.readLine()) != null) {
                    response.append(inputLine);
                }
                in.close();
                // JSON 배열을 파싱하여 커넥터 이름 목록을 만듭니다.
                connectors = new Gson().fromJson(response.toString(), List.class);
            } else {
                System.out.println("Failed to get connectors: " + responseCode);
            }

            conn.disconnect();

        } catch (Exception e) {
            e.printStackTrace();
        }

        return connectors;
    }
}
