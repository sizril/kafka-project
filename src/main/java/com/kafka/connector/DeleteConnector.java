package com.kafka.connector;

import java.io.BufferedReader;
import java.io.InputStreamReader;
import java.net.HttpURLConnection;
import java.net.URL;
import org.springframework.stereotype.Service;

// 커넥터 삭제
@Service
public class DeleteConnector {

    public void deleteConnector(String connectorName) {
        try {
            URL url = new URL("http://210.178.40.82:8083/connectors/" + connectorName);
            HttpURLConnection conn = (HttpURLConnection) url.openConnection();
            conn.setRequestMethod("DELETE");
            conn.setRequestProperty("Content-Type", "application/json");

            int responseCode = conn.getResponseCode();
            if (responseCode == HttpURLConnection.HTTP_NO_CONTENT) {
                System.out.println("Connector Deleted");
            } else {
                BufferedReader in = new BufferedReader(new InputStreamReader(conn.getInputStream()));
                String inputLine;
                StringBuilder response = new StringBuilder();
                while ((inputLine = in.readLine()) != null) {
                    response.append(inputLine);
                }
                in.close();
                System.out.println("Failed to delete connector: " + responseCode);
                System.out.println(response.toString());
            }

            conn.disconnect();

        } catch (Exception e) {
            e.printStackTrace();
        }
    }
}
