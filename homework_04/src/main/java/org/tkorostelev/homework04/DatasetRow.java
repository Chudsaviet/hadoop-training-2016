package org.tkorostelev.homework04;

import java.util.StringTokenizer;

public class DatasetRow {

    private String timestamp;
    private String iPinYouID;
    private String userAgent;
    private String ip;
    private double biddingPrice;
    private String id;
    private int streamID;

    public static DatasetRow parseInputLine(String line) {
        StringTokenizer tokenizer = new StringTokenizer(line, "\t");

        int tokensCount = tokenizer.countTokens();
        if (tokensCount != 22) {
            return null;
        }

        DatasetRow result = new DatasetRow();

        //Skip first token
        tokenizer.nextToken();

        //Token 2 is timestamp
        result.timestamp = tokenizer.nextToken();

        //Token 3 is iPinYouID
        result.iPinYouID =tokenizer.nextToken();

        //Token 4 is UserAgent
        result.userAgent = tokenizer.nextToken();

        //Token 5 is IP
        result.ip = tokenizer.nextToken();

        // Skip next 13 tokens
        for (int i = 0; i < 13; i++) {
            tokenizer.nextToken();
        }

        //Token 19 is Bidding Price
        result.biddingPrice = Double.parseDouble(tokenizer.nextToken());

        // Skip 1 token
        tokenizer.nextToken();

        // Token 21 is ID
        result.id = tokenizer.nextToken();

        // Token 22 is StreamID
        result.streamID = Integer.parseInt(tokenizer.nextToken());

        return result;
    }

    public String getTimestamp() {
        return timestamp;
    }

    public String getIPinYouID() {
        return iPinYouID;
    }

    public String getUserAgent() {
        return userAgent;
    }

    public String getIP() {
        return ip;
    }

    public double getBiddingPrice() {
        return biddingPrice;
    }

    public String getID() {
        return id;
    }

    public int getStreamID() {
        return streamID;
    }

}
