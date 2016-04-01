package org.tkorostelev.homework03;

import java.util.StringTokenizer;

public class DatasetRow {

    private String id;

    public static DatasetRow parseInputLine(String line) {
        String parsed_id;
        StringTokenizer tokenizer = new StringTokenizer(line, "\t");

        int tokensCount = tokenizer.countTokens();
        if (tokensCount != 22) {
            return null;
        }

        DatasetRow result = new DatasetRow();

        // Skip first 20 tokens
        for (int i = 0; i < 20; i++) {
            tokenizer.nextToken();
        }

        // Token 21 is ID
        result.id = tokenizer.nextToken();

        return result;
    }


    public String getID() {
        return id;
    }
}
