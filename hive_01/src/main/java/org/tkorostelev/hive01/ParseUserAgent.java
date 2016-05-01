package org.tkorostelev.hive01;

import org.apache.hadoop.hive.ql.exec.UDF;
import org.apache.hadoop.io.Text;

public class ParseUserAgent extends UDF {
    private Text result = new Text();
  
    public Text evaluate(Text userAgent) {
        if (userAgent == null) {
          return null;
        };

        String userAgentString = userAgent.toString();

        result.set(getBrowser(userAgentString)+'|'+getOS(userAgentString)+'|'+getDevice(userAgentString));

        return result;
    }

    private String getBrowser(String userAgent) {
        String userAgentLower = userAgent.toLowerCase();

        if(userAgentLower.contains("msie"))
            return "IE";
        else if(userAgentLower.contains("chrome"))
            return "CHROME";
        else if(userAgentLower.contains("opera"))
            return "OPERA";
        else if(userAgentLower.contains("mozilla"))
            return "MOZILLA";
        else
            return "OTHER";
    }

    private String getOS(String userAgent) {
        String userAgentLower = userAgent.toLowerCase();

        if(userAgentLower.contains("windows"))
            return "WINDOWS";
        else if(userAgentLower.contains("linux"))
            return "LINUX";
        else if(userAgentLower.contains("osx"))
            return "OSX";
        else
            return "OTHER";
    }

    private String getDevice(String userAgent) {
        String userAgentLower = userAgent.toLowerCase();

        if(userAgentLower.contains("mobile"))
            return "MOBILE";
        else if(userAgentLower.contains("tablet"))
            return "TABLET";
        else if(userAgentLower.contains("bot"))
            return "BOT";
        else
            return "DESKTOP/OTHER";
    }

}