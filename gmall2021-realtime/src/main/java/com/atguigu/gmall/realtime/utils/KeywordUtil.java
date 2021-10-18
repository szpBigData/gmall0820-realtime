package com.atguigu.gmall.realtime.utils;

import com.typesafe.sslconfig.ssl.FakeChainedKeyStore;
import org.wltea.analyzer.core.IKSegmenter;
import org.wltea.analyzer.core.Lexeme;

import java.io.IOException;
import java.io.StringReader;
import java.util.ArrayList;
import java.util.List;

/**
 * @author sunzhipeng
 * @create 2021-04-17 21:16
 */
public class KeywordUtil {
    //使用ik分词器对字符串进行分词
    public static List<String> analyze(String text) {
        StringReader sr = new StringReader(text);
        IKSegmenter ik = new IKSegmenter(sr, true);
        Lexeme lex = null;
        List<String> keywordList = new ArrayList<>();
        while (true) {
            try {
                if ((lex = ik.next()) != null) {
                    String lexemeText = lex.getLexemeText();
                    keywordList.add(lexemeText);
                } else {
                    break;
                }
            } catch (Exception e) {
                e.printStackTrace();
            }
        }
        return keywordList;
    }

    public static void main(String[] args) {
        String text = "Apple iPhoneXSMax (A2104) 256GB 深空灰色 移动联通电信4G手机 双卡双待";
        System.out.println(KeywordUtil.analyze(text));
    }

}
