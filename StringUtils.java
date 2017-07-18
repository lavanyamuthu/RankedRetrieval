



import java.io.ByteArrayInputStream;
import java.io.IOException;
import java.io.UnsupportedEncodingException;
import java.util.logging.Level;
import java.util.logging.Logger;

import javax.xml.parsers.DocumentBuilder;
import javax.xml.parsers.DocumentBuilderFactory;
import javax.xml.parsers.ParserConfigurationException;

import org.apache.hadoop.io.BytesWritable;
import org.w3c.dom.Document;
import org.w3c.dom.Element;
import org.xml.sax.SAXException;

/**
 *
 * @author hadoop
 */
public class StringUtils {

    private static final String[] punctuation = {"`", "&", "'", "*", "\\", "{", "}", "[",
        "]", ":", ",", "!", ">", "<", "#", "(", ")", "%", ".",
        "+", "?", "\"", ";", "/", "^", "", "|", "~", "+","$","=","@","_","-"};
    private static final String[] stopwords = {"a","aa","aaa","aaaa","aaaaaaaaa","aad","about","above","after","again",
    	"against","all","am","an","and","any","are","aren't","as","at","be",
    	"because","been","before","being","below","between","both","but","by",
    	"can't","cannot","could","couldn't","did","didn't","do","does","doesn't",
    	"doing","don't","down","during","each","few","for","from","further","had",
    	"hadn't","has","hasn't","have","haven't","having","he","he'd","he'll",
    	"he's","her","here","here's","hers","herself","him","himself","his","how",
    	"how's","i","i'd","i'll","i'm","i've","if","in","into","is","isn't",
    	"it","it's","its","itself","let's","me","more","most","mustn't","my","much",
    	"myself","no","nor","not","of","off","often","on","once","only","or","other","ought",
    	"our","ours","she","the","they","their","them","those","to","these","then",
    	"very","while","who","whom","where","what","when","were","would","wont","with",
    	"while","whose","which","whether","will","yet","you","your","us"};

    
    public static String normalizeText(String str) {
    	
        //replace punctuation
    	
        for (int i=0; i<punctuation.length;i++) {
            str = str.replace(punctuation[i], "");
        }
        str = str.replaceAll("\\d","");
        str = str.toLowerCase();
        str = " "+str+" ";
        //System.out.println("Before normalization");
        //System.out.println(str);
        //replace stop words
        for (int i=0;i<stopwords.length;i++) {
        	String repl = " "+stopwords[i]+" ";
            str = str.replace(repl, " ");
        }

        str = str.replace("-", " ").replace("\t", " ").replace("\r", " ").replace("\n", " ").replace(" ", " ");
        //System.out.println("After normalization");
        //System.out.println(str);
        /*str = str.replace("-", " ").replace(" ", "::")
                .replace("\n", "::").replace("\t", "::")
                .replace("\r", "::");*/
        return str.trim();
    }

    public static ReutersDoc getXMLContent(BytesWritable xmlStr) {
        //get the xml factory and parse the xml file
        DocumentBuilderFactory dbf = DocumentBuilderFactory.newInstance();
        Document dom;
        StringBuilder docContent = new StringBuilder();
        ReutersDoc doc = new ReutersDoc();
        try {
            //Using factory get an instance of document builder
            DocumentBuilder db = dbf.newDocumentBuilder();

            //parse using builder to get DOM representation of the XML file
            dom = db.parse(new ByteArrayInputStream(xmlStr.getBytes()));

            //now parse document
            //get document id
            Element root = dom.getDocumentElement();
            doc.setDocID(root.getAttribute("itemid"));

            //get title and headline and body of xml data
            docContent.append(root.getElementsByTagName("title").item(0).getTextContent().trim()).append(" ");
            docContent.append(root.getElementsByTagName("headline").item(0).getTextContent().trim()).append(" ");
            docContent.append(root.getElementsByTagName("text").item(0).getTextContent().trim());

            doc.setContent(docContent.toString());
        } catch (Exception pce) {
            pce.printStackTrace();
        }

        return doc;
    }
    
    @SuppressWarnings("deprecation")
	public static ReutersDoc getContent(BytesWritable xmlStr) throws UnsupportedEncodingException {
        //get the xml factory and parse the xml file
        ReutersDoc doc = new ReutersDoc();
            doc.setDocID("temp");
            doc.setContent(new String(xmlStr.get(),"UTF8"));
        return doc;
    }
    
    public static boolean isNumeric(String input) {
    	  try {
    	    Integer.parseInt(input);
    	    return true;
    	  }
    	  catch (NumberFormatException e) {
    	    // s is not numeric
    	    return false;
    	  }
    	}
}
