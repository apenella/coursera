import java.io.File;
import java.lang.reflect.Array;
import java.security.MessageDigest;
import java.security.NoSuchAlgorithmException;
import java.util.*;

// Imports Aleix Penella
// read from file
import java.io.FileInputStream;
import java.io.InputStream;
import java.io.Reader;
import java.io.InputStreamReader;
import java.io.BufferedReader;

public class MP1 {
    Random generator;
    String userName;
    String inputFileName;
    String delimiters = " \t,;.?!-:@[](){}_*/";
    String[] stopWordsArray = {"i", "me", "my", "myself", "we", "our", "ours", "ourselves", "you", "your", "yours",
            "yourself", "yourselves", "he", "him", "his", "himself", "she", "her", "hers", "herself", "it", "its",
            "itself", "they", "them", "their", "theirs", "themselves", "what", "which", "who", "whom", "this", "that",
            "these", "those", "am", "is", "are", "was", "were", "be", "been", "being", "have", "has", "had", "having",
            "do", "does", "did", "doing", "a", "an", "the", "and", "but", "if", "or", "because", "as", "until", "while",
            "of", "at", "by", "for", "with", "about", "against", "between", "into", "through", "during", "before",
            "after", "above", "below", "to", "from", "up", "down", "in", "out", "on", "off", "over", "under", "again",
            "further", "then", "once", "here", "there", "when", "where", "why", "how", "all", "any", "both", "each",
            "few", "more", "most", "other", "some", "such", "no", "nor", "not", "only", "own", "same", "so", "than",
            "too", "very", "s", "t", "can", "will", "just", "don", "should", "now"};

    void initialRandomGenerator(String seed) throws NoSuchAlgorithmException {
        MessageDigest messageDigest = MessageDigest.getInstance("SHA");
        messageDigest.update(seed.toLowerCase().trim().getBytes());
        byte[] seedMD5 = messageDigest.digest();

        long longSeed = 0;
        for (int i = 0; i < seedMD5.length; i++) {
            longSeed += ((long) seedMD5[i] & 0xffL) << (8 * i);
        }

        this.generator = new Random(longSeed);
    }

    Integer[] getIndexes() throws NoSuchAlgorithmException {
        Integer n = 10000;
        Integer number_of_lines = 50000;
        Integer[] ret = new Integer[n];
        this.initialRandomGenerator(this.userName);
        for (int i = 0; i < n; i++) {
            ret[i] = generator.nextInt(number_of_lines);
        }
        return ret;
    }

    public MP1(String userName, String inputFileName) {
        this.userName = userName;
        this.inputFileName = inputFileName;
    }

    public String[] process() throws Exception {
        String[] ret = new String[20];
        
        // TODO: Aleix Penella
        List<String> fileArrayList = dumpFileToArray(this.inputFileName);
        Map<String,Integer> wordCounterMap = listWordCount(fileArrayList, delimiters, stopWordsArray, getIndexes());
        wordCounterMap = sortMapMP1(wordCounterMap);
        
        ret = getOnlySomeOfThem(wordCounterMap,ret.length);
        return ret;
    }

// Methods Aleix Penella

    /*
    * getOnlySomeOfThem: return some of the results
    */
    private static String[] getOnlySomeOfThem(Map<String,Integer> wordCounterMap, int size) {
      Map.Entry<String, Integer> entry;
      String[] someofthem = new String[size];
      int it = 0;
      Iterator itwordCounterMap = wordCounterMap.entrySet().iterator();
      while (it < size && itwordCounterMap.hasNext() ){
        entry = (Map.Entry<String, Integer>)itwordCounterMap.next();
        someofthem[it] = entry.getKey();
        it ++;
      }
      return someofthem;
    }

    /*
    * dumpFileToArray: return a List with all the content from file
    */
    private static List<String> dumpFileToArray(String inputFileName){
        List<String> fileArrayList = new ArrayList<String>();
        String rawline;
        try {
          File f = new File(inputFileName);
          InputStream fin = new FileInputStream(f);
          Reader r = new InputStreamReader(fin,"UTF-8");
          BufferedReader br = new BufferedReader(r);

          while ((rawline = br.readLine()) != null) {
              fileArrayList.add(rawline);
          }  
        } catch (Exception e){
          System.out.println("[dumpFileToArray] filename: "+inputFileName);
        }
        
        return fileArrayList;
    }

    /*
    * wordCount: return a Map containing all the occurrences for each word that appears into the list
    *   the current method does not process the words appeared on stopWordsArrayList
    *   the current method only process the index returned from getIndexes method
    */
    private static Map<String,Integer> listWordCount(List<String> fileArrayList, 
                            String delimiters, String[] stopWordsArray, Integer[] selectedlines) {

        List<String> stopWordsArrayList = Arrays.asList(stopWordsArray);
        Map<String,Integer> wordCounterMap = new HashMap<String,Integer>();
        String token;
        StringTokenizer tokenizer;
        
        for ( Integer index: selectedlines ) {
            tokenizer = new StringTokenizer(fileArrayList.get(index),delimiters);

            while (tokenizer.hasMoreTokens()) {
                token = tokenizer.nextToken().toLowerCase().trim();
                if ( !stopWordsArrayList.contains((Object)token) ){
                    if ( !wordCounterMap.containsKey((Object)token) ){
                        wordCounterMap.put(token,1);
                    }else{
                        wordCounterMap.put(token,wordCounterMap.get(token)+1);
                    }
                }                
            }
        }

        return wordCounterMap;
    }

    private static Map<String,Integer> sortMapMP1(Map<String, Integer> unsortedMapMP1){
        List<Map.Entry<String,Integer>> list = new LinkedList<>( unsortedMapMP1.entrySet() );
        Collections.sort( list, new Comparator<Map.Entry<String,Integer>>(){
            public int compare( Map.Entry<String,Integer> o1, Map.Entry<String,Integer> o2 ){
                if ((o1.getValue()).equals(o2.getValue())){
                    return (o1.getKey()).compareTo( o2.getKey() );
                } else {
                    return (o2.getValue()).compareTo( o1.getValue() );
                }
            }
        } );
        
        Map<String, Integer> sortedMapMP1 = new LinkedHashMap<String, Integer>();
        for (Iterator<Map.Entry<String, Integer>> it = list.iterator(); it.hasNext();) {
            Map.Entry<String, Integer> entry = it.next();
            sortedMapMP1.put(entry.getKey(), entry.getValue());
        }
        return sortedMapMP1;
    }


    public static void main(String[] args) throws Exception {
        if (args.length < 1){
            System.out.println("MP1 <User ID>");
        }
        else {
            String userName = args[0];
            String inputFileName = "./input.txt";
            MP1 mp = new MP1(userName, inputFileName);
            String[] topItems = mp.process();
            for (String item: topItems){
                System.out.println(item);
            }
        }
    }
}
