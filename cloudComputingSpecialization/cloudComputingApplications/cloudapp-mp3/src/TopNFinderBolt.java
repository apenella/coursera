import backtype.storm.topology.BasicOutputCollector;
import backtype.storm.topology.OutputFieldsDeclarer;
import backtype.storm.topology.base.BaseBasicBolt;
import backtype.storm.tuple.Fields;
import backtype.storm.tuple.Tuple;
import backtype.storm.tuple.Values;

import java.util.HashMap;
import java.util.Set;
import java.util.Map.Entry;
import java.util.List;
import java.util.ArrayList;
import java.util.Comparator;
import java.util.Collections;


/**
 * a bolt that finds the top n words.
 */
public class TopNFinderBolt extends BaseBasicBolt {
  private HashMap<String, Integer> currentTopWords = new HashMap<String, Integer>();
  private int N;

  private long intervalToReport = 20;
  private long lastReportTime = System.currentTimeMillis();

  public TopNFinderBolt(int N) {
    this.N = N;
  }

  @Override
  public void execute(Tuple tuple, BasicOutputCollector collector) {
 /*
    ----------------------TODO-----------------------
    Task: keep track of the top N words


    ------------------------------------------------- */
    currentTopWords.put(tuple.getString(0),tuple.getInteger(1));
    Set<Entry<String,Integer>> setCurrentTopWords = currentTopWords.entrySet();
    List<Entry<String, Integer>> sortedTopNWords = new ArrayList<Entry<String, Integer>>(setCurrentTopWords);

    Collections.sort(sortedTopNWords, new Comparator<Entry<String, Integer>>() {
        public int compare(Entry<String, Integer> o1, Entry<String, Integer> o2) {
       		if ((o1.getValue()).equals(o2.getValue())){
                    return (o1.getKey()).compareTo( o2.getKey() );
                } else {
                    return (o2.getValue()).compareTo( o1.getValue() );
                } 
	}
    });

    if ( sortedTopNWords.size() > this.N ) {
	sortedTopNWords.remove(sortedTopNWords.size()-1);
    	currentTopWords.clear();

	for ( Entry<String, Integer> item: sortedTopNWords ) {
		currentTopWords.put(item.getKey(),item.getValue());
	}
    }

    //reports the top N words periodically
    if (System.currentTimeMillis() - lastReportTime >= intervalToReport) {
      collector.emit(new Values(printMap()));
      lastReportTime = System.currentTimeMillis();
    }
  }

  @Override
  public void declareOutputFields(OutputFieldsDeclarer declarer) {

     declarer.declare(new Fields("top-N"));

  }

  public String printMap() {
    StringBuilder stringBuilder = new StringBuilder();
    stringBuilder.append("top-words = [ ");
    for (String word : currentTopWords.keySet()) {
      stringBuilder.append("(" + word + " , " + currentTopWords.get(word) + ") , ");
    }
    int lastCommaIndex = stringBuilder.lastIndexOf(",");
    stringBuilder.deleteCharAt(lastCommaIndex + 1);
    stringBuilder.deleteCharAt(lastCommaIndex);
    stringBuilder.append("]");
    return stringBuilder.toString();

  }
}
