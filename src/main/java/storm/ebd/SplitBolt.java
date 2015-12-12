package storm.ebd;

import java.util.Map;

import backtype.storm.task.OutputCollector;
import backtype.storm.task.TopologyContext;
import backtype.storm.topology.OutputFieldsDeclarer;
import backtype.storm.topology.base.BaseRichBolt;
import backtype.storm.tuple.Tuple;
import backtype.storm.tuple.Values;
import backtype.storm.tuple.Fields;

public class SplitBolt extends BaseRichBolt {
	/**
	 * 
	 */
	private static final long serialVersionUID = 1L;
	private OutputCollector collector;
	@Override
	public void prepare(Map stormConf, TopologyContext context,
			OutputCollector collector) {
		this.collector = collector;
	}
	@Override
	public void execute(Tuple input) {
		String sentence = input.getString(0);
		//Quitamos simbolos de puntuacion y tildes, y transfromamos a minusculas
		String[] words = sentence.replaceAll("[^a-zA-Z ]", "").toLowerCase().split("\\s+");
		int countAgramas=0;
		String bigrama = null;
		//enviamos bigramas
		for(String word: words){
			word = word.trim();
			bigrama = bigrama + " " + word;
			countAgramas++;
			if(!bigrama.isEmpty() && countAgramas > 1){
				collector.emit(new Values(bigrama));
				bigrama="";
				countAgramas=0;
			}
		}
		collector.ack(input);
	}
	@Override
	public void declareOutputFields(OutputFieldsDeclarer declarer) {
		declarer.declare(new Fields("word"));
	}

	@Override
	public void cleanup() {
	}
	@Override
	public Map<String, Object> getComponentConfiguration() {
		return null;
	}
}
