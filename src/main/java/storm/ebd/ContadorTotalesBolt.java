package storm.ebd;

import java.util.HashMap;
import java.util.Iterator;
import java.util.Map;
import java.util.Map.Entry;

import redis.clients.jedis.*;

import backtype.storm.task.OutputCollector;
import backtype.storm.task.TopologyContext;
import backtype.storm.topology.OutputFieldsDeclarer;
import backtype.storm.topology.base.BaseRichBolt;
import backtype.storm.tuple.Fields;
import backtype.storm.tuple.Tuple;

public class ContadorTotalesBolt extends BaseRichBolt {
	/**
	 * 
	 */
	private static final long serialVersionUID = 1L;
	Map<String, Integer> counters;
	private OutputCollector collector;
	private Jedis jedis;
	private String host;
	private int port;
	private int uniqueWords=0;
	private int totalWords=0;

	@Override
	public void prepare(Map stormConf, TopologyContext context,
			OutputCollector collector) {
		this.counters = new HashMap<String, Integer>();
		this.collector = collector;
		host = stormConf.get(Conf.REDIS_HOST_KEY).toString();
		port = Integer.valueOf(stormConf.get(Conf.REDIS_PORT_KEY).toString());
		connectToRedis();
	}
	private void connectToRedis() {
		jedis = new Jedis(host, port);
		jedis.connect();
	}

	@Override
	public void execute(Tuple input) {
		String str = input.getString(0);
		if (!counters.containsKey(str)) {
			counters.put(str, 1);
		} else {
			Integer c = counters.get(str) + input.getInteger(1);
			counters.put(str, c);
		}
		
		collector.ack(input);
	}

	@Override
	public void cleanup() {
		//Salvar en redis todos los totales
		uniqueWords = counters.size();
		
		Iterator<Entry<String, Integer>> totales = counters.entrySet().iterator();
		
		while (totales.hasNext()) {
	        Map.Entry pair = (Map.Entry)totales.next();
	        String key = pair.getKey().toString();
	        String value = jedis.get(key);
			if (value == null) { // es un cliente nuevo
				jedis.set(key, pair.getValue().toString());
			} else {
				System.out.println("Palabra Repetida en Redis");
				jedis.del(key);
				jedis.set(key, pair.getValue().toString());
			}
			totalWords = totalWords + Integer.parseInt(pair.getValue().toString());
	    }
		jedis.set("Palabras unicas", uniqueWords+"");
		jedis.set("Palabras Totales", totalWords+"");
		
		
	}

	@Override
	public void declareOutputFields(OutputFieldsDeclarer declarer) {
		declarer.declare(new Fields("word","total"));
	}

	@Override
	public Map<String, Object> getComponentConfiguration() {
		return null;
	}
}
