package storm.ebd;


import storm.kafka.KafkaSpout;
import storm.kafka.SpoutConfig;
import storm.kafka.StringScheme;
import storm.kafka.ZkHosts;
import backtype.storm.Config;
import backtype.storm.LocalCluster;
import backtype.storm.StormSubmitter;
import backtype.storm.generated.AlreadyAliveException;
import backtype.storm.generated.InvalidTopologyException;
import backtype.storm.spout.SchemeAsMultiScheme;
import backtype.storm.topology.TopologyBuilder;
import backtype.storm.utils.Utils;
/**
 * 
 * @author Carlos Palomares Campos
 * Description: Topologia que define el contador de palabras
 *
 */
public class ContadorPalabrasTopologia {
	private TopologyBuilder builder = new TopologyBuilder();
	private Config conf = new Config();
	private LocalCluster cluster;
	//Seteamos el ficher con su ruta de manera manual, no se porque razon pero al hacerlo como parametro zookeeper falla
	//y lo interpreta como si fueran varios parametros
	private static String file="/home/cloudera/practicaStorm/CarlosPalomares/src/test/words.txt";

	public static final String DEFAULT_JEDIS_PORT = "6379";

	public ContadorPalabrasTopologia() {	
		// Configuracion -  setteamos el redis
		conf.put(Conf.REDIS_PORT_KEY, DEFAULT_JEDIS_PORT);
		conf.put("inputFile", file);
		SpoutConfig kafkaConf = new SpoutConfig(
	            new ZkHosts("localhost:9092"),
	            "carlos",
	            "/kafka",
	            "KafkaSpout");
	    kafkaConf.scheme = new SchemeAsMultiScheme(new StringScheme());
	        
        
		//Definimos el Spout que leera de kafka
		builder.setSpout("lectorSpout",  new KafkaSpout(kafkaConf), 4);
		// Capa de boults
		// Boult que emite palabras cono bigramas eliminando simbolos de puntuacion y tildes
		builder.setBolt("splitBolt", new SplitBolt(), 10)		
				.shuffleGrouping("lectorSpout");
		
		builder.setBolt("contadorParcialBolt", new ContadorParcialBolt(), 10).
				shuffleGrouping("splitBolt");
		
		builder.setBolt("contadorTotalesBolt", new ContadorTotalesBolt(), 1).
			globalGrouping("contadorParcialBolt");
		
		
	}
	
	public TopologyBuilder getBuilder() {
		return builder;
	}
	
	public LocalCluster getLocalCluster() {
		return cluster;
	}
	public Config getConf() {
		return conf;
	}
	public void runLocal(int runTime) {
		conf.setDebug(false);
		
		conf.put(Conf.REDIS_HOST_KEY, "localhost");
		cluster = new LocalCluster();
		cluster.submitTopology("test", conf, builder.createTopology());
		if (runTime > 0) {
			Utils.sleep(runTime);
			shutDownLocal();
		}
	}
	public void shutDownLocal() {
		if (cluster != null) {
			cluster.killTopology("test");
			cluster.shutdown();
		}
	}
	
	public void runCluster(String name, String redisHost)
			throws AlreadyAliveException, InvalidTopologyException {
		conf.setNumWorkers(20);
		conf.put(Conf.REDIS_HOST_KEY, redisHost);
		StormSubmitter.submitTopology(name, conf, builder.createTopology());
	}
	public static void main(String[] args) throws Exception {
		ContadorPalabrasTopologia topology = new ContadorPalabrasTopologia();
		if (args != null && args.length > 1) {
			topology.runCluster(args[0], args[1]);
		} else {
			System.out.println("Ejecutandose en modo local.");
			topology.runLocal(10000);
		}

	}
}
