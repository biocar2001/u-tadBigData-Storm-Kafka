package storm.ebd;

import java.io.BufferedReader;
import java.io.FileNotFoundException;
import java.io.FileReader;
import java.util.Properties;

import kafka.javaapi.producer.Producer;
import kafka.producer.KeyedMessage;
import kafka.producer.ProducerConfig;

/*
 * Clase utilizada para cargar el fichero que corresponda en la cola kafka para despues ser consumido*/
public class SimpleProducer {
	private static Producer<String, String> producer;
	public SimpleProducer() {
		Properties props = new Properties();
		// Set the broker list for requesting metadata to find the lead broker
		props.put("metadata.broker.list","localhost:9092, localhost:9093,localhost:9094");
		//This specifies the serializer class for keys
		props.put("serializer.class", "kafka.serializer.StringEncoder");
		// 1 means the producer receives an acknowledgment once the lead replica
		// has received the data. This option provides better durability as the
		// client waits until the server acknowledges the request as successful.
		props.put("request.required.acks", "1");
		ProducerConfig config = new ProducerConfig(props);
		producer = new Producer<String, String>(config);
	}
	public static void main(String[] args) {
		int argsCount = args.length;
		if (argsCount == 0 || argsCount == 1)
			throw new IllegalArgumentException("Por favor indica nombre del topic y ruta del fichero, 2 parametros");
		// Topic name and path of the file from the
		// command line
		String topic = (String) args[0];
		String file = (String) args[1];
		System.out.println("Topic Name - " + topic);
		System.out.println("File path - " + file);
		
		SimpleProducer simpleProducer = new SimpleProducer();
		simpleProducer.publishMessage(topic, file);
	}
	private void publishMessage(String topic, String file) {
		FileReader fileReader;
		String str;
		try {
			fileReader = new FileReader(file);
		} catch (FileNotFoundException e) {
			throw new RuntimeException("Error reading file "
					+ file);
		}
		BufferedReader reader = new BufferedReader(fileReader);
		try {
			while ((str = reader.readLine()) != null) {
				System.out.println("Insertando en kafka Line:");
				System.out.println(str);
				// Creates a KeyedMessage instance
				KeyedMessage<String, String> data = new KeyedMessage<String, String>(topic, str);
				// Publish the message
				producer.send(data);
				System.out.println("Insertada Line:");
				System.out.println(str);
			}
		} catch (Exception e) {
			throw new RuntimeException("Error reading typle", e);
		}
		
		// Close producer connection with broker.
		producer.close();
	}
}