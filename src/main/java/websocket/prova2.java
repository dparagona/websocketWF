package websocket;

import com.google.gson.Gson;
import data.model.Coordinate;
import data.model.Street;
import data.neo4j.Neo4jDAOImpl;
import logic.areaName.AreaNameLogic;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.common.serialization.StringDeserializer;
import util.ConfigurationSingleton;

import javax.websocket.*;
import javax.websocket.server.*;
import javax.ws.rs.FormParam;
import java.io.IOException;
import java.time.Duration;
import java.util.*;
import java.util.concurrent.CopyOnWriteArraySet;
import java.lang.Thread;


@ServerEndpoint(
        value = "/prova2",
        decoders = {MessageDecoder.class},
        encoders = {StreetEncoder.class, RequestEncoder.class, RequestedSquareEncoder.class}
)

public class prova2 {

    private Session session;
    private static Set<prova2> provaEndpoints = new CopyOnWriteArraySet<>();
    private RequestedSquare square;
    private final AreaNameLogic areaNameLogic = new AreaNameLogic(); //serve per ottenere le aree interne ad un riquadro
    private ArrayList<StreetMongo> streetsFromArea = new ArrayList<>(); //array di strade presenti nelle aree richieste, provenienti da mongo
    private ArrayList<Street> streetsWithGeometry = new ArrayList<>();  //array di strade contenenti un array che ne definisce la geometria, provenienti da Neo4J
    private Map<String, Thread> workers = new HashMap<>();//mappa che contiene i vari workers
	private AreaBuffer buffer;

    @OnOpen
    public void onOpen(Session session) throws IOException {
        this.session = session;
		this.buffer = new AreaBuffer(20);

        // Registra la sessione in un Set
        provaEndpoints.add(this);
        if (session != null) {
            session.getBasicRemote().sendText("Connessione Accettata!");
            provaEndpoints.add(this);
        }
    }

    @OnMessage
    public void onMessage(Session session, Message message) throws IOException {

         if (message instanceof RequestedSquare) {
            System.out.println("Messaggio: " + message);
            RequestedSquare square = (RequestedSquare) message;
            session.getBasicRemote().sendText("Riquadro ricevuto con successo!");
            //qui bisogna controllare se il riquadro ricevuto e' diverso da quello gia' in possesso di questo Endpoint
            this.square = square;//per ora faccio cosi', poi bisogna vedere se c'e' bisogno di controllare che il nuovo quadrato richiesto non sia diverso dal precedente
            this.square.print(System.out);

            ArrayList<String> areaNames = getAreaNames(this.square);//ottiene l'array delle aree da Mongo
            //stampa nella console delle aree ottenute da Mongo per debug
            int i = 0;
            System.out.println(">>AREE RICEVUTE");
            System.out.println(" ");
			
			//bisogna prima pulire la mappa dalle aree che non sono state richieste
			//per ogni chiave della mappa, se il nuovo insieme di aree richieste non contiene la chiave corrispondente all'i.esimo worker, elimina il worker che non e' stato richiesto
			for(String s: workers.keySet()){
				if(!areaNames.contains(s)){
					//elimina il worker perche' non e' stata richiesta l'area di sua competenza
					//workers.get(s).interrupt();
					workers.remove(s).interrupt();
				}
			}
			//per ogni area, se la mappa non contiene un worker corrispondente, ne istanzia uno, lo aggiunge alla mappa e lo fa partire
            for(String s : areaNames){
				if(!workers.keySet().contains(s)){
					//alloca tutto il necessario e avvia i processi da avviare
					Thread produttore = new Thread(new AreaProducer(buffer, s, session));
					Thread consumatore = new Thread(new AreaConsumer(buffer, s));
					produttore.start();
					workers.put(s, consumatore);
					consumatore.start();
				}
            }
        }
    }

    @OnClose
    public void onClose(Session session) throws IOException {
        //gestisce la chiusura della connessione
        provaEndpoints.remove(this);
        for (String w : workers.keySet()) {
            workers.remove(w);//pulizia della mappa
			//bisogna terminare tutti i workerz
        }
    }

    @OnError
    public void onError(Session session, Throwable throwable) throws IOException {
        throwable.printStackTrace(System.out);
    }

    private ArrayList<String> getAreaNames(RequestedSquare s) {
        float lon1 = Float.parseFloat(s.getUpperLeftCorner().substring(0, s.getUpperLeftCorner().indexOf(",")));
        float len1 = Float.parseFloat(s.getUpperLeftCorner().substring(s.getUpperLeftCorner().indexOf(",") + 1));
        float lon2 = Float.parseFloat(s.getLowerRightCorner().substring(0, s.getLowerRightCorner().indexOf(",")));
        float len2 = Float.parseFloat(s.getLowerRightCorner().substring(s.getLowerRightCorner().indexOf(",") + 1));
        return areaNameLogic.getAreaNameFromCorners(lon1, len1, lon2, len2);
    }
}
//ELEMENTO DEL BUFFER
class AreaElement{
	private Session session;
	private ArrayList<String> areaNames;
	private HashMap<Long, StreetMongo> streetsFromArea = new HashMap<>(); //array di strade presenti nelle aree richieste, provenienti da mongo
    private HashMap<Long, Street> streetsWithGeometry = new HashMap<>();  //array di strade contenenti un array che ne definisce la geometria, provenienti da Neo4J
	private ConfigurationSingleton conf = ConfigurationSingleton.getInstance();    
	private FeatureCollection featureCollection = new FeatureCollection();
    private Gson gson = new Gson();
	private  KafkaConsumer<String, String> consumer;

	public AreaElement(ArrayList<String> areaNames, Session session){
		Properties props = new Properties();

		this.areaNames = areaNames;
        this.session = session;
		//creazione del consumer di kafka (lo faccio qui per non doverlo creare ad ogni invocazione del metodo getStreetTraffic())
		//        props.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, KafkaConfig.KAFKA_HOST_LOCAL_NAME+":"+KafkaConfig.KAFKA_PORT);
        props.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, conf.getProperty("kafka.hostname") + ":" + conf.getProperty("kafka.port"));//KafkaConfig-->classe che contiene le info del kafka che uso
        props.put(ConsumerConfig.GROUP_ID_CONFIG, "areasConsumerGroup");
        props.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class);
        props.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class);
        props.put(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest");
        System.out.println("Creo il consumer di kafka");
		this.consumer = new KafkaConsumer<>(props);
	}

	//metodi di AreaWorker
	public void getStreetsTraffic() { // non dovrebbe piu' essere necessario effettuare la poll in un ciclo while, perche' ce n'e' gia' uno a monte, solo che in questo modo l'output sara' enorme, quindi mi converra' creare un kafka consumer fuori dall'elemento, per poi passarglielo come parametro nel costruttore 
        
        this.consumer.subscribe(this.areaNames);//il metodo subscribe vuole solo arraylist, quindi bisogna usare arraylist anche per una sola area
       // while (true) {
            System.out.println("Poll eseguito");
            ConsumerRecords<String, String> streetResults = consumer.poll(Duration.ofMillis(10000));
            int i = 0;
            for (ConsumerRecord<String, String> record : streetResults) {
                i++;
                String value = record.value();
                StreetMongo streetMongo = gson.fromJson(value, StreetMongo.class);
                streetsFromArea.put(Long.valueOf(streetMongo.getLinkid()), streetMongo);
            }
           // if (i != 0) {//se i!=0 l'array ha elementi, quindi esco dal while
            //    System.out.println("Dati prelevati da Kafka, Worker di " + areaNames.get(0)+".");
             //   i=0;
			//	break;
           // }
       // }
    }
	
	 public void getStreetsFromNeo4J() {

		//apre la connessione 
		String uri = conf.getProperty("neo4j-core.bolt-uri");
		String user = conf.getProperty("neo4j-core.user");
		String password = conf.getProperty("neo4j-core.password");
		Neo4jDAOImpl database = new Neo4jDAOImpl(uri, user, password);
		database.openConnection();

        System.out.println("Recuperando i dati da Neo4j...");
		try{
			ArrayList<Street> streets = database.getStreetsFromLinkIds(streetsFromArea.keySet());
			for (Street s: streets){
				streetsWithGeometry.put(s.getLinkId(), s);
			}
			System.out.println("Strade Recuperate");
		}catch(org.neo4j.driver.exceptions.NoSuchRecordException e){
			System.out.println("Strade non trovate, c'e' stato un problema con neo4j");
		}
    }

	public void convertToFeatures() {
        System.out.println("Conversione dati in formato geojson...");
        for(Long key : streetsWithGeometry.keySet()){
            Street s = streetsWithGeometry.get(key);
            Properties props = new Properties();
            Geometry geoms = new Geometry();
            ArrayList<Coordinate> coord = s.getGeometry();
            for (Coordinate c : coord) {
                geoms.addGeometry(c.getLongitude(), c.getLatitude());
            }
            props.put("name", s.getName());
            if (s.getFfs() > 20)
                props.put("color", "#1199dd");
            else {
                props.put("color", "#d21f1b");
            }
            Feature feature = new Feature(geoms, props);
            if (!feature.isEmpty())
                featureCollection.addFeature(feature);
        }
    }
	
	public synchronized void send() throws IOException {
        if (!streetsWithGeometry.isEmpty()) {
            AreaResponse response = new AreaResponse(areaNames.get(0), featureCollection);
            //String toClient = gson.toJson(featureCollection);
            String toClient = gson.toJson(response);
            //System.out.println(toClient);
            this.session.getBasicRemote().sendText(toClient);

            System.out.println("JSON inviato al client");
        }
    }

	//utilities
	public String getAreaName(){
		return this.areaNames.get(0);
	}
	public Session getSession(){
		return this.session;
	}
}
//BUFFER
class AreaBuffer{
	private LinkedList<AreaElement> coda;
	private int sizeMax;

	public AreaBuffer(int sizeMax){
		coda = new LinkedList();
		this.sizeMax = sizeMax;
	}
	public boolean isEmpty(){
		return coda.isEmpty();
	}
	public boolean isFull(){
		return coda.size() >= this.sizeMax;
	}
	public synchronized void aggiungiAreaElement(AreaElement element){
		while(isFull()){
			try{
				wait();
			}catch(InterruptedException exc){
				System.err.println("InterruptedException");
			}
		}
		coda.addFirst(element);
		notifyAll();
	}
	public synchronized AreaElement prelevaAreaElement(String areaName){
		while(isEmpty()){
			try{
				wait();
			}catch(InterruptedException exc){
				System.err.println("InterruptedException");
			}
		}

		for(AreaElement e: coda){
			if(e.getAreaName() == areaName){
				int position = coda.indexOf(e);
				AreaElement element = coda.get(position);
				coda.remove(position);
				notifyAll();
				return element;
			}
		}
		//AreaElement element = coda.removeFirst();
		return null;
	}
}
//PRODUTTORE
class AreaProducer implements Runnable{
	private AreaBuffer buffer;
	private String areaName;
	private Session session;
	private AreaElement element;

	public AreaProducer(AreaBuffer buffer, String areaName, Session session){
		this.buffer = buffer;
		this.areaName = areaName;
		this.session = session;

		ArrayList<String> areas = new ArrayList();
		areas.add(this.areaName);

		this.element = new AreaElement(areas, this.session);
	}

	@Override
	public void run(){
		
		//while(true){
		//	if(buffer.contains(this.element)){
			//	break; //devo capire se dopo il break devo interrompere questo threadz
			//}
			try{
				//inserisce l'istanza di elemento nel buffer
				buffer.aggiungiAreaElement(element);//non serve un while perche' questo thread viene messo in attesa se il buffer e' pieno, per cui appena il buffer si svuota, questo aggiunge l'elemento al buffer

			}catch(Exception exc){
				System.err.println("InterruptedException");
			}
		//}
	}
}
//CONSUMATORE
class AreaConsumer implements Runnable{
	private AreaBuffer buffer;
	private String areaName;
	private AreaElement element;

	public AreaConsumer(AreaBuffer buffer, String areaName){
		this.buffer = buffer;
		this.areaName = areaName;
	}


	@Override
	public void run(){//per non stressare troppo i database e il server, si puo' chiamare questo metodo con un ritardo
		while(true){
			if(element == null){//se non c'e' un'istanza di element bisogna mettersi in coda al buffer
				try{
					AreaElement element = buffer.prelevaAreaElement(areaName);
				}catch(InterruptedException exc){
					System.err.println("InterruptedException");
				}
			}
			//chiama i metodi sull'istanza di element(nel caso questo worker abbia gia' un'istanza di element, non ci sara' bisogno di mettersi in coda al buffer
				//preleva i dati da kafka usando l'area contenuta in areaNames
                this.element.getStreetsTraffic();
                //preleva i dati da Neo4J tramite LongID
                this.element.getStreetsFromNeo4J();
                //converte i dati in formato GeoJson
                this.element.convertToFeatures();
                //invio i dati
                if (element.getSession().isOpen()) {
                    try {
                        this.element.send();
                    } catch (IOException e) {
                        System.out.println("Qualcosa e' andato storto durante l'invio del GeoJson.");
                        e.printStackTrace();
                    }
                }else{
					System.out.println("Sessione chiusa.");
				}
		}
	}

	public AreaElement getElement(){return this.element;}
}

class AreaWorker extends Thread {
    private ArrayList<String> areaNames;
    private Session session;
    private HashMap<Long, StreetMongo> streetsFromArea = new HashMap<>(); //array di strade presenti nelle aree richieste, provenienti da mongo
    private HashMap<Long, Street> streetsWithGeometry = new HashMap<>();  //array di strade contenenti un array che ne definisce la geometria, provenienti da Neo4J
    private Boolean flag1 = true;
    private Boolean running = false;
    private ConfigurationSingleton conf = ConfigurationSingleton.getInstance();
    private String uri = conf.getProperty("neo4j-core.bolt-uri");
    private String user = conf.getProperty("neo4j-core.user");
    private String password = conf.getProperty("neo4j-core.password");
    private Neo4jDAOImpl database = new Neo4jDAOImpl(uri, user, password);
    private FeatureCollection featureCollection = new FeatureCollection();
    private Gson gson = new Gson();

    public AreaWorker(ArrayList<String> areaNames, Session session) {
        this.areaNames = areaNames;
        this.session = session;
    }

    public void run() {
        System.out.println("Starting Worker: " + this.areaNames);
        running = true;
        while (!isInterrupted()) {
            try {
                //qui bisogna fare le varie operazioni di connessione ai database e di recupero dati
					//meccanismo di controllo del ciclo di vita del thread (aspetta se flag1 e' vera)
				while(flag1){
					try{
						wait();
					}catch(InterruptedException e){
						interrompi();
						System.out.println("Thread "+areaNames.get(0)+" Interrupted");
					}
				}
				//connessione a Neo4J
                database.openConnection();
                //preleva i dati da kafka usando l'area contenuta in areaNames
                getStreetsTraffic();
                //preleva i dati da Neo4J tramite LongID
                getStreetsFromNeo4J();
                //converte i dati in formato GeoJson
                convertToFeatures();
                //invio i dati
                if (session.isOpen()) {
                    try {
                        send();
                    } catch (IOException e) {
                        System.out.println("Qualcosa e' andato storto durante l'invio del GeoJson.");
                        e.printStackTrace();
                    }
                }

                disabilitate();
                wait(100);
            } catch (InterruptedException e) {
                System.out.println("Thread interrotto, operazione non completata.");
            }
        }
    }

    private void getStreetsTraffic() {
        Properties props = new Properties();

//        props.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, KafkaConfig.KAFKA_HOST_LOCAL_NAME+":"+KafkaConfig.KAFKA_PORT);
        props.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, conf.getProperty("kafka.hostname") + ":" + conf.getProperty("kafka.port"));//KafkaConfig-->classe che contiene le info del kafka che uso
        props.put(ConsumerConfig.GROUP_ID_CONFIG, "areasConsumerGroup");
        props.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class);
        props.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class);
        props.put(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest");
        System.out.println("Creo il consumer di kafka");
        KafkaConsumer<String, String> consumer = new KafkaConsumer<>(props);//#1: KEY, #2: VALUE
        consumer.subscribe(areaNames);//purtroppo il metodo subscribe vuole solo arraylist, quindi bisogna usare arraylist anche per una sola area


        while (true) {//usa una variabile booleana che viene settata a true ogni volta che un nuovo messaggio viene ricevuto
            System.out.println("Poll eseguito");
            ConsumerRecords<String, String> streetResults = consumer.poll(Duration.ofMillis(10000));
            int i = 0;
            for (ConsumerRecord<String, String> record : streetResults) {
                i++;
                String value = record.value();
                StreetMongo streetMongo = gson.fromJson(value, StreetMongo.class);
                streetsFromArea.put(Long.valueOf(streetMongo.getLinkid()), streetMongo);
//                streetsFromArea.add();
            }
            if (i != 0) {//se i!=0 l'array ha elementi, quindi esco dal while
                System.out.println("Dati prelevati da Kafka, Worker di " + areaNames.get(0)+".");
                i=0;
				consumer.close();
				break;
//                disabilitate();
            }
        }
    }

    private void getStreetsFromNeo4J() {
        System.out.println("Recuperando i dati da Neo4j...");
		try{
			ArrayList<Street> streets = this.database.getStreetsFromLinkIds(streetsFromArea.keySet());
			for (Street s: streets){
				streetsWithGeometry.put(s.getLinkId(), s);
			}
			System.out.println("Strade Recuperate");
		}catch(org.neo4j.driver.exceptions.NoSuchRecordException e){
			System.out.println("Strade non trovate, c'e' stato un problema con neo4j");
		}
        //int j=0;
//        for (StreetMongo s : streetsFromArea) {
//        for (Long key : streetsFromArea.keySet()) {
//            StreetMongo s = streetsFromArea.get(key);
            //j++;
//            long localId = Long.parseLong(s.getLinkid());
//            try {
//                Street neo4jResult = this.database.getStreet(localId);
//                //System.out.println("Risultato #" + j + ": " + neo4jResult);
//                streetsWithGeometry.add(neo4jResult);
//            } catch (org.neo4j.driver.exceptions.NoSuchRecordException e) {
//                System.out.println("Valore non trovato");
//            }
//        }
    }

    private void convertToFeatures() {
        System.out.println("Conversione dati in formato geojson...");
        for(Long key : streetsWithGeometry.keySet()){
            Street s = streetsWithGeometry.get(key);
            Properties props = new Properties();
            Geometry geoms = new Geometry();
            ArrayList<Coordinate> coord = s.getGeometry();
            for (Coordinate c : coord) {
                geoms.addGeometry(c.getLongitude(), c.getLatitude());
            }
            props.put("name", s.getName());
            if (s.getFfs() > 20)
                props.put("color", "#1199dd");
            else {
                props.put("color", "#d21f1b");
            }
            Feature feature = new Feature(geoms, props);
            if (!feature.isEmpty())
                featureCollection.addFeature(feature);
        }
    }

    private synchronized void send() throws IOException {
        if (!streetsWithGeometry.isEmpty()) {
            AreaResponse response = new AreaResponse(areaNames.get(0), featureCollection);
            //String toClient = gson.toJson(featureCollection);
            String toClient = gson.toJson(response);
            //System.out.println(toClient);
            this.session.getBasicRemote().sendText(toClient);

            System.out.println("JSON inviato al client");
        }
    }
	
		
	public synchronized void interrompi() {
		interrupt();
		flag1 = false;
	}
	

    public synchronized void abilitate() {
        this.flag1 = false;
    }

    public synchronized void disabilitate() {
        this.flag1 = true;
    }

    //if (flag1==true) {polling is Suspended} else {polling is Running}
    public Boolean getStatus() {
        return flag1;
    }
}

class AreaResponse {
    private String aName;
    private FeatureCollection collection;

    public AreaResponse(String aName, FeatureCollection collection) {
        this.aName = aName;
        this.collection = collection;
    }
}