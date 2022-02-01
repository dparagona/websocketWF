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
    private Map<String, AreaWorker> workers = new HashMap<>();//mappa che contiene i vari workers

    @OnOpen
    public void onOpen(Session session) throws IOException {
        this.session = session;
        // Registra la sessione in un Set
        provaEndpoints.add(this);
        if (session != null) {
            session.getBasicRemote().sendText("Connessione Accettata!");
            provaEndpoints.add(this);
        }
    }

    @OnMessage
    public void onMessage(Session session, Message message) throws IOException {
        System.out.println("flag1 modificata a TRUE");

        if (message instanceof StreetMongo) {
            System.out.println("Messaggio: " + message);
            StreetMongo street = (StreetMongo) message;
            street.print(System.out);
            session.getBasicRemote().sendText("Oggetto ricevuto con successo!");
        } else if (message instanceof AreaRequest) {
            System.out.println("Messaggio: " + message);
            AreaRequest request = (AreaRequest) message;
            request.print(System.out);
            session.getBasicRemote().sendText("Oggetto ricevuto con successo!");

        } else if (message instanceof RequestedSquare) {
            System.out.println("Messaggio: " + message);
            RequestedSquare square = (RequestedSquare) message;
            for(String key : workers.keySet()){
                workers.get(key).interrupt();
            }
            session.getBasicRemote().sendText("Riquadro ricevuto con successo!");
            //qui bisogna controllare se il riquadro ricevuto e' diverso da quello gia' in possesso di questo Endpoint
            this.square = square;//per ora faccio cosi', poi bisogna vedere se c'e' bisogno di controllare che il nuovo quadrato richiesto non sia diverso dal precedente
            this.square.print(System.out);

            ArrayList<String> areaNames = getAreaNames(this.square);//ottiene l'array delle aree da Mongo
            //stampa nella console delle aree ottenute da Mongo per debug
            int i = 0;
            System.out.println(">>AREE RICEVUTE");
            System.out.println(" ");

			//per ogni area, se la mappa non contiene un worker per n-esima area, ne crea uno, lo abilita e lo esegue, altrimenti lo recupera dalla mappa e lo abilita
            for(String s : areaNames){
				if(!workers.keySet().contains(s)){
					i++;
					System.out.println("Area #" + i + ": " + s+"   Nuova.");
					//QUI BISOGNA INVOCARE UN THREAD PER OGNI AREA, INOLTRE BISOGNA SALVARE IN UNA MAPPA TUTTI I WORKER LANCIATI
					ArrayList<String> areas = new ArrayList<>();
					areas.add(s);
					AreaWorker worker = new AreaWorker(areas, session);
					workers.put(s, worker);

					worker.abilitate();
					worker.start();
				}else{
					i++;
					System.out.println("Area #" + i + ": " + s+"   Woke Up");
					if(!workers.get(s).isInterrupted()){workers.get(s).abilitate();}
				}
            }
			
			wakeUp();
        }
    }

    @OnClose
    public void onClose(Session session) throws IOException {
        //gestisce la chiusura della connessione
        provaEndpoints.remove(this);
        for (String w : workers.keySet()) {
            workers.remove(w);//pulizia della mappa
        }
    }

    @OnError
    public void onError(Session session, Throwable throwable) throws IOException {
        throwable.printStackTrace(System.out);
    }

    //si usa per inviare i dati, questo e' costruito per una chat, percio' bisogna capire se deve essere modificato per questa applicazione
    private static void broadcast(Message message) throws IOException, EncodeException {//
        provaEndpoints.forEach(endpoint -> {
            synchronized (endpoint) {
                try {
                    endpoint.session.getBasicRemote().sendObject(message);
                } catch (IOException | EncodeException e) {
                    e.printStackTrace();
                }
            }
        });
    }
	private synchronized void wakeUp(){
		notifyAll();
	}
    private ArrayList<String> getAreaNames(RequestedSquare s) {
        float lon1 = Float.parseFloat(s.getUpperLeftCorner().substring(0, s.getUpperLeftCorner().indexOf(",")));
        float len1 = Float.parseFloat(s.getUpperLeftCorner().substring(s.getUpperLeftCorner().indexOf(",") + 1));
        float lon2 = Float.parseFloat(s.getLowerRightCorner().substring(0, s.getLowerRightCorner().indexOf(",")));
        float len2 = Float.parseFloat(s.getLowerRightCorner().substring(s.getLowerRightCorner().indexOf(",") + 1));
        return areaNameLogic.getAreaNameFromCorners(lon1, len1, lon2, len2);
    }
}

class AreaWorker extends Thread {
    private ArrayList<String> areaNames;
    private Session session;
    private final AreaNameLogic areaNameLogic = new AreaNameLogic(); //serve per ottenere le aree interne ad un riquadro
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
    int i;

    public AreaWorker(ArrayList<String> areaNames, Session session) {
        this.areaNames = areaNames;
        this.session = session;
    }

    public synchronized void run() {
        System.out.println("Starting Worker: " + this.areaNames);
        running = true;
        while (running) {
            try {
                //qui bisogna fare le varie operazioni di connessione ai database e di recupero dati
					//meccanismo di controllo del ciclo di vita del thread (aspetta se flag1 e' vera)
				while(flag1){
					try{
						wait();
					}catch(InterruptedException e){
						interrupt();
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

//        props.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, KafkaConfig.KAFKA_HOST_LOCAL_NAME+":"+KafkaConfig.KAFKA_PORT);//KafkaConfig-->classe che contiene le info del kafka che uso
        props.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, conf.getProperty("kafka.hostname") + ":" + conf.getProperty("kafka.port"));//KafkaConfig-->classe che contiene le info del kafka che uso
        props.put(ConsumerConfig.GROUP_ID_CONFIG, "areasConsumerGroup");
        props.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class);
        props.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class);
        props.put(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest");
        System.out.println("Creo il consumer");
        KafkaConsumer<String, String> consumer = new KafkaConsumer<>(props);//#1: KEY, #2: VALUE
        consumer.subscribe(areaNames);//purtroppo il metodo subscribe vuole solo arraylist, quindi bisogna usare arraylist anche per una sola area


        while (true) {//usa una variabile booleana che viene settata a true ogni volta che un nuovo messaggio viene ricevuto
            System.out.println("Poll eseguito");
            ConsumerRecords<String, String> streetResults = consumer.poll(Duration.ofMillis(10000));
            i = 0;
            for (ConsumerRecord<String, String> record : streetResults) {
                i++;
                String value = record.value();
                StreetMongo streetMongo = gson.fromJson(value, StreetMongo.class);
                streetsFromArea.put(Long.valueOf(streetMongo.getLinkid()), streetMongo);
//                streetsFromArea.add();
            }
            if (i != 0) {//se i!=0 l'array ha elementi, quindi esco dal while
                System.out.println("Dati prelevati da Kafka, Worker di " + areaNames.get(0)+".");
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
//        for (Street s : streetsWithGeometry) {
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

    @Override
    public void interrupt() {
        super.interrupt();
        running = false;
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