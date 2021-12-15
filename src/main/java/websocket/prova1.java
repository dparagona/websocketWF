package websocket;

import com.google.gson.Gson;
import data.model.Street;
import logic.areaName.AreaNameLogic;
import logic.roadNetwork.RoadNetworkLogicLocal;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.common.serialization.StringDeserializer;

import javax.websocket.*;
import javax.websocket.server.*;
import java.io.IOException;
import java.io.PrintStream;
import java.time.Duration;
import java.util.ArrayList;
import java.util.Properties;
import java.util.Set;
import java.util.concurrent.CopyOnWriteArraySet;

//per richiedere questo servizio serve specificare 'ws:' nell'url al posto del protocollo
@ServerEndpoint(
        value = "/prova1",
        decoders = {MessageDecoder.class},
        encoders = {StreetEncoder.class, RequestEncoder.class, RequestedSquareEncoder.class}
)

public class prova1 {

    private Session session;
    private RoadNetworkLogicLocal roadNetworkLogic;
    private static Set<prova1> provaEndpoints = new CopyOnWriteArraySet<>();
    private RequestedSquare square;
    private final AreaNameLogic areaNameLogic = new AreaNameLogic(); //serve per ottenere le aree interne ad un riquadro
    private ArrayList<String> valuesFromArea = new ArrayList<>();//array contenente i valori restituiti da kafka
    private ArrayList<StreetMongo> streetsFromArea = new ArrayList<>(); //array di strade presenti nelle aree richieste, provenienti da mongo
    private ArrayList<Street> streetsWithGeometry = new ArrayList<>();  //array di strade contenenti un array che ne definisce la geometria, provenienti da Neo4J
    private Boolean flag1 = false;//non sono ancora del tutto sicuro che la gestione di questa flag funzioni correttamente
    @OnOpen
    public void onOpen(Session session)throws IOException {
        this.session = session;
        provaEndpoints.add(this);
        if(session != null){
            session.getBasicRemote().sendText("Connessione Accettata!");
            provaEndpoints.add(this);
		}
        //Si accerta che la connessione sia stabilita
        // Registra il client in un Set
    }
    @OnMessage
    public void onMessage(Session session, Message message)throws IOException {
        flag1 = true;
        session.getBasicRemote().sendText("Messaggio Ricevuto!");
        System.out.println("flag1 modificata a TRUE");

		if(message instanceof StreetMongo) {
            System.out.println("Messaggio: " + message);
            StreetMongo street = (StreetMongo) message;
            street.print(System.out);
            if (message != null)
                session.getBasicRemote().sendText("Oggetto ricevuto con successo!");
            if (street != null)
                session.getBasicRemote().sendText("Oggetto castato con successo!");
        }else if (message instanceof AreaRequest){
            System.out.println("Messaggio: " + message);
            AreaRequest request = (AreaRequest) message;
            request.print(System.out);
            if(message != null)
                session.getBasicRemote().sendText("Oggetto ricevuto con successo!");
            if (request != null)
                session.getBasicRemote().sendText("Oggetto castato con successo!");
        }else if (message instanceof RequestedSquare){
			System.out.println("Messaggio: " + message);
            RequestedSquare square = (RequestedSquare) message;
            if(message != null && square != null) {
                session.getBasicRemote().sendText("Oggetto ricevuto con successo!");
                session.getBasicRemote().sendText("Oggetto castato con successo!");
                //qui bisogna controllare se il riquadro ricevuto e' diverso da quello gia' in possesso di questo Endpoint
                this.square = square;//per ora faccio cosi', poi bisogna vedere se c'e' bisogno di controllare che il nuovo quadrato richiesto non sia diverso dal precedente
                this.square.print(System.out);

                ArrayList<String> areaNames = getAreaNames(this.square);//ottiene l'array delle aree da Mongo
                //stampa nella console delle aree ottenute da Mongo per debug
                int i=0;
                System.out.println(">>AREE RICEVUTE");
                System.out.println(" ");

                for(String s : areaNames){
                    i++;
                    System.out.println("Area #"+i+": "+s);
                }
                getStreetsTraffic(areaNames);//preleva i dati da kafka
                //dovrebbe chiedere i dati a neo4j

                System.out.println("Recuperando i dati da Neo4j....");
                //for(StreetMongo s : streetsFromArea){
                  //  streetsWithGeometry.add(roadNetworkLogic.getStreet(Long.parseLong(s.getLinkid())));
               // }
                Long id = Long.parseLong(streetsFromArea.get(1).getLinkid());
                System.out.println(id);
                System.out.println("Risultato da neo4j: "+roadNetworkLogic.getStreet(id));
                
                System.out.println("Nomi delle strade ricevute: ");
                for(Street s: streetsWithGeometry){
                    System.out.println(s.getName());//stampo il nome delle strade ottenute da neo4j per debug
                }
            }
            else{
                //never reached
                session.getBasicRemote().sendText("Messaggio o Richiesta non ricevute con successo.");
            }
            //qui bisogna usare un'interazione con Mongo per ottenere i nommi delle aree all'interno del riquadro DONE
            //ottenuto cio', si puo' creare un Consumer per i vari topic corrispondenti alle aree ottenute
            //per poi inviare al client i JSON corrispondenti alle strade prelevati da Neo4J grazie ai nomi delle aree
            //ottenuti, per infine filtrare le strade in base a quelle che ricadono nel riquadro
		}
   }

    @OnClose
    public void onClose(Session session)throws IOException{
        //gestisce la chiusura della connessione
        provaEndpoints.remove(this);
    }
    @OnError
    public void onError(Session session, Throwable throwable)throws IOException{
        throwable.printStackTrace(System.out);
    }
    //si usa per inviare i dati, questo e' costruito per una chat, percio' bisogna capire se deve essere modificato per questa applicazione
    private static void broadcast(Message message)throws IOException, EncodeException{//
        provaEndpoints.forEach(endpoint -> {
               synchronized (endpoint) {
                        try{
                            endpoint.session.getBasicRemote().sendObject(message);
                        }catch(IOException | EncodeException e){
                            e.printStackTrace();
                   }
               }
        });
    }

    private ArrayList<String> getAreaNames(RequestedSquare s){
        float lon1 = Float.parseFloat(s.getUpperLeftCorner().substring(0, s.getUpperLeftCorner().indexOf(",")));
        float len1 = Float.parseFloat(s.getUpperLeftCorner().substring(s.getUpperLeftCorner().indexOf(",")+1));
        float lon2 = Float.parseFloat(s.getLowerRightCorner().substring(0, s.getLowerRightCorner().indexOf(",")));
        float len2 = Float.parseFloat(s.getLowerRightCorner().substring(s.getLowerRightCorner().indexOf(",")+1));
        return areaNameLogic.getAreaNameFromCorners(lon1, len1, lon2, len2);
    }
    private void getStreetsTraffic(ArrayList<String> areaNames){
        Properties props = new Properties();

        props.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, KafkaConfig.KAFKA_HOST_LOCAL_NAME+":"+KafkaConfig.KAFKA_PORT);//KafkaConfig-->classe che contiene le info del kafka che uso
        props.put(ConsumerConfig.GROUP_ID_CONFIG, "areasConsumerGroup");
        props.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class);
        props.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class);
        props.put(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest"); //solo se necessaria, implica molti messaggi aggiuntivi
        System.out.println("Creo il consumer");
        KafkaConsumer<String, String> consumer = new KafkaConsumer<>(props);//#1: KEY, #2: VALUE
        consumer.subscribe(areaNames);
        Gson gson = new Gson();

        while(flag1){//usa una variabile booleana che viene settata a false ogni volta che un nuovo messaggio viene ricevuto
            System.out.println("While eseguito");
            ConsumerRecords<String, String> streetResults = consumer.poll(Duration.ofMillis(10000));
            int i=0;
            for(ConsumerRecord<String, String> record: streetResults){
                i++;
                System.out.println("For eseguito "+i+" volte.");
                //String key = record.key(); //mi restituisce sempre null
                String value = record.value();
                //String topic = record.topic();
                //int partition = record.partition();
                //long offset = record.offset();
                //qui si elabora il messaggio
                //System.out.println("RECORD#"+i+": "+ "\n KEY: "+key+ "\n VALUE: "+value+ "\n TOPIC: "+topic+ "\n PARTITION: "+partition+ "\n OFFSET: "+offset);//stampa delle strade ottenute da Kafka per debug
                //System.out.println("VALUE: "+value);
                streetsFromArea.add(gson.fromJson(value, StreetMongo.class));
            }
            if(i != 0) {//se i!=0 l'array ha elementi, quindi esco dal while
                flag1 = false;
                System.out.println("flag1 modificato a FALSE");
            }
        }
    }
}

//Classi che rappresentano gli oggetti Json gestiti da questo endpoint
class Message{
    private String type;

    public Message(){}

    public void setType(String type){this.type=type;}
    public String getType(){return this.type;}
}
class StreetMongo extends Message{
    private double avgTravelTime;
    private double sdTravelTime;
    private long numVehicles;
    private long aggPeriod;
    private long domainAggTimestamp;
    private long aggTimestamp;
    private String linkid;
    private String areaName;

    public StreetMongo(){}

    public void setAvgTravelTime(double avgTravelTime) {this.avgTravelTime = avgTravelTime;}
    public void setSdTravelTime(double sdTravelTime) {this.sdTravelTime = sdTravelTime;}
    public void setNumVehicles(long numVehicles) {this.numVehicles = numVehicles;}
    public void setAggPeriod(long aggPeriod) {this.aggPeriod = aggPeriod;}
    public void setDomainAggTimestamp(long domainAggTimestamp) {this.domainAggTimestamp = domainAggTimestamp;}
    public void setAggTimestamp(long aggTimestamp) {this.aggTimestamp = aggTimestamp;}
    public void setLinkid(String linkid) {this.linkid = linkid;}
    public void setAname(String areaName) {this.areaName = areaName;}

    public double getAvgTravelTime() {return this.avgTravelTime;}
    public double getSdTravelTime() {return this.sdTravelTime;}
    public long getNumVehicles() {return this.numVehicles;}
    public long getAggPeriod() {return this.aggPeriod;}
    public long getDomainAggTimestamp() {return this.domainAggTimestamp;}
    public long getAggTimestamp() {return this.aggTimestamp;}
    public String getLinkid() {return this.linkid;}
    public String getAname() {return this.areaName;}

    public void print(PrintStream ps){
        ps.println(     "\nLinkid: "+this.linkid
                        +"\nAreaName: "+this.areaName
                        +"\nAvgTravelTime: "+this.avgTravelTime
                        +"\nSdTravelTime: "+this.sdTravelTime
                        +"\nNumVehicles: "+this.numVehicles
                        +"\nAggPeriod: "+this.aggPeriod
                        +"\nDomainAggTimestamp: "+this.domainAggTimestamp
                        +"\nAggTimestamp: "+this.aggTimestamp);
    }
}
class AreaRequest extends Message{
    private String areaname;
    private int zoom;
    private int decimateSkip;

    public AreaRequest(){}

    public void setAname(String areaName){this.areaname=areaName;}
    //public void setType(String type){this.type=type;}
    public void setZoom(int zoom){this.zoom=zoom;}
    public void setDecimateSkip(int decimateSkip){this.decimateSkip=decimateSkip;}

    public String getAname(){return this.areaname;}
    //public String getType(){return this.type;}
    public int getZoom(){return this.zoom;}
    public int getDecimateSkip(){return this.decimateSkip;}

    public void print(PrintStream ps){
        String temp = super.getType();
        ps.println(">REQUEST FROM CLIENT"
                +"\nAreaName: "+this.areaname
                +"\nZoom: "+this.zoom
                +"\nType: "+temp
                +"\nDecimateSkip: "+this.decimateSkip);
    }
}
class RequestedSquare extends Message{
	private String upperLeftCorner;
	private String lowerRightCorner;
	
	public RequestedSquare(){}
	
	public void setUpperLeftCorner(String upperLeftCorner){this.upperLeftCorner=upperLeftCorner;}
	public void setLowerRightCornerLat(String lowerRightCorner){this.lowerRightCorner=lowerRightCorner;}

	public String getUpperLeftCorner(){return this.upperLeftCorner;}
	public String getLowerRightCorner(){return this.lowerRightCorner;}
	
	public void print(PrintStream ps){
		String temp = super.getType();
		ps.println(">>SQUARE FROM CLIENT"
				+"\nType: "+temp
				+"\nUpper Left Corner: "+this.upperLeftCorner
				+"\nLower Right Corner: "+this.lowerRightCorner);
	}
}
// Esempio elemento da restituire
//{"avgTravelTime":9.4185001373291,"sdTravelTime":0.0,"numVehicles":1,"aggPeriod":179000,"domainAggTimestamp":1536186598000,"aggTimestamp":1626183204071,"linkid":"12500009324848","areaName":"Albigny-sur-Saone"}

//ENCODERS
class StreetEncoder implements Encoder.Text<StreetMongo>{
    private static Gson gson = new Gson();

    @Override
    public String encode(StreetMongo street) throws EncodeException {
            return gson.toJson(street);
    }

    @Override
    public void init(EndpointConfig config) {    }

    @Override
    public void destroy() {    }

}
class RequestEncoder implements Encoder.Text<AreaRequest>{
    private static Gson gson = new Gson();

    @Override
    public String encode(AreaRequest request) throws EncodeException {
        return gson.toJson(request);
    }

    @Override
    public void init(EndpointConfig config) {    }

    @Override
    public void destroy() {    }

}
class RequestedSquareEncoder implements Encoder.Text<RequestedSquare>{
	private static Gson gson = new Gson();

	@Override
    public String encode(RequestedSquare square) throws EncodeException {
        return gson.toJson(square);
    }

    @Override
    public void init(EndpointConfig config) {    }

    @Override
    public void destroy() {    }
}

//DECODER
class MessageDecoder implements Decoder.Text<Message>{
    private static Gson gson = new Gson();

    @Override
    public Message decode(String s) throws DecodeException {

        if(willDecode(s)) {
            if (s.contains("geojson")) {
                System.out.println("Decodifica effettuata.");
                return gson.fromJson(s, AreaRequest.class);
            } else if (s.contains("RequestedSquare")){
				 System.out.println("Decodifica effettuata.");
		         return gson.fromJson(s, RequestedSquare.class);
			}else if (!s.contains("RequestedSquare") && !s.contains("geojson")) {
                System.out.println("Decodifica effettuata.");
                return gson.fromJson(s, StreetMongo.class);
            } 
        }
        return null;
    }

    @Override
    public boolean willDecode(String s) {return (s != null);}

    @Override
    public void init(EndpointConfig config) {    }

    @Override
    public void destroy() {    }
}
//Utility
class KafkaConfig{
    public final static String KAFKA_HOST_LOCAL_NAME = "kafka.promenade-lyon";
    public final static String KAFKA_PORT = "9092";
}