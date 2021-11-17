package websocket;

import com.google.gson.Gson;

import javax.websocket.*;
import javax.websocket.server.*;
import java.io.IOException;
import java.io.PrintStream;
import java.util.Set;
import java.util.concurrent.CopyOnWriteArraySet;

//per richiedere questo servizio serve specificare 'ws:' nell'url al posto del protocollo
@ServerEndpoint(
        value = "/prova1",
        decoders = MessageDecoder.class,
        encoders = MessageEncoder.class)

public class prova1 {

    private Session session;
    private static Set<prova1> provaEndpoints = new CopyOnWriteArraySet<>();

    @OnOpen
    public void onOpen(Session session)throws IOException {
        this.session = session;
        provaEndpoints.add(this);
        if(session != null){
            session.getBasicRemote().sendText("Connessione Accettata!");
		}
        //analizza la richiesta
        //recupera i dati
        //crea un thread che invia i dati
    }
    @OnMessage
    public void onMessage(Session session, Message message)throws IOException {
		System.out.println("Messaggio: "+message);
        message.print(System.out);
        if(message != null)
            session.getBasicRemote().sendText("Oggetto ricevuto con successo!");
   }
    @OnClose
    public void onClose(Session session)throws IOException{
        //gestisce la chiusura della connessione
        provaEndpoints.remove(this);
    }
    @OnError
    public void onError(Session session, Throwable throwable)throws IOException{
        //gestione eccezioni
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
}

//Classe che rappresenta gli oggetti Json restituiti da questo endpoint
class Message {
    private double avgTravelTime;
    private double sdTravelTime;
    private long numVehicles;
    private long aggPeriod;
    private long domainAggTimestamp;
    private long aggTimestamp;
    private String linkid;
    private String areaName;

    public Message(){}

    public void setAvgTravelTime(double avgTravelTime) {this.avgTravelTime = avgTravelTime;}
    public void setSdTravelTime(double sdTravelTime) {this.sdTravelTime = sdTravelTime;}
    public void setNumVehicles(long numVehicles) {this.numVehicles = numVehicles;}
    public void setAggPeriod(long aggPeriod) {this.aggPeriod = aggPeriod;}
    public void setDomainAggTimestamp(long domainAggTimestamp) {this.domainAggTimestamp = domainAggTimestamp;}
    public void setAggTimestamp(long aggTimestamp) {this.aggTimestamp = aggTimestamp;}
    public void setLinkid(String linkid) {this.linkid = linkid;}
    public void setAreaName(String areaName) {this.areaName = areaName;}

    public double getAvgTravelTime() {return this.avgTravelTime;}
    public double getSdTravelTime() {return this.sdTravelTime;}
    public long getNumVehicles() {return this.numVehicles;}
    public long getAggPeriod() {return this.aggPeriod;}
    public long getDomainAggTimestamp() {return this.domainAggTimestamp;}
    public long getAggTimestamp() {return this.aggTimestamp;}
    public String getLinkid() {return this.linkid;}
    public String getAreaName() {return this.areaName;}

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
// Esempio elemento da restituire
//{"avgTravelTime":9.4185001373291,"sdTravelTime":0.0,"numVehicles":1,"aggPeriod":179000,"domainAggTimestamp":1536186598000,"aggTimestamp":1626183204071,"linkid":"12500009324848","areaName":"Albigny-sur-Saone"}

class MessageEncoder implements Encoder.Text<Message>{
    private static Gson gson = new Gson();

    @Override
    public String encode(Message object) throws EncodeException {
		System.out.println("Codifica effettuata.");
        return gson.toJson(object);
    }

    @Override
    public void init(EndpointConfig config) {    }

    @Override
    public void destroy() {    }

}
class MessageDecoder implements Decoder.Text<Message>{
    private static Gson gson = new Gson();

    @Override
    public Message decode(String s) throws DecodeException {
		System.out.println("Decodifica effettuata.");
        return gson.fromJson(s, Message.class);
    }

    @Override
    public boolean willDecode(String s) {
        return (s != null);
    }

    @Override
    public void init(EndpointConfig config) {    }

    @Override
    public void destroy() {    }

}