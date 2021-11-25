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
        decoders = {MessageDecoder.class},
        encoders = {StreetEncoder.class, RequestEncoder.class, RequestedSquareEncoder.class}
)

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
        //Si accerta che la connessione sia stabilita
        // Registra il client in un Set
    }
    @OnMessage
    public void onMessage(Session session, Message message)throws IOException {
		if(message instanceof Street) {
            System.out.println("Messaggio: " + message);
            Street street = (Street) message;
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
            square.print(System.out);
            if(message != null)
                session.getBasicRemote().sendText("Oggetto ricevuto con successo!");
            if (square != null)
                session.getBasicRemote().sendText("Oggetto castato con successo!");
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
}

//Classi che rappresentano gli oggetti Json gestiti da questo endpoint
class Message{
    private String type;

    public Message(){}

    public void setType(String type){this.type=type;}
    public String getType(){return this.type;}
}
class Street extends Message{
    private double avgTravelTime;
    private double sdTravelTime;
    private long numVehicles;
    private long aggPeriod;
    private long domainAggTimestamp;
    private long aggTimestamp;
    private String linkid;
    private String areaName;

    public Street(){}

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
class StreetEncoder implements Encoder.Text<Street>{
    private static Gson gson = new Gson();

    @Override
    public String encode(Street street) throws EncodeException {
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
			}else if (!s.contains("type")) {
                System.out.println("Decodifica effettuata.");
                return gson.fromJson(s, Street.class);
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