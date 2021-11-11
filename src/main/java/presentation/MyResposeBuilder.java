package presentation;

import javax.ws.rs.core.MediaType;
import javax.ws.rs.core.Response;
import java.io.File;

public class MyResposeBuilder {

    private MyResposeBuilder(){
        throw new IllegalStateException("Utility class");
    }

    public static Response createResponse(Response.Status status) {
        return MyResposeBuilder.createResponse(status, null);
    }

    public static Response createResponse(Response.Status status, Object response) {
        return Response.status(status)
                .header("Access-Control-Allow-Origin", "*")
                .header("Access-Control-Allow-Headers", "origin, content-type, accept, authorization")
                .header("Access-Control-Allow-Credentials", "true")
                .header("Access-Control-Allow-Methods", "GET, POST, PUT, DELETE, OPTIONS, HEAD")
                .header("Access-Control-Max-Age", "1209600")
                .entity(response)
                .build();
    }

    public static Response createResponseOctetStream(File file) {
        return Response.ok(file, MediaType.APPLICATION_OCTET_STREAM)
                .header("Access-Control-Allow-Origin", "*")
                .header("Access-Control-Allow-Headers", "origin, content-type, accept, authorization")
                .header("Access-Control-Allow-Credentials", "true")
                .header("Access-Control-Allow-Methods", "GET, POST, PUT, DELETE, OPTIONS, HEAD")
                .header("Access-Control-Max-Age", "1209600")
                .header("Content-Disposition", "attachment; filename=\"" + file.getName() + "\"") //optional
                .build();
    }
}
