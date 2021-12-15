package presentation.AreaService;

import javax.ws.rs.*;
import javax.ws.rs.core.MediaType;
import javax.ws.rs.core.Response;

@Produces(MediaType.APPLICATION_JSON)
@Consumes(MediaType.APPLICATION_JSON)
public interface AreaServiceAPI {

    @GET
    @Path("/areas")
    public Response getAreaNameFromCorners(
            @QueryParam("upperLeft") String upperLeft,
            @QueryParam("lowerRight") String lowerRight
    );

    @GET
    @Path("/streets")
    public Response searchStreets(@QueryParam("areaname") String areaname,
                                  @QueryParam("zoom") int zoom,
                                  @DefaultValue("street") @QueryParam("type") String type,
                                  @DefaultValue("0") @QueryParam("decimateSkip") int decimateSkip);

    @GET
    @Path("/streets/{linkId}")
    public Response getStreet(@PathParam("linkId") long linkId);

}