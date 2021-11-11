package data.model;


import com.mongodb.client.model.geojson.Polygon;

public class Area {

    private Integer insee_com;
    private String nom_com;
    private Polygon polygon;

    public Area(){

    }

    public Area(Integer insee_com, String nom_com, Polygon polygon) {
        this.insee_com = insee_com;
        this.nom_com = nom_com;
        this.polygon = polygon;
    }

    public Integer getInsee_com() {
        return insee_com;
    }

    public void setInsee_com(Integer isee_com) {
        this.insee_com = isee_com;
    }

    public String getNom_com() {
        return nom_com;
    }

    public void setNom_com(String nom_com) {
        this.nom_com = nom_com;
    }

    public Polygon getPolygon() {
        return polygon;
    }

    public void setPolygon(Polygon polygon) {
        this.polygon = polygon;
    }

    @Override
    public String toString() {
        return "Area{" +
                "isee_com='" + insee_com + '\'' +
                ", nom_com='" + nom_com + '\'' +
                ", polygon='" + polygon + '\'' +
                '}';
    }
}
