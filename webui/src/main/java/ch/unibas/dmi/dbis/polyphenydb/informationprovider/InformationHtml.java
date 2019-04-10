package ch.unibas.dmi.dbis.polyphenydb.informationprovider;


import com.google.gson.Gson;
import com.google.gson.GsonBuilder;
import com.google.gson.internal.GsonBuildConfig;


public class InformationHtml extends Information {

    private String html;

    public InformationHtml ( String id, String group, String html ) {
        super( id, group );
        this.type = InformationType.HTML;
        this.html = html;
    }

    /*@Override
    public String toString() {
        Gson gson = new GsonBuilder().disableHtmlEscaping().create();
        return gson.toJson( this );
    }*/
}
