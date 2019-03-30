package ch.unibas.dmi.dbis.polyphenydb.webui;


import com.google.gson.Gson;
import com.google.gson.GsonBuilder;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;

/** page for the WebUi containing WebUiGroups that contain Configuration object */
public class WebUiPage {
    private Integer id;
    private String title;
    private String description;
    private String icon;
    private WebUiPage parentPage;
    private ConcurrentMap<Integer, WebUiGroup> groups = new ConcurrentHashMap<Integer, WebUiGroup>(  );

    /** @param id unique id for the page */
    public WebUiPage ( int id ) {
        this.id = id;
    }

    public WebUiPage ( int id, String title, String description ) {
        this.id = id;
        this.title = title;
        this.description = description;
    }

    public WebUiPage withIcon( String icon ) {
        this.icon = icon;
        return this;
    }

    public WebUiPage withParent ( WebUiPage parent ) {
        this.parentPage = parent;
        return this;
    }

    public boolean hasTitle () {
        return this.title != null;
    }

    /**
     * applies all attributes of page p to this existing page object
     * @param p page with more attributes
     * */
    public WebUiPage override ( WebUiPage p ) {
        if ( p.id != null ) this.id = p.id;
        if ( p.title != null ) this.title = p.title;
        if ( p.description != null ) this.description = p.description;
        if ( p.icon != null ) this.icon = p.icon;
        if ( p.parentPage != null ) this.parentPage = p.parentPage;
        return this;
    }

    /** @return id of this WebUiPage*/
    public int getId() {
        return id;
    }

    /** add a WebUiGroup for this WebUiPage */
    public void addWebUiGroup ( WebUiGroup g ) {
        if ( groups.get( g.getId() ) != null ) {
            groups.get( g.getId() ).override( g );
        }else {
            groups.put( g.getId(),g );
        }
    }

    /** @return returns WebUiPage as json object */
    @Override
    public String toString() {
        Gson gson = new GsonBuilder().enableComplexMapKeySerialization().setPrettyPrinting().create();
        //Gson gson = new Gson();
        return gson.toJson( this );
    }
}
