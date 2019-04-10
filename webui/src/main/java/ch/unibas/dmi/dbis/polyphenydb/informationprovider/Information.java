package ch.unibas.dmi.dbis.polyphenydb.informationprovider;


import ch.unibas.dmi.dbis.polyphenydb.informationprovider.InformationGraph.GraphType;
import com.google.gson.Gson;


public abstract class Information <T extends Information<T>> {
    private String id;
    InformationType type;
    private String informationGroup;

    Information ( String id, String group) {
        this.id = id;
        this.informationGroup = group;
    }

    public String getId() {
        return id;
    }

    public String getGroup() {
        return informationGroup;
    }

    public T ofType ( GraphType t ) { throwError(); return (T) this; }
    public T setColor ( String color ) { throwError(); return (T) this; }
    public T setMin ( int min ) { throwError(); return (T) this; }
    public T setMax ( int max ) { throwError(); return (T) this; }

    public void updateGraph( GraphData... data ) { throwError();}
    public void updateHeader ( String header ) { throwError();}
    public void updateHtml( String header ) { throwError();}
    public void updateLink ( String label, String... routerLink ) { throwError(); }
    public void updateProgress ( int value ) { throwError(); }

    private void throwError () {
        throw new InformationException( "This method cannot be applied to Information of type "+this.getClass().getSimpleName() );
    }

    @Override
    public String toString() {
        Gson gson = new Gson();
        return gson.toJson( this );
    }
}
