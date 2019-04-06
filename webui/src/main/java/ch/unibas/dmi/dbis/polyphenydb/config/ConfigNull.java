package ch.unibas.dmi.dbis.polyphenydb.config;


public class ConfigNull extends ConfigScalar {

    private Object value = null;

    public ConfigNull( String key ) {
        super( key );
    }

    public ConfigNull( String key, String description ) {
        super( key, description );
    }

    @Override
    public void setObject( Object o ) {
        this.value = null;
        notifyConfigListeners( this );
    }

    @Override
    public Object getObject() {
        return this.value;
    }
}
