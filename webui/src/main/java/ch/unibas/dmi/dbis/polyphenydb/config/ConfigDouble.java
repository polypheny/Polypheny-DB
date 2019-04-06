package ch.unibas.dmi.dbis.polyphenydb.config;


public class ConfigDouble extends ConfigNumber {

    double value;

    public ConfigDouble ( String key ) {
        super( key );
    }

    public ConfigDouble ( String key, String description ) {
        super( key, description );
    }

    @Override
    public void setObject( Object value ) {
        this.value = ( double ) value;
        notifyConfigListeners( this );
    }

    @Override
    public Double getDouble() {
        return this.value;
    }

    @Override
    public void setDouble( double value ) {
        this.value = value;
        notifyConfigListeners( this );
    }
}
