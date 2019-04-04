package ch.unibas.dmi.dbis.polyphenydb.config;


import java.math.BigDecimal;


public class ConfigDecimal extends ConfigNumber {

    private BigDecimal value;

    public ConfigDecimal ( String key ) {
        super( key );
    }

    public ConfigDecimal ( String key, String description ) {
        super( key, description );
    }

    @Override
    public void setObject( Object value ) {
        this.value = ( BigDecimal ) value;
    }

    @Override
    public BigDecimal getDecimal() {
        return this.value;
    }

    @Override
    public void setDecimal( BigDecimal value ) {
        this.value = value;
    }

}
