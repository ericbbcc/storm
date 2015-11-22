package backtype.storm.entity;


/**
 * @author float.lu
 */
public class DataWithVersion {
    private byte[] data;
    private int version;
    private Object deserialized;

    public DataWithVersion(byte[] data, int version) {
        this.data = data;
        this.version = version;
    }

    public Object getDeserialized() {
        return deserialized;
    }

    public void setDeserialized(Object deserialized) {
        this.deserialized = deserialized;
    }

    public byte[] getData() {
        return data;
    }

    public void setData(byte[] data) {
        this.data = data;
    }

    public int getVersion() {
        return version;
    }

    public void setVersion(int version) {
        this.version = version;
    }
}
