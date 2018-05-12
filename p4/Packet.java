import java.io.Serializable;
import java.util.UUID;

public class Packet implements Serializable, Comparable<Packet> {
  enum Type implements Serializable {
    PREPARE, ACK_PRE, COMMIT, ACK_COM,
    SYN, ACK_SYN, FIN, ACK_FIN
  }

  enum Reply implements Serializable {
    OK, NO
  }

  public String ID;
  public String ACK_ID;
  public String SessionID;
  public Type type;
  public Reply reply;
  public byte[] img;
  public String[] sources;
  public String source;
  public String filename;
  public String from;
  public String destination;
  public String msg;
  
  //used for time checking: pkt ignored if older than 3 seconds
  public long timeStamp; 

  public Packet() {
    super();
    this.ID = UUID.randomUUID().toString().replaceAll("-", "");
  }

  public Packet(Type type, byte[] img, String[] sources) {
    super();
    this.type = type;
    this.img = img;
    this.sources = sources;
    this.ID = UUID.randomUUID().toString().replaceAll("-", "");
  }
  
  public String getFrom() {
    return from;
  }

  public Packet setFrom(String from) {
    this.from = from;
    return this;
  }
  
  public String getSessionID() {
    return SessionID;
  }

  public String getID() {
    return ID;
  }

  public Packet setID(String iD) {
    ID = iD;
    return this;
  }

  public String getACK_ID() {
    return ACK_ID;
  }

  public Packet setACK_ID(String aCK_ID) {
    ACK_ID = aCK_ID;
    return this;
  }

  public Packet setSessionID(String sessionID) {
    SessionID = sessionID;
    return this;
  }

  public String getSource() {
    return source;
  }

  public Packet setSource(String source) {
    this.source = source;
    return this;
  }

  public String getFilename() {
    return filename;
  }

  public Packet setFilename(String filename) {
    this.filename = filename;
    return this;
  }

  public String getDestination() {
    return destination;
  }

  public Packet setDestination(String destination) {
    this.destination = destination;
    return this;
  }

  public Reply getReply() {
    return reply;
  }

  public Packet setReply(Reply reply) {
    this.reply = reply;
    return this;
  }

  public Type getType() {
    return type;
  }

  public Packet setType(Type type) {
    this.type = type;
    return this;
  }

  public byte[] getImg() {
    return img;
  }

  public Packet setImg(byte[] img) {
    this.img = img;
    return this;
  }

  public String[] getSources() {
    return sources;
  }

  public Packet setSources(String[] sources) {
    this.sources = sources;
    return this;
  }

  public String getMsg() {
    return msg;
  }

  public Packet setMsg(String msg) {
    this.msg = msg;
    return this;
  }

  public long getTimeStamp() {
    return timeStamp;
  }

  public Packet setTimeStamp(long timeStamp) {
    this.timeStamp = timeStamp;
    return this;
  }

  @Override
  public int compareTo(Packet pkt) {
    return (int) (this.timeStamp - pkt.getTimeStamp());
  }
}
