import java.io.File;
import java.io.IOException;
import java.io.Serializable;
import java.nio.file.Files;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Set;
import java.util.UUID;

public class Server implements ProjectLib.CommitServing {

  class Image implements Serializable{
    byte[] img;

    public Image() { }
    public Image(byte[] _img) {
      img = _img;
    }
  }

  private static ProjectLib PL;
  private static CommChannel CommChannel;

  /*
   * save necessary info in memory, the following is an example
   * Server(com:s1.jpg) --> a (a:1.jpg) -- Session 1 
   * Server(com:s1.jpg) --> b (b:2.jpg) -- Session 2
   * Server(com:s1.jpg) --> c (c:3.jpg) -- Session 3
   * ==========================================================================
   * id_source = {Session 1 = "a:1.jpg:s1.jpg", Session 2 = "b:2.jpg:s1.jpg"..}
   * f_image = {"s1.jpg" = s1.jpg binary}
   * f_PREids = {"s1.jpg" = {Session 1, Session 2, Session 3}}
   * f_sources = {"s1.jpg" = {"a:1.jpg", "b:2.jpg", "c:3.jpg"}}
   * f_TS = {"s1.jpg" = Time Stamp}
   */
  private static HashMap<String, String> id_source;
  private static HashMap<String, Image> f_image;
  private static HashMap<String, HashSet<String>> f_PREids;
  private static HashMap<String, HashSet<String>> f_sources;
  private static HashMap<String, Long> f_TS;
  
  // init all the data structure
  static{
    id_source = new HashMap<String, String>();
    f_image = new HashMap<String, Image>();
    f_PREids = new HashMap<String, HashSet<String>>();
    f_sources = new HashMap<String, HashSet<String>>();
    f_TS = new HashMap<String, Long>();
  }

  /**
   * @param filename
   * @param img
   * @param sources
   */
  public void startCommit(String filename, byte[] img, String[] sources) {
    
    snapshotIMG(filename, img);
    
    HashSet<String> set = new HashSet<String>();
    for (String source: sources) {
      set.add(source);
    }
    f_sources.put(filename, set);
    
    /* 
     * save sessionIDs in memory, the following is an example
     * Server(com:1.jpg) --> a (a:1.jpg) -- Session 1
     * Server(com:1.jpg) --> b (b:1.jpg) -- Session 2
     * Server(com:1.jpg) --> c (c:1.jpg) -- Session 3
     * ========================================================================
     * ids = {Session 1, Session 2, Session 3}
     */
    HashSet<String> ids = new HashSet<String>();
    
    // prepare
    for (String source : sources) {
      String UserNode = source.split(":")[0];
      String sessionID = UUID.randomUUID().toString().replaceAll("-", "");
      
      Packet pktSent = new Packet();
      pktSent.setFrom("Server")
             .setType(Packet.Type.PREPARE)
             .setImg(img)
             .setSource(source)
             .setSources(sources)
             .setFilename(filename)
             .setSessionID(sessionID)
             .setDestination(UserNode);
      
      ids.add(sessionID);
      
      id_source_PUT(sessionID, source + ":" + filename);
      CommChannel.sendPkt(pktSent);
    }

    f_PREids_PUT(filename, ids);
    f_COMids_PUT(filename, System.currentTimeMillis());
    f_image_PUT(filename, img);
    
    snapshotDS();
    
  }

  /**
   * Snap Shot the Image into the disk
   */
  private synchronized static void snapshotIMG(String filename, byte[] img) {
    Utils.write_Image(filename + "_tmp", img);
  }
  
  /**
   * Snap Shot the Data Structure into the disk
   */
  private synchronized static void snapshotDS() {
    Set<String> keySet = f_image.keySet();
    HashSet<String> set = new HashSet<String>();
    set.addAll(keySet);
    Utils.write_object("f_image_keys", set);
    Utils.write_object("f_PREids", f_PREids);
    Utils.write_object("f_COMids", f_TS);
    Utils.write_object("id_source", id_source);
    Utils.write_object("f_sources", f_sources);
  }

  /**
   * Read the Snap Shot from the disk
   */
  private static void readSnapshot() {
    Object f_image_object = Utils.read_object("f_image_keys");
    
    if(f_image_object != null){
      HashSet<String> f_image_files = (HashSet<String>) f_image_object;
      for (String filename : f_image_files) {
        byte[] img = Utils.read_image(filename + "_tmp");
        Image image = new Server().new Image(img);
        f_image.put(filename, image);
      }
    }
    
    Object f_PREids_object = Utils.read_object("f_PREids");
    Object f_COMids_object = Utils.read_object("f_COMids");
    Object id_source_object = Utils.read_object("id_source");
    Object f_sources_object = Utils.read_object("f_sources");
    
    if(f_PREids_object != null)
      f_PREids = (HashMap<String, HashSet<String>>) f_PREids_object;
    
    if(f_sources_object != null)
      f_sources = (HashMap<String, HashSet<String>>) f_sources_object;
    
    // flush the commited ones
    for (String filename : f_PREids.keySet()) {
      File tmpFile = new File(filename + "_tmp");
      File file = new File(filename);
      if(f_PREids.get(filename).isEmpty() && tmpFile.exists()){
        tmpFile.renameTo(file);
      }
      
      if(f_PREids.get(filename).isEmpty()){
        HashSet<String> set = f_sources.get(filename);
        
        if(set == null){
          System.err.println("Set is NULL!!");
          break;
        }
        
        String[] sources = new String[set.size()];
        
        int index = 0;
        for (String source : set) {
          sources[index] = source;
          index++;
        }
        
        // Resend COMMIT messages in case the messages were not sent
        for (String source : sources) {
          String UserNode = source.split(":")[0];
          
          Packet pktSent = new Packet();
          pktSent.setType(Packet.Type.COMMIT)
                 .setReply(Packet.Reply.OK)
                 .setFilename(filename)
                 .setSource(source)
                 .setSources(sources)
                 .setDestination(UserNode);
   
          CommChannel.sendPkt(pktSent);
        }
      }
    }
    
    if(f_COMids_object != null)
      f_TS = (HashMap<String, Long>) f_COMids_object;
    
    if(id_source_object != null)
      id_source = (HashMap<String, String>) id_source_object;
  }
  
  /**
   * Put necessary info in f_image in a thread-safe fashion
   * @param filename
   * @param img
   */
  private synchronized void f_image_PUT(String filename, byte[] img) {
    f_image.put(filename, new Image(img));    
  }

  /**
   * Put necessary info in f_COMids in a thread-safe fashion
   * @param filename
   * @param ids
   */
  private synchronized void f_COMids_PUT(String filename, long timestamp) {
    f_TS.put(filename, timestamp);  
  }

  /**
   * Put necessary info in f_PREids in a thread-safe fashion
   * @param filename
   * @param ids
   */
  private synchronized void f_PREids_PUT(String filename, HashSet<String> ids) {
    f_PREids.put(filename, ids);    
  }

  /**
   * Put necessary info in id_source in a thread-safe fashion
   * @param sessionID
   * @param source
   */
  private synchronized void id_source_PUT(String sessionID, String source) {
    id_source.put(sessionID, source);
  }

  public static void main(String args[]) throws Exception {
    if (args.length != 1)
      throw new Exception("Need 1 arg: <port>");
    
    Server srv = new Server();
    PL = new ProjectLib(Integer.parseInt(args[0]), srv);
    Utils.PL = PL;
    
    // Start the thread used for retransmission
    CommChannel = new CommChannel(PL);
    Thread CommThread = new Thread(CommChannel);
    CommThread.start();
    
    readSnapshot();
    // main loop for handling messages
    while (true) {
      ProjectLib.Message msg = PL.getMessage();
      Packet pktRcv = (Packet) Utils.deSerialize(msg.body);
      packetHandler(pktRcv);
    }
  }
  
  /**
   * The main logic for handling messages.
   * @param pktRcv
   */
  public synchronized static void packetHandler(Packet pktRcv) {
    CommChannel.rmPkt(pktRcv.getSessionID());
    Packet.Reply reply = pktRcv.getReply();
    switch (pktRcv.type) {
      case ACK_PRE:
        
        long timeStamp = f_TS.get(pktRcv.getFilename());
        long curTime = System.currentTimeMillis();
        
        // If prepare all successful
        // tell all the nodes to commit
        if(reply == Packet.Reply.OK){
          String filename = pktRcv.getFilename();
          String sessionID = pktRcv.getSessionID();
          HashSet<String> PREids = f_PREids.get(filename); 

          // in case PREids is not ready
          while(PREids == null){
            PREids = f_PREids.get(filename);
          }
          
          PREids.remove(sessionID);
          snapshotDS();
          
          // If all PRE_OK are got, then commit the transaction
          if(PREids.isEmpty()){
            String[] sources = pktRcv.getSources();

            File tmpfile = new File(filename + "_tmp");
            File file = new File(filename);
            
            PL.fsync();
            
            if(tmpfile.exists()) {
              try {
                Files.copy( tmpfile.toPath(), file.toPath() );
              } catch (IOException e) {
                e.printStackTrace();
              }
              tmpfile.delete();
              PL.fsync();
            }
            
            for (String source : sources) {
              String UserNode = source.split(":")[0];
              Packet pktSent = new Packet();
              pktSent.setType(Packet.Type.COMMIT)
                     .setReply(Packet.Reply.OK)
                     .setFilename(pktRcv.getFilename())
                     .setSource(source)
                     .setSources(sources)
                     .setSessionID(pktRcv.getSessionID())
                     .setDestination(UserNode);
              CommChannel.sendPkt(pktSent);

            }

          }
        }
        
        // If prepare not successful
        // tell all the nodes to release the resource
        if(reply == Packet.Reply.NO || curTime - timeStamp > 6000){
          String[] sources = pktRcv.getSources();
          
          for (String source : sources) {
            String UserNode = source.split(":")[0];
            
            Packet pktSent = new Packet();
            pktSent.setType(Packet.Type.COMMIT)
                   .setReply(Packet.Reply.NO)
                   .setFilename(pktRcv.getFilename())
                   .setSource(source)
                   .setSources(sources)
                   .setSessionID(pktRcv.getSessionID())
                   .setDestination(UserNode);
     
            CommChannel.sendPkt(pktSent);
            
            File file = new File(pktRcv.getFilename() + "_tmp");
            file.delete();
          }
        }
        
        break;
        
    }
    
    snapshotDS();
  }

}
