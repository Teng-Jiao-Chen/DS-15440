import java.io.File;
import java.util.HashMap;

public class UserNode implements ProjectLib.MessageHandling {
  public static String myId;
  private static ProjectLib PL;
  private static UserNode UN;
  private static CommChannel CommChannel;
  public static HashMap<String, String> img_comb;

  public UserNode(String id) {
    myId = id;
  }

  /**
   * A callback method to receive messages from "Server"
   * and respond back
   */
  public boolean deliverMessage(ProjectLib.Message msg) {
    Packet pktRcv = (Packet)Utils.deSerialize(msg.body);
    Packet pktReply = new Packet();
    CommChannel.rmPkt(pktRcv.getSessionID());
    
    String mysource = "";
    
    switch (pktRcv.type) {
      case PREPARE:
        pktReply.setType(Packet.Type.ACK_PRE);
        
        for (String source : pktRcv.sources) {
          if(source.startsWith(myId)){
            String imgName = source.split(":")[1];
            mysource = mysource + imgName + " ";
          }
        }
        mysource.trim();
        String[] mysources = mysource.split(" ");
        
        boolean notUsed = true;

        // Check whether the image has already been used
        synchronized (img_comb) {
          for (String source : mysources) {
            String filename = pktRcv.getFilename();
            
            if(img_comb.containsKey(source) && !img_comb.get(source).equals(filename)){
              notUsed = false;
              break;
            }
            
            img_comb.put(source, filename);
          }
        }
        
        boolean userAgree = PL.askUser(pktRcv.img, mysources);
        boolean canUse = notUsed && userAgree;
        
        if(canUse) pktReply.setReply(Packet.Reply.OK);
        else pktReply.setReply(Packet.Reply.NO);

        pktReply.setFrom(myId)
                .setACK_ID(pktRcv.getID())
                .setSessionID(pktRcv.getSessionID())
                .setSource(pktRcv.getSource())
                .setSources(pktRcv.getSources())
                .setFilename(pktRcv.getFilename())
                .setDestination("Server");

        CommChannel.sendPkt(pktReply);
        
        break;
      case COMMIT:
        synchronized (this) {
          
          // delete the image if reply is OK
          if(pktRcv.getReply() == Packet.Reply.OK){
            String myfile = pktRcv.getSource().split(":")[1];
            File file = new File(myfile);
            file.delete();
          } else{ // release it from used_image if not OK for other transactions
            for (String source : pktRcv.sources) {
              if(source.startsWith(myId)){
                String imgName = source.split(":")[1];
                synchronized (img_comb){
                  img_comb.remove(imgName);
                }
              }
            }
          }
  
        }
        
        break;
    }
    
    snapshotDS();
    return true;
  }

  /**
   * Snap Shot the Data Structure into the disk
   */
  private synchronized static void snapshotDS() {
    Utils.write_object("img_comb", img_comb);
  }
  
  /**
   * Read the Snap Shot from the disk
   */
  private static void readSnapshot(){
    Object img_comb_object = Utils.read_object("img_comb");
    
    if(img_comb_object != null)
      img_comb = (HashMap<String, String>) img_comb_object;
  }
  
  public static void main(String args[]) throws Exception {
    if (args.length != 2)
      throw new Exception("Need 2 args: <port> <id>");
    
    readSnapshot();
    
    UN = new UserNode(args[1]);
    PL = new ProjectLib(Integer.parseInt(args[0]), args[1], UN);
    img_comb = new HashMap<String, String>();
    
    CommChannel = new CommChannel(PL);
    Thread CommThread = new Thread(CommChannel);
    CommThread.start();
  }
}
