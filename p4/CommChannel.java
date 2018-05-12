import java.util.HashMap;
import java.util.PriorityQueue;
import java.util.concurrent.locks.Lock;
import java.util.concurrent.locks.ReentrantLock;

import javax.jws.soap.SOAPBinding.ParameterStyle;

public class CommChannel implements Runnable {

  private ProjectLib PL;
  private HashMap<String, Packet> map = new HashMap<String, Packet>();
  private Thread curThread;
  private Lock lock = new ReentrantLock();
  
  public CommChannel() {
    super();
  }
  
  public CommChannel(ProjectLib PL) {
    super();
    this.PL = PL;
  }

  private PriorityQueue<Packet> pktQueue = new PriorityQueue<Packet>();
  private static final long RRT = 3000;

  /**
   * Send packet and retransmit if not replied
   * @param pktReply
   */
  public synchronized void sendPkt(Packet pktReply){
    pktReply.setTimeStamp(System.currentTimeMillis());
    String dst = pktReply.getDestination();
    byte[] pktBytes = Utils.serialize(pktReply);
    ProjectLib.Message msgReply = new ProjectLib.Message(dst, pktBytes);
    PL.sendMessage(msgReply);
    addPkt(pktReply);

    if(pktQueue.size()==1 || pktReply.getTimeStamp() < pktQueue.peek().getTimeStamp()){
      curThread.interrupt();
    }
  }

  /**
   * Add packet to the priority queue
   * Retransmit if needed.
   * @param pkt
   */
  public void addPkt(Packet pkt){
    lock.lock();
    try {
      pktQueue.add(pkt);
      map.put(pkt.getSessionID(), pkt);
    } catch (Exception e) {
      e.printStackTrace();
      System.err.println("Something Bad Happeded in sendPkt");
    } finally {
      lock.unlock();
    }
  }

  /**
   * Remove the packet from the priority queue
   * when the ACK is received.
   * @param SessionID
   */
  public void rmPkt(String SessionID){
    lock.lock();
    try {
      Packet pkt = map.get(SessionID);
      pktQueue.remove(pkt);
      map.remove(SessionID);
    } catch (Exception e) {
      e.printStackTrace();
      System.err.println("Something Bad Happeded in rcvPkt");
    } finally {
      lock.unlock();
    }
  }

  /**
   * Remove the packet from the priority queue
   * when the ACK is received.
   * @param pkt
   */
  public void rmPkt(Packet pkt){
    lock.lock();
    try {
      pktQueue.remove(pkt);
      map.remove(pkt.getSessionID());
    } catch (Exception e) {
      e.printStackTrace();
      System.err.println("Something Bad Happeded in rcvPkt");
    } finally {
      lock.unlock();
    }
  }
  
  @Override
  public void run() {
    curThread = Thread.currentThread();
    while (true) {
      if (pktQueue.isEmpty()) {
        try {
          synchronized (this) {
            wait();
          }
        } catch (InterruptedException e) {
          // do nothing
        }
      } 
      else {
        Packet curPkt = pktQueue.peek();
        long curTime = System.currentTimeMillis();

        if (curTime >= curPkt.getTimeStamp()  + RRT) {
          
          if(curPkt.type == Packet.Type.PREPARE){
            rmPkt(curPkt);
            
            curPkt.setType(Packet.Type.ACK_PRE)
                  .setReply(Packet.Reply.NO);
            
            Server.packetHandler(curPkt);
          }
          else if(curPkt.type == Packet.Type.ACK_PRE){
            synchronized (UserNode.img_comb){
              String imgName = curPkt.source.split(":")[1];
              UserNode.img_comb.remove(imgName);
            }
            rmPkt(curPkt);
          }
          else if(curPkt.type == Packet.Type.COMMIT){
            rmPkt(curPkt);
            sendPkt(curPkt);
          }
          else{
            rmPkt(curPkt);
          }
        } 
        else {
          /*
           * Here RRT = 6s. 
           * E.G. If curTime = PktTime, then we need to sleep 6s. 
           * E.G. If curTime = PktTime + 3, then we need to sleep 3s.
           * E.G. If curTime = PktTime + 4, then we need to sleep 2s. 
           * Thus curTime - PktTime = timeElapsed,
           * And we need to wait for (6-timeElapsed) seconds.
           */
          long sleepTime = RRT - (curTime - curPkt.getTimeStamp());
          try {
            Thread.sleep(sleepTime);
          } catch (InterruptedException e) {
            // do nothing
          }
        }
      }
    }
  }

}
