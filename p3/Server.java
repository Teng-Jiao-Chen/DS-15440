import java.net.MalformedURLException;
import java.rmi.AlreadyBoundException;
import java.rmi.Naming;
import java.rmi.NotBoundException;
import java.rmi.RemoteException;
import java.rmi.server.UnicastRemoteObject;
import java.util.Date;
import java.util.LinkedList;
import java.util.PriorityQueue;
import java.util.Queue;
import java.util.TreeMap;

/* Sample code for basic Server */

public class Server extends UnicastRemoteObject implements ServerI {

  protected Server() throws RemoteException {
    super();
  }

  // Sample frequency of request queue length per second
  // Sampled average is used for scaling reference
  private static final int SAMPLE_RATE = 10;
  
  private static TreeMap<Integer, ServerI.Role> jobAssign;
  private static LinkedList<Cloud.FrontEndOps.Request> requestQueue;
  private static LinkedList<Cloud.FrontEndOps.Request> virtualQueue;
  private static int frontBase = 1;
  private static int midBase = 2;
  private static int frontNum = 0;
  private static int midNum = 0;
  private static int frontNeeded = 0;
  private static int midNeeded = 0;
  private static int qLength = 0;
  private static boolean shouldStop = false;
  private static Cloud.DatabaseOps cache = null;

  private static String serverIP;
  private static int port;

  private static int vMid;
  
  public static void main(String args[]) throws Exception {
    if (args.length != 3)
      throw new Exception("Need 3 args: <cloud_ip> <cloud_port> <VMid>");

    serverIP = args[0];
    port = Integer.parseInt(args[1]);
    
    vMid = Integer.parseInt(args[2]);
    ServerLib SL = new ServerLib(serverIP, port);

    // ********* MASTER METHOD **************
    if (is_master(serverIP, port)) {
      System.err.println("Master VMid: " + vMid);
      jobAssign = new TreeMap<Integer, ServerI.Role>();
      requestQueue = new LinkedList<Cloud.FrontEndOps.Request>();
      virtualQueue = new LinkedList<Cloud.FrontEndOps.Request>();
      jobAssign.put(vMid, ServerI.Role.MASTER);

      // ****************** START CACHE ***********************************
      Cache.boundCache(serverIP, port);

      // ****************** START TWO MIDDLE TIERS *************************
      midNum++;
      int _VMid = SL.startVM();
      jobAssign.put(_VMid, ServerI.Role.MIDDLE);
      
      midNum++;
      _VMid = SL.startVM();
      jobAssign.put(_VMid, ServerI.Role.MIDDLE);

      // ****************** START ONE FRONT TIER  *************************
      frontNum++;
      int __VMid = SL.startVM();
      jobAssign.put(__VMid, ServerI.Role.FRONT);
      
      // *********** START ONE DEAMON FOR SCALE PURPOSE *******************
      Thread deamon = new Thread() {
        public void run() {
          updateScaleRate(SL);
        }
      };
      
      deamon.start();
      
      master_routine(SL, vMid);
    }
    
    // get master instance
    String masterUrl = "//" + serverIP + ":" + port + "/master";
    ServerI master = (ServerI) Naming.lookup(masterUrl);
    ServerI.Job job = master.assign(vMid);
    
    if(job.VMid != vMid){
      System.err.println("ID does not match -- VMid: "+vMid+" job.VMid: " + job.VMid);
    }
    
    // ********* FRONT METHOD **************
    if(job.role == ServerI.Role.FRONT){
      System.err.println("Front VMid: " + vMid);
      String URL = "//" + serverIP + ":" + port + "/front"+ vMid;
      boundURL(URL);

      front_routine(SL, master);
      finish_routine(SL, vMid, URL, master);
    } 
    // ********* MIDDLE METHOD **************
    else if (job.role == ServerI.Role.MIDDLE){
      System.err.println("Middle VMid: " + vMid);
      String URL = "//" + serverIP + ":" + port + "/middle"+ vMid;
      boundURL(URL);
      
      requestQueue = new LinkedList<Cloud.FrontEndOps.Request>();
      middle_routine(SL, master, cache);
      finish_routine(SL, vMid, URL, master);
    }
    else {
      System.err.println("Undefined Role! -- VMid: " + vMid);
      System.exit(0);
    }
  }

  /**
   * A daemon thread used for calculating the current scaling
   * parameters. The main idea is to calculate the queue length
   * with 900ms, and then use average queue length to set the
   * number of instances based on heuristic.
   * @param SL
   */
  protected static void updateScaleRate(ServerLib SL) {
    
    // Idea: Record 10 queue length and average them 
    //         --> use the avg to decide the scale number
    Queue<Double> qRecord = new PriorityQueue<Double>();
    Date date = new Date();
    
    while(true){
      double delta = qLength ;
      qRecord.add(delta);
      
      //****** CALCULATE NEEDED FRONT NUMBER AND MID NUMBER **********
      double average = 0;

      try {
        Thread.sleep(900 / SAMPLE_RATE);
      } catch (InterruptedException e) {
        e.printStackTrace();
      }
      
      if(qRecord.size() < SAMPLE_RATE) continue;
      else{
        // Compute the average queue length
        System.err.println("Master: qRecord " + qRecord);
        for (double length : qRecord) {
          average += length;
        }
    
        System.err.println("Master: average " + average);
        average = average / (SAMPLE_RATE);
        
        qRecord.clear();
        virtualQueue.clear();
        
        System.err.println("Master: average " + average);
        
      }
      
      // ******************* Heuristics for scaling ****************************
      if(jobAssign.keySet().size() >= 12) continue;
      
      if(2.8 <= average && average < 4){
        frontNeeded = 0;
        if(midNum < 7 && (7 - midNum) + jobAssign.keySet().size() <= 12 ) 
          midNeeded = 7 - midNum;
        frontBase = 1;
        midBase = 2;
      } else if(average >= 4.5){
        if(midNum < 10 && (10 - midNum) + jobAssign.keySet().size() <= 12) 
          midNeeded = 10 - midNum;
        frontBase = 1;
        midBase = 2;
      }
      
    }
  }

  /**
   * Finish current instance
   * @param SL
   * @param VMid
   * @param URL
   * @param master
   */
  public static void finish_routine(ServerLib SL, int VMid, String URL, ServerI master) {
    try {
      SL.unregister_frontend();
      master.removeVM(VMid);
      ServerI server = (ServerI) Naming.lookup(URL);
      SL.interruptGetNext();
      SL.shutDown();
      UnicastRemoteObject.unexportObject(server, true);
    } catch (Exception e) {
      // do nothing
    }
    
    System.exit(0);
  }

  /**
   * all the logic handled by middle instance
   * @param master 
   * @param cache 
   */
  private static void middle_routine(ServerLib SL, ServerI master, Cloud.DatabaseOps cache) {
    // ************** get cache instance **************
    String cacheUrl = "//" + serverIP + ":" + port + "/cache";
    while(true){
      try {
        cache = (Cloud.DatabaseOps) Naming.lookup(cacheUrl);
      } catch (MalformedURLException e) {
        e.printStackTrace();
        continue;
      } catch (RemoteException e) {
        e.printStackTrace();
        continue;
      } catch (NotBoundException e) {
        continue;
      }
      break;
    }
    
    Date date = new Date();
    long requireStopTime = date.getTime() ;
    long lastProcessTime = date.getTime() ;
    while (true) {
      long currentTime = new Date().getTime();
      if(shouldStop && currentTime - requireStopTime > 1000) break;
      
      // If the waiting interval is long enough, then shutdown this instance.
      // 4 instances(2 mid, 1 front, 1 master) are reserved in the cluster 
      if(currentTime - lastProcessTime > 2000 && vMid > 3 
          || currentTime - lastProcessTime > 2500 && vMid == 3) {
        if(!shouldStop) requireStopTime = new Date().getTime();
        shouldStop = true;
      }
      
      Cloud.FrontEndOps.Request r = null;
      try {
        r = master.getRequest();
      } catch (RemoteException e) {
        
      }

      if(r != null){
        SL.processRequest(r, cache);
        lastProcessTime = currentTime;
      }
    }
    System.err.println("Gonna shut down");
  }

  /**
   * all the logic handled by front instance
   * @param VMid 
   * @param master 
   */
  private static void front_routine(ServerLib SL, ServerI master) {
    SL.register_frontend();
    
    while (true) {
      
      try {
        Cloud.FrontEndOps.Request r = SL.getNextRequest();
        if(r != null) master.addRequest(r);
      } catch (RemoteException e) {
        e.printStackTrace();
      }
    }
  }

  /**
   * all the logic handled by master instance
   * @param VMid 
   * @param sL 
   */
  private static void master_routine(ServerLib SL, int VMid) {
    frontNum ++;
    
    SL.register_frontend();
    
    Date date = new Date();
    long lastDropTime = date.getTime() ;
    
    while (true) {
      //********** SAMPLE THE CONGESTED QUEUE LENGTH **************
      double curQLength = SL.getQueueLength() ;
      qLength = (int)curQLength + requestQueue.size() + virtualQueue.size();
      
      // If we cannot handle the request, we drop them first.
      if(curQLength - midNum > 0){
        SL.dropHead();
        continue;
      }
      
      //****************** PROCESS THE REQUESTS **********************
      Cloud.FrontEndOps.Request r = SL.getNextRequest();
      System.err.println((1.0*qLength)/(1.0*midNum));
      long currentTime = new Date().getTime();
      if( (1.0*qLength)/(1.0*midNum) >= 1.7 ){
        SL.drop(r);
        virtualQueue.add(r);
        System.err.println("DROP (1.0*qLength)/(1.0*midNum) >= 1.7");
        lastDropTime = currentTime;
      }
      else if(SL.getStatusVM(VMid) == Cloud.CloudOps.VMStatus.Booting){
        SL.drop(r);
        virtualQueue.add(r);
      }
      else if(SL.getStatusVM(VMid+frontBase+midBase) == Cloud.CloudOps.VMStatus.NonExistent){
        SL.drop(r);
        virtualQueue.add(r);
      }
      else if(SL.getStatusVM(VMid+frontBase+midBase) == Cloud.CloudOps.VMStatus.Booting){
        SL.drop(r);
        virtualQueue.add(r);
      }
      else{
        requestQueue.add(r);
        virtualQueue.clear();
      }
      
      if(frontNeeded <= 0 && midNeeded <= 0) continue;
      
      //******************* SCALING FRONT **********************
      if(frontNeeded > 0)
        for (int i = 0; i < frontNeeded; i++) {
          frontNum++;
          int _VMid = SL.startVM();
          jobAssign.put(_VMid, ServerI.Role.FRONT);
        }
      
      //******************* SCALING MIDDLE **********************
      if(midNeeded > 0)
        for (int i = 0; i < midNeeded; i++) {
          midNum++;
          int _VMid = SL.startVM();
          jobAssign.put(_VMid, ServerI.Role.MIDDLE);
        }
      
      frontNeeded = 0;
      midNeeded = 0;
    }
  }

  /**
   * Bound the machine to a specific URL
   * @param URL
   */
  private static void boundURL(String URL) {
    Server server = null;
    try {
      server = new Server();
    } catch (RemoteException e) {
      e.printStackTrace();
    }

    try {
      Naming.bind(URL, server);
    } catch (MalformedURLException e) {
      e.printStackTrace();
    } catch (RemoteException e) {
      e.printStackTrace();
    } catch (AlreadyBoundException e) {
      e.printStackTrace();
    }
  }
  
  /**
   * Only ONE master appear in the cluster.
   * This method will ensure ONLY THE FIRST CALL WILL RETURN TRUE
   * 
   * @param serverIP
   * @param port
   * @return
   */
  private static boolean is_master(String serverIP, int port) {
    Server server = null;
    try {
      server = new Server();
    } catch (RemoteException e) {
      e.printStackTrace();
    }

    String masterUrl = "//" + serverIP + ":" + port + "/master";
    try {
      Naming.bind(masterUrl, server);
      return true;
    } catch (MalformedURLException e) {
      e.printStackTrace();
      return false;
    } catch (RemoteException e) {
      e.printStackTrace();
      return false;
    } catch (AlreadyBoundException e) {
      return false;
    }
  }

  @Override
  public Job assign(int VmID) throws RemoteException {
    Job job = new ServerI.Job();
    
    // ********** typically unused code ********
    // Wait for the main thread to insert the <VMid, Role> mapping
    while(!jobAssign.containsKey(VmID)){
      System.err.println("Assign NULL");
      try {
        Thread.sleep(1000);
      } catch (InterruptedException e) {
        e.printStackTrace();
      }
    }
    
    Role role = jobAssign.get(VmID);
    
    job.VMid = VmID;
    job.role = role;
    return job;
  }

  @Override
  public synchronized void addRequest(Cloud.FrontEndOps.Request r) throws RemoteException {
    requestQueue.add(r);
  }

  @Override
  public synchronized Cloud.FrontEndOps.Request getRequest() throws RemoteException {
    if(requestQueue.isEmpty()) return null;
    return requestQueue.pollFirst();
  }

  @Override
  public void removeVM(int VmID) throws RemoteException {
    Role role = jobAssign.get(VmID);
    if(role == ServerI.Role.FRONT) frontNum--;
    if(role == ServerI.Role.MIDDLE) midNum--;
    jobAssign.put(VmID, ServerI.Role.DEAD);
  }

}


