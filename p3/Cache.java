import java.net.MalformedURLException;
import java.rmi.AlreadyBoundException;
import java.rmi.Naming;
import java.rmi.RemoteException;
import java.rmi.server.UnicastRemoteObject;
import java.util.HashMap;

public class Cache extends UnicastRemoteObject implements Cloud.DatabaseOps {

  private static final long serialVersionUID = 4L;
  private static HashMap<String, String> map = new HashMap<String, String>();
  private static Cloud.DatabaseOps DB = null;

  protected Cache() throws RemoteException {
    super();
  }

  /**
   * Bound the cache instance to a certain URL
   * @param serverIP
   * @param port
   */
  public static void boundCache(String serverIP, int port) {
    Cache cache = null;
    try {
      cache = new Cache();
    } catch (RemoteException e) {
      e.printStackTrace();
    }

    ServerLib SL = new ServerLib(serverIP, port);
    String masterUrl = "//" + serverIP + ":" + port + "/cache";
    Cache.DB = SL.getDB();
    
    try {
      Naming.bind(masterUrl, cache);
    } catch (MalformedURLException e) {
      e.printStackTrace();
    } catch (RemoteException e) {
      e.printStackTrace();
    } catch (AlreadyBoundException e) {
      e.printStackTrace();
    }
  }

  @Override
  public String get(String key) throws RemoteException {
    if (!map.containsKey(key)) {
      String value = DB.get(key);
      map.put(key, value);
    }

    return map.get(key);
  }

  @Override
  public boolean set(String key, String value, String auth) throws RemoteException {
    return DB.set(key, value, auth);
  }

  @Override
  public boolean transaction(String item, float price, int qty) throws RemoteException {
    return DB.transaction(item, price, qty);
  }

}

