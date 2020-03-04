package leal.abraham.rmiInterface;

import java.rmi.Remote;
import java.rmi.RemoteException;

public interface RMIStreams extends Remote {
    String getValueFromGlobalKTable(String key) throws RemoteException;
}
