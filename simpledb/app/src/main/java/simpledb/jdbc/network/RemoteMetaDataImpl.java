package simpledb.jdbc.network;

import static java.sql.Types.INTEGER;

import java.rmi.RemoteException;
import java.rmi.server.UnicastRemoteObject;
import java.util.ArrayList;
import java.util.List;

import simpledb.record.Schema;

public class RemoteMetaDataImpl extends UnicastRemoteObject implements RemoteMetaData {
  private Schema sch;
  private List<String> fields = new ArrayList<>();

  public RemoteMetaDataImpl(Schema sch) throws RemoteException {
    this.sch = sch;
    for (String fld : sch.fields())
      fields.add(fld);
  }

  @Override
  public int getColumnCount() throws RemoteException {
    return fields.size();
  }

  @Override
  public String getColumnName(int column) throws RemoteException {
    return fields.get(column - 1);
  }

  @Override
  public int getColumnType(int column) throws RemoteException {
    String fldname = getColumnName(column);
    return sch.type(fldname);
  }

  /*
   * Return the number of characters required to display the specified column.
   */
  @Override
  public int getColumnDisplaySize(int column) throws RemoteException {
    String fldname = getColumnName(column);
    int fldtype = sch.type(fldname);
    int fldlength = (fldtype == INTEGER) ? 6 : sch.length(fldname);
    return Math.max(fldname.length(), fldlength) + 1;
  }
}
