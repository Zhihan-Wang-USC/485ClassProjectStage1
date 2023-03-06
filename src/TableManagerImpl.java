import java.io.*;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.concurrent.ExecutionException;

import com.apple.foundationdb.Database;
import com.apple.foundationdb.Transaction;
import com.apple.foundationdb.directory.DirectoryLayer;
import com.apple.foundationdb.directory.DirectorySubspace;
import com.apple.foundationdb.directory.PathUtil;
import com.apple.foundationdb.tuple.Tuple;
import com.apple.foundationdb.FDB;

/**
 * TableManagerImpl implements interfaces in {#TableManager}. You should put your implementation
 * in this class.
 */
public class TableManagerImpl implements TableManager {

  private FDB fdb = FDB.selectAPIVersion(710);
  private Database db = null;
  private static DirectorySubspace dbMetadataDirectory = null;
  private static String dbMedataDirectoryName = "dbMetadata";

  public static String tablePrefix = "__table_";
  public static String tableNameListKey = "table_name_list";

  public static String EmployeeTableName = "Employee";
  public static String[] EmployeeTableAttributeNames = new String[]{"SSN", "Name"};
  public static AttributeType[] EmployeeTableAttributeTypes =
          new AttributeType[]{AttributeType.INT, AttributeType.VARCHAR};
  public static String[] EmployeeTablePKAttributes = new String[]{"SSN"};


  public TableManagerImpl() {
    // Basic setup
    try {
      db = fdb.open();
    } catch (Exception e) {
      System.out.println("ERROR: the database is not successfully opened: " + e);
    }

    System.out.println("Open FDB Successfully!");
    try {
      dbMetadataDirectory = DirectoryLayer.getDefault().createOrOpen(db,
              PathUtil.from(dbMedataDirectoryName)).join();
    } catch (Exception e) {
      System.out.println("ERROR: the " + dbMedataDirectoryName + " directory is not successfully opened: " + e);
    }
    System.out.println("Create dbMetadata directory successfully!");
    // End of basic setup /////////////////////////////////

    Transaction tx = null;
    try {
      tx = db.createTransaction();
      ArrayList<String> tableNameList = readTableNameList(tx);
      if (tableNameList == null) {
        System.out.println("tableNameList is null, creating a new one.");
        tableNameList = new ArrayList<String>();
        writeTableNameList(tx, tableNameList);
        tx.commit().join();
      }
    } catch (Exception e) {
      System.out.println("initialization failed: " + e);
    } finally {
      tx.close();
    }

    // Test writeMetadata and readMetadata
//    Transaction tx2 = null;
//    try {
//      tx = db.createTransaction();
//      TableMetadata tableMetadata = new TableMetadata(EmployeeTableAttributeNames, EmployeeTableAttributeTypes, EmployeeTablePKAttributes);
//      writeMetadata(tx, EmployeeTableName, tableMetadata);
//      tx.commit().join();
//      System.out.println("writeMetadata done writting");
//      tx2 = db.createTransaction();
//      TableMetadata tableMetadata2 = readMetadata(tx2, EmployeeTableName);
//      System.out.println("tableMetadata2: " + tableMetadata2);
//    } catch (Exception e) {
//      System.out.println("ERROR: writeMetadata and readMetadata failed: " + e);
//    } finally {
//      tx.close();
//      if (tx2 != null) {
//        tx2.close();
//      }
//    }

  }

  public static StatusCode writeTableNameList(Transaction tx, ArrayList tableNames) {
    Tuple keyTuple = new Tuple().add(tableNameListKey);
    Tuple valueTuple = new Tuple().add(tableNames);
    tx.set(dbMetadataDirectory.pack(keyTuple), valueTuple.pack());
    return StatusCode.SUCCESS;
  }

  public static ArrayList<String> readTableNameList(Transaction tx) throws ExecutionException, InterruptedException {
    ArrayList<String> tableNames = null;
    Tuple keyTuple = new Tuple().add(tableNameListKey);
    byte[] value = tx.get(dbMetadataDirectory.pack(keyTuple)).get();
    if (value != null) {
      tableNames = (ArrayList<String>) Tuple.fromBytes(value).get(0);
    }
    return tableNames;
  }

  public static StatusCode writeMetadata(Transaction tx, String tableName, TableMetadata tableMetadata) throws IOException {
    Tuple keyTuple = new Tuple().add(tablePrefix).add(tableName);
    Tuple valueTuple = new Tuple().addObject(tableMetadata.serializeToString());
    tx.set(dbMetadataDirectory.pack(keyTuple), valueTuple.pack());
    return StatusCode.SUCCESS;
  }

  public static TableMetadata readMetadata(Transaction tx, String tableName) throws ExecutionException, InterruptedException, IOException, ClassNotFoundException {
    Tuple keyTuple = new Tuple().add(tablePrefix).add(tableName);
    TableMetadata tableMetadata = (TableMetadata) TableMetadata.deserializeFromString((String) Tuple.fromBytes(tx.get(dbMetadataDirectory.pack(keyTuple)).get()).get(0));
    return tableMetadata;
  }

  public static StatusCode removeMetadata(Transaction tx, String tableName) {
    Tuple keyTuple = new Tuple().add(tablePrefix).add(tableName);
    tx.clear(dbMetadataDirectory.pack(keyTuple));
    return StatusCode.SUCCESS;
  }

  private static boolean attributeTypeValid(AttributeType type) {
    return type == AttributeType.INT || type == AttributeType.VARCHAR || type == AttributeType.DOUBLE;
  }

  @Override
  public StatusCode createTable(String tableName, String[] attributeNames, AttributeType[] attributeType,
                                String[] primaryKeyAttributeNames) {


    // if primaryKeyAttributeNames is empty, throw error code
    if (attributeNames == null || primaryKeyAttributeNames.length == 0) {
      return StatusCode.TABLE_CREATION_ATTRIBUTE_INVALID;
    }

    if (attributeNames == null || attributeType == null) {
      return StatusCode.TABLE_CREATION_ATTRIBUTE_INVALID;
    }

    // if attributeNames or attributeType is empty, throw error code
    if (attributeNames.length != attributeType.length) {
      System.out.println("ERROR: the number of attribute names is not equal to the number of attribute types");
      return StatusCode.TABLE_CREATION_ATTRIBUTE_NUM_MISMATCH;
    }

    // if primarykey attribute name not in attribute names, throw error code
    for (String primaryKeyAttributeName : primaryKeyAttributeNames) {
      if (!Arrays.asList(attributeNames).contains(primaryKeyAttributeName)) {
        return StatusCode.TABLE_CREATION_PRIMARY_KEY_NOT_FOUND;
      }
    }

    // if table name attribute invalid
    for (AttributeType type : attributeType) {
      if (!attributeTypeValid(type)) {
        return StatusCode.TABLE_CREATION_ATTRIBUTE_INVALID;
      }
    }

    Transaction tx = null;
    Transaction tx2 = null;
    try {
      tx = db.createTransaction();
      ArrayList<String> tableNameList = readTableNameList(tx);
      if (tableNameList == null) {
        tableNameList = new ArrayList<>();
      } else if (tableNameList.contains(tableName)) {
        return StatusCode.TABLE_ALREADY_EXISTS;
      }
      tx.commit().join();

      tx2 = db.createTransaction();
      tableNameList.add(tableName);
      writeTableNameList(tx2, tableNameList);
      TableMetadata metadata = new TableMetadata(attributeNames, attributeType, primaryKeyAttributeNames);
      writeMetadata(tx2, tableName, metadata);
      tx2.commit().join();
    } catch (Exception e) {
      throw new RuntimeException(e);
    } finally {
      tx.close();
      if (tx2 != null) {
        tx2.close();
      }
    }

    return StatusCode.SUCCESS;
  }

  @Override
  public StatusCode deleteTable(String tableName) {
    Transaction tx = null;
    Transaction tx2 = null;
    try {
      tx = db.createTransaction();
      ArrayList<String> tableNameList = readTableNameList(tx);
      if (tableNameList == null || !tableNameList.contains(tableName)) {
        return StatusCode.TABLE_NOT_FOUND;
      }
      tx.commit().join();

      tx2 = db.createTransaction();
      tableNameList.remove(tableName);
      writeTableNameList(tx2, tableNameList);
      removeMetadata(tx2, tableName);
      tx2.commit().join();
    } catch (Exception e) {
      throw new RuntimeException(e);
    } finally {
      tx.close();
      if (tx2 != null) {
        tx2.close();
      }
    }

    return StatusCode.SUCCESS;
  }

  @Override
  public HashMap<String, TableMetadata> listTables() {
    HashMap<String, TableMetadata> allMetadatas = new HashMap<>();
    Transaction tx = null;
    try {
      tx = db.createTransaction();
      ArrayList<String> tableNameList = readTableNameList(tx);
      for (String tableName : tableNameList) {
        TableMetadata metadata = readMetadata(tx, tableName);
        allMetadatas.put(tableName, metadata);
      }
    } catch (Exception e) {
      throw new RuntimeException(e);
    } finally {
      tx.close();
    }
    return allMetadatas;
  }

  @Override
  public StatusCode addAttribute(String tableName, String attributeName, AttributeType attributeType) {
    Transaction tx = null;
    try {
      tx = db.createTransaction();
      ArrayList<String> tableNameList = readTableNameList(tx);
      if (tableNameList == null || !tableNameList.contains(tableName)) {
        return StatusCode.TABLE_NOT_FOUND;
      }
      TableMetadata metadata = readMetadata(tx, tableName);
      if (metadata.doesAttributeExist(attributeName)) {
        return StatusCode.ATTRIBUTE_ALREADY_EXISTS;
      }
      if (!attributeTypeValid(attributeType)) {
        return StatusCode.TABLE_CREATION_ATTRIBUTE_INVALID;
      }

      metadata.addAttribute(attributeName, attributeType);
      writeMetadata(tx, tableName, metadata);
      tx.commit().join();
    } catch (Exception e) {
      throw new RuntimeException(e);
    } finally {
      tx.close();
    }
    return StatusCode.SUCCESS;
  }

  @Override
  public StatusCode dropAttribute(String tableName, String attributeName) {
    Transaction tx = null;
    try {
      tx = db.createTransaction();
      ArrayList<String> tableNameList = readTableNameList(tx);
      if (tableNameList == null || !tableNameList.contains(tableName)) {
        return StatusCode.TABLE_NOT_FOUND;
      }
      TableMetadata metadata = readMetadata(tx, tableName);

      if (!metadata.doesAttributeExist(attributeName)) {
        return StatusCode.ATTRIBUTE_NOT_FOUND;
      }
      ArrayList<String> primaryKeyAttributeNames = new ArrayList<>(metadata.getPrimaryKeys());
      if (primaryKeyAttributeNames != null && primaryKeyAttributeNames.contains(attributeName)) {
        return StatusCode.PRIMARY_KEY_CANNOT_DROP;
      }

      HashMap<String, AttributeType> attributes = metadata.getAttributes();
      attributes.remove(attributeName);
      metadata.setAttributes(attributes);
      writeMetadata(tx, tableName, metadata);
      tx.commit().join();
    } catch (Exception e) {
      throw new RuntimeException(e);
    } finally {
      tx.close();
    }
    return StatusCode.SUCCESS;
  }

  @Override
  public StatusCode dropAllTables() {
    Transaction tx = db.createTransaction();
    tx.clear(dbMetadataDirectory.range());
    tx.commit().join();
    tx.close();
    return StatusCode.SUCCESS;
  }
}
