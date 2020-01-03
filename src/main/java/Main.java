import org.w3c.dom.Document;
import org.w3c.dom.Element;
import org.w3c.dom.Node;
import org.w3c.dom.NodeList;
import org.xml.sax.SAXException;

import javax.xml.parsers.DocumentBuilder;
import javax.xml.parsers.DocumentBuilderFactory;
import javax.xml.parsers.ParserConfigurationException;
import java.io.File;
import java.io.IOException;
import java.sql.*;
import java.util.*;

public class Main {

    public static void main(String[] args) throws ParserConfigurationException, SAXException, IOException, SQLException {
        parseXml();
    }

    private static Connection createDbForAddressObjects(Set<String> uniqueNodes) throws SQLException {
        Connection connection =
                DriverManager.getConnection("jdbc:postgresql://localhost:5432/parsingxml", "postgres", "12345678");
        Statement statement = connection.createStatement();
        String sql = "CREATE TABLE IF NOT EXISTS addressobjects(\n";
        sql+= " id SERIAL PRIMARY KEY,\n";
        List<String> nodeNames = new ArrayList<>(uniqueNodes);
        StringBuilder stringBuilder = new StringBuilder();
        stringBuilder.append(sql);
        for (int i = 0; i < nodeNames.size(); i++) {
            stringBuilder.append(" ");
            stringBuilder.append(nodeNames.get(i));
            if(i==nodeNames.size()-1) {
                stringBuilder.append(" varchar(100)\n");
            } else {
                stringBuilder.append(" varchar(100), \n");
            }
        }

        sql=stringBuilder.append(")").toString();
        statement.execute(sql);
        return connection;
    }

    private static void parseXml() throws IOException, SAXException, ParserConfigurationException, SQLException {
        DocumentBuilderFactory factory = DocumentBuilderFactory.newInstance();
        DocumentBuilder builder = factory.newDocumentBuilder();

        String path = "../AS_ADDROBJ_20191226_728d035c-64d4-4576-be98-7675b6a6d94e.XML";
//        String path = "C:\\Users\\petru\\Downloads\\AS_ADDROBJ_20191226_728d035c-64d4-4576-be98-7675b6a6d94e.XML";
        Document document = builder.parse(new File(path));

        document.getDocumentElement().normalize();

        Element root = document.getDocumentElement();
        System.out.println(root.getNodeName());

        NodeList nList = document.getElementsByTagName("Object");

        Set<String> nodeNames = new HashSet<>();
        for (int j = 0; j < nList.getLength(); j++) {
            Node node = nList.item(j);
            for (int i=0; i<node.getAttributes().getLength(); i++) {
                String nodeName = node.getAttributes().item(i).getNodeName();
                nodeNames.add(nodeName);
            }
        }

        Connection connection = createDbForAddressObjects(nodeNames);

        for (int temp = 0; temp < nList.getLength(); temp++) {
            Node addressObjectNode = nList.item(temp);
            HashMap<String, String> addressObjectData = new HashMap<>();
            for (int i = 0; i < addressObjectNode.getAttributes().getLength(); i++) {
                String nodeName = addressObjectNode.getAttributes().item(i).getNodeName();
                String nodeValue = addressObjectNode.getAttributes().item(i).getNodeValue();
                addressObjectData.put(nodeName, nodeValue);
            }
            if(isAlreadyInDb(connection, addressObjectData.get("AOID"))) {
                System.out.println("address is already exist in db with this AOID: " + addressObjectData.get("AOID"));
            } else {
                System.out.println("Number of address object: " + temp);
                insertDataToDb(addressObjectData, nodeNames, connection);
            }
        }
        System.out.println("Insertion to db is completed");
    }

    private static boolean isAlreadyInDb(Connection connection, String aoid) throws SQLException {
        Statement statement = connection.createStatement();
        String sql = "SELECT * FROM addressobjects "+
                "WHERE aoid='" + aoid + "'";
        ResultSet result = statement.executeQuery(sql);
        return result.next();
    }

    private static void insertDataToDb(HashMap<String, String> addressObjectData, Set<String> nodeNames, Connection connection) throws SQLException {
        Statement statement = connection.createStatement();
        String sql = "INSERT INTO addressobjects(";
        Iterator nodeNamesIterator = nodeNames.iterator();
        StringBuilder stringBuilder = new StringBuilder();
        stringBuilder.append(sql);
        while (nodeNamesIterator.hasNext()) {
            stringBuilder.append(nodeNamesIterator.next());
            if(nodeNamesIterator.hasNext()) stringBuilder.append(", ");
        }
        stringBuilder.append(") \n VALUES (");
        nodeNamesIterator = nodeNames.iterator();
        while(nodeNamesIterator.hasNext()) {
            Object key = nodeNamesIterator.next();
            stringBuilder.append("'");
            if(addressObjectData.containsKey(key)) {
                stringBuilder.append(addressObjectData.get(key));
            } else {
                stringBuilder.append("null");
            }
            stringBuilder.append("'");
            if(nodeNamesIterator.hasNext()) stringBuilder.append(", ");

        }
        sql = stringBuilder.append(");").toString();
        statement.execute(sql);
    }
}
