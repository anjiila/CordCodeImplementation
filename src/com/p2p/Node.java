package com.p2p;

import java.lang.module.ModuleDescriptor;
import java.security.*;
import java.sql.*;
import java.util.Scanner;
import java.util.logging.Level;
import java.util.logging.Logger;

public class Node {
    public void getNodeDetails(String resourceType){
        Connection con = null;
        PreparedStatement p = null;
        ResultSet rs = null;
        String sql = "";

        con = connection.connectDB();

        try {
            sql = "SELECT * FROM node_details where ResourceType = resourceType;";

            p = con.prepareStatement(sql);
            rs = p.executeQuery();

            if (rs.next()) {//get first result
                System.out.println(rs.getString(1));//coloumn 1
            }

        } catch (SQLException ex) {
            Logger lgr = Logger.getLogger(ModuleDescriptor.Version.class.getName());
            lgr.log(Level.SEVERE, ex.getMessage(), ex);
        }
    }

    public void addNewNodes(){
        System.out.println("Enter number of nodes to be added: ");
        Scanner scanner = new Scanner(System.in);
        int nodeCount = Integer.parseInt(scanner.nextLine());
        if(nodeCount>0){
            System.out.println("Enter resourceType: ");
            Scanner scanner1 = new Scanner(System.in);
            String resourceType = scanner1.nextLine();

            checkResourceHead(String.valueOf(resourceType), nodeCount);
        }

    }

    public void checkResourceHead(String resourceType, int nodeCount){
        System.out.println("resourceType--------"+resourceType);
        Connection con = null;
        PreparedStatement p = null;
        ResultSet rs = null;
        String sql = "";

        con = connection.connectDB();

        String resourceTypeName = resourceType;
        try {
            sql = "SELECT ipAddress, tableName, nodeId FROM group_heads where ResourceType = '"+resourceTypeName+ "'";

            p = con.prepareStatement(sql);
            rs = p.executeQuery();

            if(rs.next()){
                System.out.println("Group Head exists");//coloumn 1
                System.out.println(rs.getInt("nodeId"));
                saveNewNode(rs.getString("tableName"), nodeCount);

            }else {
                System.out.println("Group Head does not exists");//coloumn 1
                createNewTable(resourceTypeName);
                updateGroupHead(resourceTypeName);
                saveNewNode("nodeDetails_"+resourceTypeName,nodeCount);
            }


        } catch (SQLException ex) {
            Logger lgr = Logger.getLogger(ModuleDescriptor.Version.class.getName());
            lgr.log(Level.SEVERE, ex.getMessage(), ex);
        }
    }

    public void saveNewNode(String tableName, int nodeCount){
        System.out.println("tableName--------"+tableName);
        Connection con = null;
        PreparedStatement p = null;
        String sql = "";
        ResultSet rs = null;
        int lastNodeId = 0;

        con = connection.connectDB();

        try {
            sql = "SELECT * FROM "+ tableName+ " ORDER BY nodeId DESC limit 1;";

            p = con.prepareStatement(sql);
            rs = p.executeQuery();

            if (rs.next()) {//get last result in db
                System.out.println(rs.getString("nodeId"));//coloumn 1
                lastNodeId = Integer.parseInt(rs.getString("nodeId"));

                //update here
                String updateNextNodesql = "Update "+tableName+" SET nextNode = "+(lastNodeId+15)+" WHERE nodeId = "+lastNodeId;
                System.out.println("sql query here");
                System.out.println(updateNextNodesql);
                p = con.prepareStatement(updateNextNodesql);
                p.executeUpdate();

            }

        } catch (SQLException ex) {
            Logger lgr = Logger.getLogger(ModuleDescriptor.Version.class.getName());
            lgr.log(Level.SEVERE, ex.getMessage(), ex);
        }

        for (int i = 1; i <= nodeCount; i++) {
            try {
                KeyPairGenerator keyPairGen = KeyPairGenerator.getInstance("DSA");

                //Initializing the KeyPairGenerator
                keyPairGen.initialize(1024);

                //Generating the pair of keys
                KeyPair pair = keyPairGen.generateKeyPair();

                //Getting the private key from the key pair
                PrivateKey privateKey = pair.getPrivate();

                //Getting the public key from the key pair
                PublicKey publicKey = pair.getPublic();

                System.out.println("public key");
                System.out.println(publicKey);

                String insertSql = "Insert into "+tableName+" values(?,?,?,?,?)";

                p = con.prepareStatement(insertSql);

                p.setInt(1, lastNodeId+(i*15));
                p.setString(2, "abc"); //generate dynamic ip here
                p.setString(3, String.valueOf(publicKey.getEncoded()));
                p.setInt(4, i == nodeCount?0:lastNodeId+((i+1)*15));
                p.setInt(5, lastNodeId+((i-1)*15));

                p.executeUpdate();

                if(i == nodeCount){
                    String nodeIdSql = "SELECT * FROM "+ tableName+ " ORDER BY nodeId ASC limit 1;";

                    p = con.prepareStatement(nodeIdSql);
                    rs = p.executeQuery();

                    if (rs.next()) {//get last result in db
                        System.out.println(rs.getString("nodeId"));//coloumn 1
                        int firstNodeId = Integer.parseInt(rs.getString("nodeId"));

                        //update here
                        String updateSql = "Update "+tableName+" SET prevNode = "+(lastNodeId+(i*15))+" WHERE nodeId ="+firstNodeId;

                        System.out.println(updateSql);

                        p = con.prepareStatement(updateSql);
                        p.executeUpdate();
                    }
                }

            } catch (SQLException ex) {
                Logger lgr = Logger.getLogger(ModuleDescriptor.Version.class.getName());
                lgr.log(Level.SEVERE, ex.getMessage(), ex);
            } catch (NoSuchAlgorithmException e) {
                throw new RuntimeException(e);
            }
        }

    }

    public void createNewTable(String resourceType){
        System.out.println("resourceType--------"+resourceType);
        Connection con = null;
        PreparedStatement p = null;

        con = connection.connectDB();

        try {
            String sql = "Create table nodeDetails_"+resourceType+"(\n" +
                    "`nodeId` int NOT NULL,\n" +
                    "`ipAddress` varchar(45) DEFAULT NULL,  \n" +
                    "`publicKey` varchar(1024) DEFAULT NULL,\n" +
                    "`nextNode` int NOT NULL,\n" +
                    "`prevNode` int NOT NULL,\n" +
                    "PRIMARY KEY (`nodeId`));";

            p = con.prepareStatement(sql);
            p.executeUpdate();

            System.out.println("nodeDetails_"+resourceType+" Table created");

        } catch (SQLException ex) {
            Logger lgr = Logger.getLogger(ModuleDescriptor.Version.class.getName());
            lgr.log(Level.SEVERE, ex.getMessage(), ex);
        }
    }

    public void updateGroupHead(String resourceType){
        Connection con = null;
        PreparedStatement p = null;
        String sql = "";
        ResultSet rs = null;
        int lastNodeId = 0;

        con = connection.connectDB();

        try {
            sql = "SELECT * FROM group_heads ORDER BY nodeId DESC limit 1;";

            p = con.prepareStatement(sql);
            rs = p.executeQuery();

            if (rs.next()) {//get last result in db
                System.out.println(rs.getString("nodeId"));//coloumn 1
                lastNodeId = Integer.parseInt(rs.getString("nodeId"));

                //update here
                String updateNextNodesql = "Update group_heads SET nextNode = "+(lastNodeId+1)+" WHERE nodeId = "+lastNodeId;
                System.out.println("sql query here");
                System.out.println(updateNextNodesql);
                p = con.prepareStatement(updateNextNodesql);
                p.executeUpdate();

            }

        } catch (SQLException ex) {
            Logger lgr = Logger.getLogger(ModuleDescriptor.Version.class.getName());
            lgr.log(Level.SEVERE, ex.getMessage(), ex);
        }


        try {
            KeyPairGenerator keyPairGen = KeyPairGenerator.getInstance("DSA");

            //Initializing the KeyPairGenerator
            keyPairGen.initialize(1024);

            //Generating the pair of keys
            KeyPair pair = keyPairGen.generateKeyPair();

            //Getting the private key from the key pair
            PrivateKey privKey = pair.getPrivate();

            //Getting the public key from the key pair
            PublicKey publicKey = pair.getPublic();

            System.out.println("public key");
            System.out.println(publicKey.getEncoded());

            String nodeIdSql = "SELECT * FROM group_heads ORDER BY nodeId ASC limit 1;";

            p = con.prepareStatement(nodeIdSql);
            rs = p.executeQuery();

            if (rs.next()) {//get last result in db

            String insertSql = "Insert into group_heads values(?,?,?,?,?,?,?)";
            p = con.prepareStatement(insertSql);

            p.setString(1, resourceType);
            p.setInt(2, lastNodeId+1);
            p.setString(3, "abc"); //generate dynamic ip here
            p.setString(4, String.valueOf(publicKey.getEncoded()));
            p.setInt(5, 0);
            p.setInt(6, lastNodeId);
            p.setString(7, "nodeDetails_"+resourceType);

            p.executeUpdate();

            String insertNodeDetailsSql = "Insert into nodeDetails_"+resourceType+" values(?,?,?,?,?)";

            p = con.prepareStatement(insertNodeDetailsSql);

            p.setInt(1, lastNodeId+1);
            p.setString(2, "abc"); //generate dynamic ip here
            p.setString(3, String.valueOf(publicKey.getEncoded()));
            p.setInt(4, 0);
            p.setInt(5, 0);

            p.executeUpdate();


            System.out.println(rs.getString("nodeId"));//coloumn 1
            int firstNodeId = Integer.parseInt(rs.getString("nodeId"));

            //update here
            String updateSql = "Update group_heads SET prevNode = "+(lastNodeId+1)+" WHERE nodeId ="+firstNodeId;

            System.out.println(updateSql);

            p = con.prepareStatement(updateSql);
            p.executeUpdate();

        }

        } catch (SQLException ex) {
            Logger lgr = Logger.getLogger(ModuleDescriptor.Version.class.getName());
            lgr.log(Level.SEVERE, ex.getMessage(), ex);
        } catch (NoSuchAlgorithmException e) {
            throw new RuntimeException(e);
        }

    }

    public void deleteNode(){
        System.out.println("Enter the nodeId to be deleted: ");
        Scanner scanner = new Scanner(System.in);
        int nodeId = Integer.parseInt(scanner.nextLine());

        int groupHeadNodeId = nodeId % 15;
        System.out.println(groupHeadNodeId);

        String tableName = getGroupHeadDetails(groupHeadNodeId);

        System.out.println(tableName);

        deleteFromNodeDetails(tableName, nodeId);
    }

    public String getGroupHeadDetails(int nodeId){
        String tableName = null;

        Connection con = null;
        PreparedStatement p = null;
        ResultSet rs = null;

        con = connection.connectDB();

        try {
            String sql = "SELECT tableName FROM group_heads where nodeId = "+nodeId;

            p = con.prepareStatement(sql);
            rs = p.executeQuery();

            if(rs.next()){
                System.out.println("Group Head exists");//coloumn 1
                System.out.println(rs.getString("tableName"));
                tableName = (rs.getString("tableName"));
            }

        } catch (SQLException ex) {
            Logger lgr = Logger.getLogger(ModuleDescriptor.Version.class.getName());
            lgr.log(Level.SEVERE, ex.getMessage(), ex);
        }
        return tableName;
    }

    public void deleteFromNodeDetails(String tableName, int nodeId){

        Connection con = null;
        PreparedStatement p = null;
        ResultSet rs = null;

        con = connection.connectDB();

        try {
            String sql = "DELETE FROM "+tableName+" where nodeId = "+nodeId;

            p = con.prepareStatement(sql);
            p.executeUpdate();


            System.out.println("Node "+nodeId+" has been deleted from "+tableName);//coloumn 1

            String sql1 = "SELECT * FROM "+tableName+" where nodeId > "+nodeId;

            p = con.prepareStatement(sql1);
            rs = p.executeQuery();
            int currentNodeId = 0;
            int prevNodeId = 0;
            int nextNodeId = 0;

            while (rs.next()){
                currentNodeId = rs.getInt("nodeId");
                prevNodeId = rs.getInt("prevNode")==0?0:rs.getInt("prevNode")-15;
                nextNodeId = rs.getInt("nextNode")==0?0:rs.getInt("nextNode")-15;
                System.out.println("previous node id"+currentNodeId);
                System.out.println("updated node id"+nodeId);
                System.out.println("previous node id"+prevNodeId);
                System.out.println("next node id"+nextNodeId);
               //ToDo: add update query for next and previous node as well.
                String sql2 = "UPDATE "+tableName+" SET nodeId ="+nodeId+", prevNode ="+prevNodeId+", nextNode ="+nextNodeId+"  WHERE nodeId = "+currentNodeId;
                System.out.println("--------------------- "+sql2);
                p = con.prepareStatement(sql2);
                p.executeUpdate();
                nodeId = currentNodeId;

            }

        } catch (SQLException ex) {
            Logger lgr = Logger.getLogger(ModuleDescriptor.Version.class.getName());
            lgr.log(Level.SEVERE, ex.getMessage(), ex);
        }
    }
}
