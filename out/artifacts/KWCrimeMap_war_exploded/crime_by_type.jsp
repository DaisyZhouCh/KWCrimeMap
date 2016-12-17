<%--
  Created by IntelliJ IDEA.
  User: chongyizhou
  Date: 2016-11-23
  Time: 12:43 PM
  To change this template use File | Settings | File Templates.
--%>
<%@ page contentType="text/html;charset=UTF-8" language="java" %>
<%@ page import="DAO.*" %>
<%@ page import="com.mongodb.client.FindIterable" %>
<%@ page import="org.bson.Document" %>
<%@ page import="com.mongodb.client.MongoCursor" %>
<%@ page import="com.mongodb.BasicDBList" %>
<%@ page import="com.mongodb.util.JSON" %>
<%@ page import="org.json.JSONObject" %>


<%
    DbHelper db = new DbHelperImpl();
    FindIterable<Document> findIterable = db.selectByField("crime_by_type", "_id");
    MongoCursor<Document> iterator = findIterable.iterator();
    BasicDBList list = new BasicDBList();
    while (iterator.hasNext()) {
        Document doc = iterator.next();
        list.add(doc);
    }
    String result = JSON.serialize(list);
    response.setContentType("application/json");
    response.getWriter().print(result);
%>

