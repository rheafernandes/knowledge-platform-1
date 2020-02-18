package org.sunbird.graph.service.operation;

import org.apache.commons.collections4.CollectionUtils;
import org.apache.commons.collections4.MapUtils;
import org.apache.commons.lang3.StringUtils;
import org.neo4j.driver.v1.Driver;
import org.neo4j.driver.v1.Record;
import org.neo4j.driver.v1.Session;
import org.neo4j.driver.v1.StatementResultCursor;
import org.sunbird.common.DateUtils;
import org.sunbird.common.JsonUtils;
import org.sunbird.common.dto.Request;
import org.sunbird.common.exception.ClientException;
import org.sunbird.common.exception.MiddlewareException;
import org.sunbird.common.exception.ServerException;
import org.sunbird.graph.common.Identifier;
import org.sunbird.graph.common.enums.AuditProperties;
import org.sunbird.graph.common.enums.GraphDACParams;
import org.sunbird.graph.common.enums.SystemProperties;
import org.sunbird.graph.dac.enums.RelationTypes;
import org.sunbird.graph.dac.enums.SystemNodeTypes;
import org.sunbird.graph.dac.model.Node;
import org.sunbird.graph.dac.util.Neo4jNodeUtil;
import org.sunbird.graph.service.common.CypherQueryConfigurationConstants;
import org.sunbird.graph.service.common.DACErrorCodeConstants;
import org.sunbird.graph.service.common.DACErrorMessageConstants;
import org.sunbird.graph.service.common.GraphOperation;
import org.sunbird.graph.service.util.DriverUtil;
import org.sunbird.graph.service.util.NodeQueryGenerationUtil;
import org.sunbird.telemetry.logger.TelemetryManager;
import scala.Int;
import scala.compat.java8.FutureConverters;
import scala.concurrent.Future;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.CompletionStage;
import java.util.stream.Collectors;

public class NodeAsyncOperations {

    private final static String DEFAULT_CYPHER_NODE_OBJECT = "ee";


    public static Future<Node> addNode(String graphId, Node node) {
        if (StringUtils.isBlank(graphId))
            throw new ClientException(DACErrorCodeConstants.INVALID_GRAPH.name(),
                    DACErrorMessageConstants.INVALID_GRAPH_ID + " | [Create Node Operation Failed.]");

        if (null == node)
            throw new ClientException(DACErrorCodeConstants.INVALID_NODE.name(),
                    DACErrorMessageConstants.INVALID_NODE + " | [Create Node Operation Failed.]");

        Driver driver = DriverUtil.getDriver(graphId, GraphOperation.WRITE);
        TelemetryManager.log("Driver Initialised. | [Graph Id: " + graphId + "]");

        Map<String, Object> parameterMap = new HashMap<String, Object>();
        parameterMap.put(GraphDACParams.graphId.name(), graphId);
        parameterMap.put(GraphDACParams.node.name(), setPrimitiveData(node));
        NodeQueryGenerationUtil.generateCreateNodeCypherQuery(parameterMap);
        Map<String, Object> queryMap = (Map<String, Object>) parameterMap.get(GraphDACParams.queryStatementMap.name());
        Map<String, Object> entry = (Map<String, Object>) queryMap.entrySet().stream().findFirst().get().getValue();

        try (Session session = driver.session()) {
            String statementTemplate = StringUtils.removeEnd((String) entry.get(GraphDACParams.query.name()), CypherQueryConfigurationConstants.COMMA);
            Map<String, Object> statementParameters = (Map<String, Object>) entry.get(GraphDACParams.paramValueMap.name());
            CompletionStage<Node> cs = session.runAsync(statementTemplate, statementParameters)
            .thenCompose(fn -> fn.singleAsync())
            .thenApply(record -> {
                org.neo4j.driver.v1.types.Node neo4JNode = record.get(DEFAULT_CYPHER_NODE_OBJECT).asNode();
                String versionKey = (String) neo4JNode.get(GraphDACParams.versionKey.name()).asString();
                String identifier = (String) neo4JNode.get(SystemProperties.IL_UNIQUE_ID.name()).asString();
                node.setGraphId(graphId);
                node.setIdentifier(identifier);
                if (StringUtils.isNotBlank(versionKey))
                    node.getMetadata().put(GraphDACParams.versionKey.name(), versionKey);
                return node;
            }).exceptionally(error -> {
                        if (error.getCause() instanceof org.neo4j.driver.v1.exceptions.ClientException)
                            throw new ClientException(DACErrorCodeConstants.CONSTRAINT_VALIDATION_FAILED.name(), DACErrorMessageConstants.CONSTRAINT_VALIDATION_FAILED + node.getIdentifier());
                        else
                            throw new ServerException(DACErrorCodeConstants.SERVER_ERROR.name(),
                                    "Error! Something went wrong while creating node object. ", error.getCause());
            });
            return FutureConverters.toScala(cs);
        } catch (Throwable e) {
            e.printStackTrace();
            if (!(e instanceof MiddlewareException)) {
                throw new ServerException(DACErrorCodeConstants.CONNECTION_PROBLEM.name(),
                        DACErrorMessageConstants.CONNECTION_PROBLEM + " | " + e.getMessage(), e);
            } else {
                throw e;
            }
        }
    }

    public static Future<Node> upsertNode(String graphId, Node node, Request request) {
        TelemetryManager.log("Applying the Consumer Authorization Check for Node Id: " + node.getIdentifier());
        setRequestContextToNode(node, request);
        validateAuthorization(graphId, node, request);
        TelemetryManager.log("Consumer is Authorized for Node Id: " + node.getIdentifier());

        TelemetryManager.log("Validating the Update Operation for Node Id: " + node.getIdentifier());
        node.getMetadata().remove(GraphDACParams.versionKey.name());
        TelemetryManager.log("Node Update Operation has been Validated for Node Id: " + node.getIdentifier());

        Driver driver = DriverUtil.getDriver(graphId, GraphOperation.WRITE);
        TelemetryManager.log("Driver Initialised. | [Graph Id: " + graphId + "]");

        Map<String, Object> parameterMap = new HashMap<String, Object>();
        parameterMap.put(GraphDACParams.graphId.name(), graphId);
        parameterMap.put(GraphDACParams.node.name(), setPrimitiveData(node));
        parameterMap.put(GraphDACParams.request.name(), request);
        NodeQueryGenerationUtil.generateUpsertNodeCypherQuery(parameterMap);
        Map<String, Object> queryMap = (Map<String, Object>) parameterMap.get(GraphDACParams.queryStatementMap.name());
        Map<String, Object> entry = (Map<String, Object>) queryMap.entrySet().stream().findFirst().get().getValue();


        try(Session session = driver.session()) {
            String statement = StringUtils.removeEnd((String) entry.get(GraphDACParams.query.name()), CypherQueryConfigurationConstants.COMMA);
            Map<String, Object> statementParams = (Map<String, Object>) entry.get(GraphDACParams.paramValueMap.name());
            CompletionStage<Node> cs = session.runAsync(statement, statementParams).thenCompose(fn -> fn.singleAsync())
                    .thenApply(record -> {
                        org.neo4j.driver.v1.types.Node neo4JNode = record.get(DEFAULT_CYPHER_NODE_OBJECT).asNode();
                        String versionKey = (String) neo4JNode.get(GraphDACParams.versionKey.name()).asString();
                        String identifier = (String) neo4JNode.get(SystemProperties.IL_UNIQUE_ID.name()).asString();
                        node.setGraphId(graphId);
                        node.setIdentifier(identifier);
                        if (StringUtils.isNotBlank(versionKey))
                            node.getMetadata().put(GraphDACParams.versionKey.name(), versionKey);
                        return node;
                    }).exceptionally(error -> {
                        throw new ServerException(DACErrorCodeConstants.SERVER_ERROR.name(),
                                "Error! Something went wrong while creating node object. ", error.getCause());
                    });
            return FutureConverters.toScala(cs);
        } catch (Exception e) {
            if (!(e instanceof MiddlewareException)) {
                throw new ServerException(DACErrorCodeConstants.CONNECTION_PROBLEM.name(),
                        DACErrorMessageConstants.CONNECTION_PROBLEM + " | " + e.getMessage());
            } else {
                throw e;
            }
        }
    }

    public static Future<Map<String, Node>> updateNodes(String graphId, List<String> identifiers, Map<String, Object> data) {
        if (StringUtils.isBlank(graphId))
            throw new ClientException(DACErrorCodeConstants.INVALID_GRAPH.name(),
                    DACErrorMessageConstants.INVALID_GRAPH_ID + " | [Invalid or 'null' Graph Id.]");
        if (CollectionUtils.isEmpty(identifiers))
            throw new ClientException(DACErrorCodeConstants.INVALID_IDENTIFIER.name(),
                    DACErrorMessageConstants.INVALID_IDENTIFIER + " | [Please Provide Node Identifier.]");
        if (MapUtils.isEmpty(data))
            throw new ClientException(DACErrorCodeConstants.INVALID_METADATA.name(),
                    DACErrorMessageConstants.INVALID_METADATA + " | [Please Provide Valid Node Metadata]");

        Driver driver = DriverUtil.getDriver(graphId, GraphOperation.WRITE);
        TelemetryManager.log("Driver Initialised. | [Graph Id: " + graphId + "]");
        Map<String, Object> parameterMap = new HashMap<>();
        Map<String, Node> output = new HashMap<>();
        String query = NodeQueryGenerationUtil.generateUpdateNodesQuery(graphId, identifiers, setPrimitiveData(data), parameterMap);
        try (Session session = driver.session()) {
            CompletionStage<Map<String, Node>> cs = session.runAsync(query, parameterMap).thenCompose(fn -> fn.listAsync())
                    .thenApply(result -> {
                        if (null != result) {
                            for (Record record : result) {
                                if (null != record) {
                                    org.neo4j.driver.v1.types.Node neo4JNode = record.get(DEFAULT_CYPHER_NODE_OBJECT).asNode();
                                    String identifier = neo4JNode.get(SystemProperties.IL_UNIQUE_ID.name()).asString();
                                    Node node = Neo4jNodeUtil.getNode(graphId, neo4JNode, null, null, null);
                                    output.put(identifier, node);
                                }
                            }
                        }
                        return output;
                    }).exceptionally(error -> {
                        throw new ServerException(DACErrorCodeConstants.SERVER_ERROR.name(), "Error! Something went wrong while performing bulk update operations. ", error.getCause());
                    });
            return FutureConverters.toScala(cs);
        }
    }


    public static Future<Node> upsertRootNode(String graphId, Request request) throws Exception {
        if (StringUtils.isBlank(graphId))
            throw new ClientException(DACErrorCodeConstants.INVALID_GRAPH.name(),
                    DACErrorMessageConstants.INVALID_GRAPH_ID + " | [Upsert Root Node Operation Failed.]");

        Node node = new Node();
        node.setMetadata(new HashMap<String, Object>());
        Driver driver = DriverUtil.getDriver(graphId, GraphOperation.WRITE);
        TelemetryManager.log("Driver Initialised. | [Graph Id: " + graphId + "]");
        try (Session session = driver.session()) {
            TelemetryManager.log("Session Initialised. | [Graph Id: " + graphId + "]");

            // Generating Root Node Id
            String rootNodeUniqueId = Identifier.getIdentifier(graphId, SystemNodeTypes.ROOT_NODE.name());
            TelemetryManager.log("Generated Root Node Id: " + rootNodeUniqueId);

            node.setGraphId(graphId);
            node.setNodeType(SystemNodeTypes.ROOT_NODE.name());
            node.setIdentifier(rootNodeUniqueId);
            node.getMetadata().put(SystemProperties.IL_UNIQUE_ID.name(), rootNodeUniqueId);
            node.getMetadata().put(SystemProperties.IL_SYS_NODE_TYPE.name(), SystemNodeTypes.ROOT_NODE.name());
            node.getMetadata().put(AuditProperties.createdOn.name(), DateUtils.formatCurrentDate());
            node.getMetadata().put(GraphDACParams.Nodes_Count.name(), 0);
            node.getMetadata().put(GraphDACParams.Relations_Count.name(), 0);

            Map<String, Object> parameterMap = new HashMap<String, Object>();
            parameterMap.put(GraphDACParams.graphId.name(), graphId);
            parameterMap.put(GraphDACParams.rootNode.name(), node);
            parameterMap.put(GraphDACParams.request.name(), request);

            CompletionStage<Node> cs = session.runAsync(NodeQueryGenerationUtil.generateUpsertRootNodeCypherQuery(parameterMap))
                    .thenCompose(fn -> fn.singleAsync())
                    .thenApply(record -> {
                        org.neo4j.driver.v1.types.Node neo4JNode = record.get(DEFAULT_CYPHER_NODE_OBJECT).asNode();
                        String versionKey = (String) neo4JNode.get(GraphDACParams.versionKey.name()).asString();
                        String identifier = (String) neo4JNode.get(SystemProperties.IL_UNIQUE_ID.name()).asString();
                        node.setGraphId(graphId);
                        node.setIdentifier(identifier);
                        if (StringUtils.isNotBlank(versionKey))
                            node.getMetadata().put(GraphDACParams.versionKey.name(), versionKey);
                        return node;
                    }).exceptionally(error -> {
                        if (error.getCause() instanceof org.neo4j.driver.v1.exceptions.ServiceUnavailableException)
                            throw new ServerException(DACErrorCodeConstants.CONNECTION_PROBLEM.name(),
                                    DACErrorMessageConstants.CONNECTION_PROBLEM + " | " + error.getMessage(), error.getCause());
                        else
                            throw new ServerException(DACErrorCodeConstants.SERVER_ERROR.name(),
                                    "Error! Something went wrong while creating node object. ", error.getCause());
                    });
            return FutureConverters.toScala(cs);
        } catch (Exception e) {
                throw new ServerException(DACErrorCodeConstants.CONNECTION_PROBLEM.name(),
                        DACErrorMessageConstants.CONNECTION_PROBLEM + " | " + e.getMessage(), e);
        }
    }

    public static Future<List<Record>> bulkAddNodes(String graphId, Set<Map<String, String>> nodesData) {
        if (StringUtils.isBlank(graphId))
            throw new ClientException(DACErrorCodeConstants.INVALID_GRAPH.name(),
                    DACErrorMessageConstants.INVALID_GRAPH_ID + " | [Create Node Operation Failed.]");
        Driver driver = DriverUtil.getDriver(graphId, GraphOperation.WRITE);
        TelemetryManager.log("Driver Initialised. | [Graph Id: " + graphId + "]");
        String nodesDataString = nodesData.toString().replaceAll("=([A-Za-z0-9\\s_-]+)", "\\:\"$1\\\"");
        try (Session session = driver.session()) {
            String statementTemplate = "UNWIND " + nodesDataString + " as row CREATE (n:education) SET n += row return n";
            CompletionStage<List<Record>> cs = session.runAsync(statementTemplate)
                    .thenCompose(fn -> fn.listAsync())
                    .whenComplete((records, error) -> {
                        if (records == null)   error.printStackTrace();
                        session.closeAsync();
                    });
            return FutureConverters.toScala(cs);
        } catch (Throwable e) {
            e.printStackTrace();
            if (!(e instanceof MiddlewareException)) {
                throw new ServerException(DACErrorCodeConstants.CONNECTION_PROBLEM.name(),
                        DACErrorMessageConstants.CONNECTION_PROBLEM + " | " + e.getMessage(), e);
            } else {
                throw e;
            }
        }
    }

    public static Future<List<Record>> bulkCreateRelations(String graphId, Map<String, Int> relationData) {
        if (StringUtils.isBlank(graphId))
            throw new ClientException(DACErrorCodeConstants.INVALID_GRAPH.name(),
                    DACErrorMessageConstants.INVALID_GRAPH_ID + " | [Create Node Operation Failed.]");
        Driver driver = DriverUtil.getDriver(graphId, GraphOperation.WRITE);
        TelemetryManager.log("Driver Initialised. | [Graph Id: " + graphId + "]");
        List<Map<String, Object>> relData = getRelationData(relationData);
        Map<String, Object> dataMap = new HashMap<String, Object>() {{
            put("data", relData);
        }};
        try (Session session = driver.session()) {
            StringBuilder statementTemplate = new StringBuilder();
            statementTemplate.append("UNWIND {data} ");
            statementTemplate.append("AS ROW WITH ROW.startNodeId AS startNode, ROW.endNodeId AS endNode, ROW.relMetadata as relMetadata ");
            statementTemplate.append("MATCH(n:education {careerLevel:startNode}) ");
            statementTemplate.append("MATCH(m:education {careerLevel:endNode}) \n");
            statementTemplate.append("CREATE (n)-[r:associatedTo]->(m) set r += relMetadata ");
            statementTemplate.append("RETURN COUNT(*) AS RESULT;");
            CompletionStage<List<Record>> cs = session.runAsync(statementTemplate.toString(), dataMap)
                    .thenCompose(fn -> fn.listAsync())
                    .whenComplete((records, error) -> {
                        if (records == null) error.printStackTrace();
                        session.closeAsync();
                    });
            return FutureConverters.toScala(cs);
        } catch (Throwable e) {
            e.printStackTrace();
            if (!(e instanceof MiddlewareException)) {
                throw new ServerException(DACErrorCodeConstants.CONNECTION_PROBLEM.name(),
                        DACErrorMessageConstants.CONNECTION_PROBLEM + " | " + e.getMessage(), e);
            } else {
                throw e;
            }
        }
    }

    public static Future<List<Record>> getShortestPath(String graphId, String startNode, String endNode, List<Map<String, List<Map<String, String>>>> paths) {
        if (StringUtils.isBlank(graphId))
            throw new ClientException(DACErrorCodeConstants.INVALID_GRAPH.name(),
                    DACErrorMessageConstants.INVALID_GRAPH_ID + " | [Create Node Operation Failed.]");
        Driver driver = DriverUtil.getDriver(graphId, GraphOperation.WRITE);
        TelemetryManager.log("Driver Initialised. | [Graph Id: " + graphId + "]");
        try (Session session = driver.session()) {
            String statementTemplate = getTemplateStatement(startNode, endNode);
            CompletionStage<List<Record>> cs = session.runAsync(statementTemplate)
                    .thenCompose(fn -> fn.listAsync())
                    .whenComplete((records, error) -> {
                        if (records == null) error.printStackTrace();
                        else
                            records.forEach(record -> {
                                org.neo4j.driver.v1.types.Path path = record.get("path").asPath();
                                List<Map<String, String>> nodes = new ArrayList<>();
                                List<Map<String, String>> relations = new ArrayList<>();
                                path.nodes().forEach(node -> {
                                    nodes.add(new HashMap<String, String>() {{
                                        put("id", node.id() + "");
                                        put("name", node.get("careerLevel").asString());
                                    }});
                                });
                                path.relationships().forEach(relation -> {
                                    relations.add(new HashMap<String, String>() {{
                                        put("startId", relation.startNodeId() + "");
                                        put("endId", relation.endNodeId() + "");
                                        put("noOfPeople", relation.get("people") + "");
                                    }});
                                });
                                paths.add(new HashMap<String, List<Map<String, String>>>() {{
                                    put("nodes", nodes);
                                    put("relations", relations);
                                }});
                            });
                        session.closeAsync();
                    });
            return FutureConverters.toScala(cs);
        } catch (Throwable e) {
            e.printStackTrace();
            if (!(e instanceof MiddlewareException)) {
                throw new ServerException(DACErrorCodeConstants.CONNECTION_PROBLEM.name(),
                        DACErrorMessageConstants.CONNECTION_PROBLEM + " | " + e.getMessage(), e);
            } else {
                throw e;
            }
        }
    }

    private static List<Map<String, Object>> getRelationData(Map<String, Int> relData) {
       return relData.keySet().stream().map(key -> new HashMap<String, Object>() {{
            put("startNodeId", key.split("->")[0]);
            put("endNodeId", key.split("->")[1]);
            put("relMetadata", new HashMap<String, Object>() {{ put("people", relData.get(key));}});
        }}).collect(Collectors.toList());
    }

    private static String getTemplateStatement(String startNodeName, String endNodeName) {
        if (StringUtils.isNotBlank(startNodeName) && StringUtils.isNotBlank(endNodeName))
            return "MATCH (n:education {careerLevel:\"" + startNodeName + "\"}), (m:education {careerLevel:\"" + endNodeName + "\"}) MATCH path = allShortestPaths( (n)-[*]-(m) ) RETURN path";
        else if(StringUtils.isNotBlank(startNodeName) && StringUtils.isBlank(endNodeName) )
            return "MATCH path = (n:education {careerLevel: \""+ startNodeName +"\"})-[r *]->(m:education) return path;";
        else if(StringUtils.isNotBlank(endNodeName) && StringUtils.isBlank(startNodeName))
            return "MATCH path = (n:education)-[r *]->(m:education {careerLevel: \""+ endNodeName +"\"}) return path;";
        else return "MATCH path = (n:education)-[r *]->(m:education) return paths;";
    }

    private static Node setPrimitiveData(Node node) {
        Map<String, Object> metadata = node.getMetadata();
        metadata.entrySet().stream()
                .map(entry -> {
                    Object value = entry.getValue();
                    try {
                        if(value instanceof Map) {
                            value = JsonUtils.serialize(value);
                        } else if (value instanceof List) {
                            List listValue = (List) value;
                            if(CollectionUtils.isNotEmpty(listValue) && listValue.get(0) instanceof Map) {
                                value = JsonUtils.serialize(value);
                            }
                        }
                        entry.setValue(value);
                    } catch (Exception e) {
                        e.printStackTrace();
                    }

                    return entry;
                })
                .collect(HashMap::new, (m,v)->m.put(v.getKey(), v.getValue()), HashMap::putAll);
        return node;
    }

    private static Map<String, Object> setPrimitiveData(Map<String, Object> metadata) {
        metadata.entrySet().stream()
                .map(entry -> {
                    Object value = entry.getValue();
                    try {
                        if (value instanceof Map) {
                            value = JsonUtils.serialize(value);
                        } else if (value instanceof List) {
                            List listValue = (List) value;
                            if (CollectionUtils.isNotEmpty(listValue) && listValue.get(0) instanceof Map) {
                                value = JsonUtils.serialize(value);
                            }
                        }
                        entry.setValue(value);
                    } catch (Exception e) {
                        TelemetryManager.error("Exception Occurred While Processing Primitive Data Types | Exception is : " + e.getMessage(), e);
                    }

                    return entry;
                })
                .collect(HashMap::new, (m, v) -> m.put(v.getKey(), v.getValue()), HashMap::putAll);
        return metadata;
    }


    private static void setRequestContextToNode(Node node, Request request) {
        if (null != request && null != request.getContext()) {
            String channel = (String) request.getContext().get(GraphDACParams.CHANNEL_ID.name());
            TelemetryManager.log("Channel from request: " + channel + " for content: " + node.getIdentifier());
            if (StringUtils.isNotBlank(channel))
                node.getMetadata().put(GraphDACParams.channel.name(), channel);

            String consumerId = (String) request.getContext().get(GraphDACParams.CONSUMER_ID.name());
            TelemetryManager.log("ConsumerId from request: " + consumerId + " for content: " + node.getIdentifier());
            if (StringUtils.isNotBlank(consumerId))
                node.getMetadata().put(GraphDACParams.consumerId.name(), consumerId);

            String appId = (String) request.getContext().get(GraphDACParams.APP_ID.name());
            TelemetryManager.log("App Id from request: " + appId + " for content: " + node.getIdentifier());
            if (StringUtils.isNotBlank(appId))
                node.getMetadata().put(GraphDACParams.appId.name(), appId);
        }
    }

    private static void validateAuthorization(String graphId, Node node, Request request) {
        if (StringUtils.isBlank(graphId))
            throw new ClientException(DACErrorCodeConstants.INVALID_GRAPH.name(),
                    DACErrorMessageConstants.INVALID_GRAPH_ID + " | [Invalid or 'null' Graph Id.]");
        if (null == node)
            throw new ClientException(DACErrorCodeConstants.INVALID_NODE.name(),
                    DACErrorMessageConstants.INVALID_NODE + " | [Invalid or 'null' Node.]");
        if (null == request)
            throw new ClientException(DACErrorCodeConstants.INVALID_REQUEST.name(),
                    DACErrorMessageConstants.INVALID_REQUEST + " | [Invalid or 'null' Request Object.]");
    }
}
