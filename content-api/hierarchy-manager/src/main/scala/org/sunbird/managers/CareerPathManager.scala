package org.sunbird.managers

import java.io.File

import scala.collection.JavaConversions._
import scala.io.Source
import scala.collection.JavaConverters
import java.util
import java.util.concurrent.CompletionException

import org.sunbird.common.dto.{Request, Response, ResponseHandler}
import org.sunbird.graph.service.operation.NodeAsyncOperations

import scala.concurrent.{ExecutionContext, Future}

object CareerPathManager {
    def createCareerPath(request: Request)(implicit ec: ExecutionContext): Future[Response] = {
        val file = request.getRequest.get("file").asInstanceOf[File]
        val response = ResponseHandler.OK()
        response.put("fileName", file.getName)
        val nodesData = new util.HashSet[util.Map[String, String]]()
        nodesData.add(new util.HashMap[String, String]() {
            {
                put("careerLevel", "global")
                put("type", "Education")
            }
        })

        val relationFrequencyMap: util.Map[String, Int] = new util.HashMap[String, Int]()
        nodesData.addAll(getEducationNodeMaps(file, relationFrequencyMap))
        NodeAsyncOperations.bulkAddNodes("domain", nodesData).map(res => {
            NodeAsyncOperations.bulkCreateRelations("domain", relationFrequencyMap)
                .map(res => response)
        }).flatMap(f => f)
    }

    def getEducationNodeMaps(file: File, relationFrequency: util.Map[String, Int]): util.Set[util.HashMap[String, String]] = {
        val bufferedSource = Source.fromFile(file)
        //        bufferedSource.getLines().drop(1)
        val educationSet: util.HashSet[String] = new util.HashSet[String]()
        val educationRelationList: util.ArrayList[String] = new util.ArrayList[String]()
        bufferedSource.getLines().foreach(line => {
            val cols = line.split(",").map(_.trim).toList
            educationSet.addAll(cols)
            educationRelationList.add("global->" + cols.get(0))
            for (i <- 0 until cols.length - 1) educationRelationList.add(cols.get(i) + "->" + cols.get(i + 1))
        })
        val relation = educationRelationList.groupBy(identity).mapValues(_.map(_ => 1).sum)
        relationFrequency.putAll(mapAsJavaMap(relation))
        bufferedSource.close
        setAsJavaSet(educationSet.map(value => new util.HashMap[String, String]() {
            {
                put("careerLevel", value)
                put("type", "Education")
            }
        }))
    }

    def getCareerPath(request: Request)(implicit ec: ExecutionContext): Future[Response] = {
        val startNodeName = request.get("startNodeName").asInstanceOf[String]
        val endNodeName = request.get("endNodeName").asInstanceOf[String]
        val containsNodeName = request.get("containsNodeName").asInstanceOf[String]
        val relations: util.Map[String, util.List[util.Map[String, String]]] = new util.HashMap[String, util.List[util.Map[String, String]]]()
        val response = ResponseHandler.OK()
        NodeAsyncOperations.getShortestPath("domain", startNodeName, endNodeName, containsNodeName, relations).map(res => {
            response.put("careerPaths", relations)
            response
        })
    }

}
