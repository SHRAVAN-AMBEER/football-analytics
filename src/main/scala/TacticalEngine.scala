package org.tactical

import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions._
import org.apache.spark.sql.streaming.{GroupStateTimeout, GroupState, Trigger}
import org.apache.spark.graphx._
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.types._
import java.sql.Timestamp

// Case classes for parsed JSON
case class BallData(x: Double, y: Double, z: Double)
case class PlayerData(id: String, team: String, x: Double, y: Double, vel: Double)
case class Telemetry(timestamp: Long, ball: BallData, players: Array[PlayerData])

// Stateful tracking
case class PossessionState(possessorId: String, possessorTeam: String, startTime: Long)
case class PassEvent(fromPlayer: String, toPlayer: String, team: String, time: Long)

object TacticalEngine {

  def distance(x1: Double, y1: Double, x2: Double, y2: Double): Double = {
    math.sqrt(math.pow(x1 - x2, 2) + math.pow(y1 - y2, 2))
  }

  def updatePossession(
      key: String,
      values: Iterator[Telemetry],
      state: GroupState[PossessionState]
  ): Iterator[PassEvent] = {
    var passes = List[PassEvent]()
    var currentState = if (state.exists) state.get else PossessionState("None", "None", 0L)

    for (telemetry <- values) {
      val ball = telemetry.ball
      
      // Find player closest to the ball
      val (closestPlayer, minDist) = telemetry.players.map(p => 
        (p, distance(p.x, p.y, ball.x, ball.y))
      ).minBy(_._2)

      // Touch detection: within 2.5 meters (relaxed for simulation)
      if (minDist <= 2.5) {
        if (currentState.possessorId != "None" && currentState.possessorId != closestPlayer.id && currentState.possessorTeam == closestPlayer.team) {
          // Pass completed!
          passes = passes :+ PassEvent(
            currentState.possessorId, 
            closestPlayer.id, 
            closestPlayer.team,
            telemetry.timestamp
          )
        }
        currentState = PossessionState(closestPlayer.id, closestPlayer.team, telemetry.timestamp)
      }
    }
    
    state.update(currentState)
    passes.iterator
  }

  // Global pass accumulator for the simplistic graph building
  var allPasses: Seq[PassEvent] = Seq()

  def main(args: Array[String]): Unit = {
    val spark = SparkSession.builder
      .appName("FootballTacticalEngine")
      .master("local[*]")
      .getOrCreate()

    import spark.implicits._
    spark.sparkContext.setLogLevel("WARN")

    val kafkaTopic = "player_telemetry"
    val brokers = "localhost:9092"

    val df = spark.readStream
      .format("kafka")
      .option("kafka.bootstrap.servers", brokers)
      .option("subscribe", kafkaTopic)
      .load()

    val playerSchema = ArrayType(StructType(Array(
      StructField("id", StringType),
      StructField("team", StringType),
      StructField("x", DoubleType),
      StructField("y", DoubleType),
      StructField("vel", DoubleType)
    )))
    val ballSchema = StructType(Array(
      StructField("x", DoubleType), StructField("y", DoubleType), StructField("z", DoubleType)
    ))
    val schema = StructType(Array(
      StructField("timestamp", LongType),
      StructField("ball", ballSchema),
      StructField("players", playerSchema)
    ))

    val parsedDf = df.select(from_json(col("value").cast("string"), schema).as("data"))
      .select("data.*")
      .as[Telemetry]

    // We use a dummy key "1" so all tracking routes to a single state group (since its 1 match)
    val passStream = parsedDf
      .groupByKey(_ => "1")
      .flatMapGroupsWithState(org.apache.spark.sql.streaming.OutputMode.Update(), GroupStateTimeout.NoTimeout())(updatePossession)

    val query = passStream.writeStream
      .foreachBatch { (batchDF: org.apache.spark.sql.Dataset[PassEvent], batchId: Long) =>
        val newPasses = batchDF.collect()
        
        // Update global state with a sliding window of last 200 passes
        if (newPasses.nonEmpty) {
          allPasses = (allPasses ++ newPasses).takeRight(200)
          println(s"[SPARK-STREAM] --- Micro-Batch $batchId Progress ---\n[SPARK-STREAM] Detected ${newPasses.length} new passes. Total window: ${allPasses.length}")
        }

        // Even if no new passes, we can still re-calculate based on existing window or send possession
        if (allPasses.nonEmpty) {
          // Build GraphX graph from allPasses
          val uniquePlayers = allPasses.flatMap(p => Seq(p.fromPlayer, p.toPlayer)).distinct
          val playerToId = uniquePlayers.zipWithIndex.map{case (name, id) => (name, id.toLong)}.toMap
          val idToPlayer = playerToId.map(_.swap)

          val vertices: RDD[(VertexId, String)] = spark.sparkContext.parallelize(
            playerToId.map { case (name, id) => (id, name) }.toSeq
          )
          
          val edges: RDD[Edge[Int]] = spark.sparkContext.parallelize(allPasses).map { p =>
            Edge(playerToId(p.fromPlayer), playerToId(p.toPlayer), 1)
          }.distinct()

          val graph = Graph(vertices, edges)

          // 1. Triangle Count
          val triangles = graph.triangleCount().vertices
          val trianglesJoined = triangles.join(vertices).map {
            case (id, (count, name)) => s""""$name": $count"""
          }

          // 2. PageRank
          val pageRank = graph.pageRank(0.001).vertices
          val rankJoined = pageRank.join(vertices).map {
            case (id, (rank, name)) => s""""$name": $rank"""
          }

          val trianglesJson = "{" + trianglesJoined.collect().mkString(",") + "}"
          val rankJson = "{" + rankJoined.collect().mkString(",") + "}"
          
          // Determine current possession from the last pass
          val currentPossessorTeam = allPasses.lastOption.map(_.team).getOrElse("None")

          val insightJson = s"""{"type": "graph_metrics", "triangles": $trianglesJson, "influence": $rankJson, "possessionTeam": "$currentPossessorTeam"}"""
          
          println(s"--- Tactical Insights ---")
          println(s"Current Possession: $currentPossessorTeam")
          println(insightJson)
          println("-------------------------")

          // Send to Kafka
          import org.apache.kafka.clients.producer.{KafkaProducer, ProducerRecord}
          import java.util.Properties
          val props = new Properties()
          props.put("bootstrap.servers", brokers)
          props.put("key.serializer", "org.apache.kafka.common.serialization.StringSerializer")
          props.put("value.serializer", "org.apache.kafka.common.serialization.StringSerializer")
          val producer = new KafkaProducer[String, String](props)
          producer.send(new ProducerRecord[String, String]("tactical_insights", insightJson))
          producer.close()
        }
      }
      .outputMode("update")
      .trigger(Trigger.ProcessingTime("5 seconds"))
      .start()

    query.awaitTermination()
  }
}
