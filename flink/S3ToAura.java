package com.example;

import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.api.common.functions.FilterFunction;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.connector.file.src.FileSource;
import org.apache.flink.connector.file.src.reader.TextLineInputFormat;
import org.apache.flink.core.fs.Path;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.sink.legacy.SinkFunction;

import org.neo4j.driver.*;

import java.time.Duration;
import java.time.LocalDate;
import java.time.LocalDateTime;
import java.util.*;

public class S3ToAura {

  static class NodeRec { String id; Map<String,Object> props = new HashMap<>(); }
  static class EdgeRec { String src, dst, type; Map<String,Object> props = new HashMap<>(); }

  static boolean blank(String s){ return s==null || s.isEmpty(); }
  static Double toD(String s){ try { return blank(s)? null : Double.valueOf(s); } catch(Exception e){ return null; } }
  static Integer toI(String s){ try { return blank(s)? null : Integer.valueOf(s); } catch(Exception e){ return null; } }
  static LocalDate toDate(String s){ try { return blank(s)? null : LocalDate.parse(s); } catch(Exception e){ return null; } }
  static LocalDateTime toDT(String s){ try { return blank(s)? null : LocalDateTime.parse(s); } catch(Exception e){ return null; } }
  static String sanitizeRelType(String t){
    String up = t==null? "REL" : t.trim().toUpperCase().replaceAll("[^A-Z0-9_]", "_");
    return up.isEmpty()? "REL" : up;
  }

  // ---- Neo4j sinks (simple) ----
  static class Neo4jNodeSink implements SinkFunction<NodeRec> {
    private static transient Driver driver;
    private final String uri,user,pass;
    Neo4jNodeSink(String uri,String user,String pass){ this.uri=uri; this.user=user; this.pass=pass; }
    private static synchronized void ensure(String uri,String user,String pass){
      if (driver == null) {
        driver = GraphDatabase.driver(uri, AuthTokens.basic(user, pass)); // no manual TLS config
      }
    }
    @Override public void invoke(NodeRec v) {
      ensure(uri,user,pass);
      try (Session session = driver.session(SessionConfig.forDatabase("neo4j"))) {
        session.executeWrite(tx -> {
          tx.run("MERGE (b:Bank {id:$id}) SET b += $props",
                 Values.parameters("id", v.id, "props", v.props)).consume();
          return null;
        });
      }
    }
  }

  static class Neo4jEdgeSink implements SinkFunction<EdgeRec> {
    private static transient Driver driver;
    private final String uri,user,pass;
    Neo4jEdgeSink(String uri,String user,String pass){ this.uri=uri; this.user=user; this.pass=pass; }
    private static synchronized void ensure(String uri,String user,String pass){
      if (driver == null) {
        driver = GraphDatabase.driver(uri, AuthTokens.basic(user, pass)); // no manual TLS config
      }
    }
    @Override public void invoke(EdgeRec e) {
      ensure(uri,user,pass);

      // Use type-only MERGE (no props in the pattern) then SET properties.
      String q = String.format(
        "MERGE (s:Bank {id:$src}) " +
        "MERGE (t:Bank {id:$dst}) " +
        "MERGE (s)-[r:%s]->(t) " +
        "SET r += $props", e.type);

      Map<String,Object> params = new HashMap<>();
      params.put("src", e.src);
      params.put("dst", e.dst);
      params.put("props", e.props);  // <-- provide the map as 'props'

      try (Session session = driver.session(SessionConfig.forDatabase("neo4j"))) {
        session.executeWrite(tx -> { tx.run(q, params).consume(); return null; });
      }
    }
  }

  public static void main(String[] args) throws Exception {
    Map<String,String> A = parseArgs(args);
    String s3Prefix = A.getOrDefault("s3Prefix","s3://risk-prediction-multilayer/graph/integrated/neo4j_import");
    String auraUri  = A.getOrDefault("auraUri",  System.getenv("AURA_URI"));
    String auraUser = A.getOrDefault("auraUser", System.getenv("AURA_USER"));
    String auraPass = A.getOrDefault("auraPass", System.getenv("AURA_PASS"));
    if (auraUri==null || auraUser==null || auraPass==null)
      throw new IllegalArgumentException("Provide Aura creds via args or env (AURA_URI/AURA_USER/AURA_PASS).");

    String s3Nodes = s3Prefix.replaceAll("/$","") + "/nodes/";
    String s3Edges = s3Prefix.replaceAll("/$","") + "/edges/";

    StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();

    // NODES
    FileSource<String> nodesSrc = FileSource
      .forRecordStreamFormat(new TextLineInputFormat(), new Path(s3Nodes))
      .monitorContinuously(Duration.ofSeconds(5))
      .build();

    DataStream<NodeRec> nodes = env.fromSource(nodesSrc, WatermarkStrategy.noWatermarks(), "nodes-s3")
      .filter((FilterFunction<String>) line -> !line.startsWith("node_id:ID"))
      .map((MapFunction<String,NodeRec>) line -> {
        String[] c = line.split(",", -1);
        NodeRec n = new NodeRec();
        n.id = c[0];
        n.props.put("node_type", c[1]); n.props.put("network_layer", c[2]); n.props.put("name", c[3]);
        n.props.put("institution_id", c[4]); n.props.put("tier", c[5]);
        n.props.put("total_assets", toD(c[6])); n.props.put("total_deposits", toD(c[7])); n.props.put("total_loans", toD(c[8]));
        n.props.put("equity", toD(c[9])); n.props.put("num_branches", toI(c[10])); n.props.put("num_employees", toI(c[11]));
        n.props.put("cert", blank(c[12])? null : c[12]); n.props.put("assets", toD(c[13])); n.props.put("city", blank(c[14])? null : c[14]);
        n.props.put("state", blank(c[15])? null : c[15]); n.props.put("ticker", blank(c[16])? null : c[16]);
        n.props.put("sector", blank(c[17])? null : c[17]); n.props.put("industry", blank(c[18])? null : c[18]);
        n.props.put("market_cap", toD(c[19])); n.props.put("trailing_pe", toD(c[20])); n.props.put("debt_to_equity", toD(c[21]));
        n.props.put("beta", toD(c[22])); n.props.put("cik", blank(c[23])? null : c[23]); n.props.put("created_at", toDT(c[24]));
        n.props.values().removeIf(Objects::isNull);
        return n;
      });

    nodes.addSink(new Neo4jNodeSink(auraUri, auraUser, auraPass));

    // EDGES
    FileSource<String> edgesSrc = FileSource
      .forRecordStreamFormat(new TextLineInputFormat(), new Path(s3Edges))
      .monitorContinuously(Duration.ofSeconds(5))
      .build();

    DataStream<EdgeRec> edges = env.fromSource(edgesSrc, WatermarkStrategy.noWatermarks(), "edges-s3")
      .filter((FilterFunction<String>) line -> !line.startsWith(":START_ID"))
      .map((MapFunction<String,EdgeRec>) line -> {
        String[] c = line.split(",", -1);
        EdgeRec e = new EdgeRec();
        e.src = c[0]; e.dst = c[1]; e.type = sanitizeRelType(c[2]);
        e.props.put("network_layer", c[3]); e.props.put("weight", toD(c[4]));
        e.props.put("tx_date", toDate(c[5])); e.props.put("maturity_days", toI(c[6]));
        e.props.put("interest_rate", toD(c[7])); e.props.put("currency", blank(c[8])? null : c[8]);
        e.props.put("correlation", toD(c[9])); e.props.put("window_days", toI(c[10]));
        e.props.put("shares_held", toD(c[11])); e.props.put("created_at", toDT(c[12]));
        e.props.values().removeIf(Objects::isNull);
        return e;
      });

    edges.addSink(new Neo4jEdgeSink(auraUri, auraUser, auraPass));

    env.execute("S3 → AuraDB (Flink) — fixed props param");
  }

  private static Map<String,String> parseArgs(String[] args){
    Map<String,String> m = new HashMap<>();
    for (int i = 0; i < args.length - 1; i += 2) m.put(args[i].replaceFirst("^--",""), args[i+1]);
    return m;
  }
}
