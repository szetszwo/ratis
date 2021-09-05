/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.apache.ratis.client;

import org.apache.ratis.thirdparty.com.google.gson.JsonArray;
import org.apache.ratis.thirdparty.com.google.gson.JsonElement;
import org.apache.ratis.thirdparty.com.google.gson.JsonObject;
import org.apache.ratis.thirdparty.com.google.gson.JsonParser;
import org.apache.ratis.util.MemoizedSupplier;

import javax.net.ssl.HttpsURLConnection;
import javax.net.ssl.SSLContext;
import javax.net.ssl.TrustManager;
import javax.net.ssl.X509TrustManager;
import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStreamReader;
import java.net.URL;
import java.security.KeyManagementException;
import java.security.NoSuchAlgorithmException;
import java.security.SecureRandom;
import java.security.cert.X509Certificate;
import java.time.LocalDate;
import java.time.LocalTime;
import java.time.format.DateTimeFormatter;
import java.util.*;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;
import java.util.function.Consumer;
import java.util.function.Supplier;

public class Kmb {
  static class Print {
    static synchronized void println(String s, Consumer<String> out) {
      final int max = 1000;
      out.accept(s == null ? "<EMPTY_LINE>" : s.length() < max ? s : s.substring(0, max) + " ...");
    }

    static void ln() {
      ln("");
    }

    static void ln(Object s) {
      println(Objects.toString(s), System.out::println);
    }

    static void debug(String s) {
      println(s, System.err::println);
    }
  }

  interface URLs {
    String BASE = "https://data.etabus.gov.hk/v1/transport/kmb/";
    String ROUTE = BASE + "route";
    String STOP = BASE + "stop";
    String STOP_PREFIX = BASE + "stop/";
    String STOP_ETA_PREFIX = BASE + "stop-eta/";

    String ROUTE_STOP_PREFIX = BASE + "route-stop/"; // route-stop/92/inbound/1

    JsonParser JSON_PARSER = new JsonParser();

    static List<BusStop> readRouteStop(Route route, Route.Type type, BusStopMap stopMap) {
      final String url = ROUTE_STOP_PREFIX + route.getId() + "/" + type;
      final JsonArray array = JSON_PARSER.parse(readLine(url)).getAsJsonObject().getAsJsonArray("data");
      final List<BusStop> stops = new ArrayList<>(array.size());
      for(int i = 0; i < array.size(); i++) {
        final String stopId = array.get(i).getAsJsonObject().get("stop").getAsString();
        stops.add(Objects.requireNonNull(stopMap.get(stopId)));
      }
      return Collections.unmodifiableList(stops);
    }

    static Map<String, Route> readRoutes(BusStopMap stops) {
      final JsonArray routes = JSON_PARSER.parse(readLine(ROUTE)).getAsJsonObject().getAsJsonArray("data");
      final Map<String, Route> map = new TreeMap<>();
      for (int i = 0; i < routes.size(); i++) {
        final JsonObject json = routes.get(i).getAsJsonObject();
        final String route = json.get("route").getAsString();
        map.compute(route, (k, v) -> v != null ? v : new Route(route)).put(json, stops);
      }
      return Collections.unmodifiableMap(map);
    }

    static Map<String, BusStop> readStops() {
      final JsonArray stops = JSON_PARSER.parse(readLine(STOP)).getAsJsonObject().getAsJsonArray("data");
      final Map<String, BusStop> map = new TreeMap<>();
      for (int i = 0; i < stops.size(); i++) {
        final JsonObject json = stops.get(i).getAsJsonObject();
        final BusStop stop = BusStop.valueOf(json);
        map.put(stop.getId(), stop);
      }
      return Collections.unmodifiableMap(map);
    }

    static JsonObject readStop(String stopId) {
      return JSON_PARSER.parse(readLine(STOP_PREFIX + stopId)).getAsJsonObject();
    }

    ConcurrentMap<BusStop, List<Eta>> ETAS = new ConcurrentHashMap<>();

    static List<Eta> getStopEtas(BusStop stop) {
      return ETAS.compute(stop, (k, v) -> v != null? v: readStopEtas(stop));
    }

    static List<Eta> readStopEtas(BusStop stop) {
      final String url = STOP_ETA_PREFIX + stop.getId();
      final JsonArray data = JSON_PARSER.parse(readLine(url)).getAsJsonObject().getAsJsonArray("data");
      final List<Eta> etas = new ArrayList<>(data.size());
      for (int i = 0; i < data.size(); i++) {
        etas.add(Eta.get(stop, data.get(i).getAsJsonObject()));
      }
      return etas;
    }

    static String readLine(String url) {
      Print.debug("readLine " + url);
      String line = null;
      try(BufferedReader in = new BufferedReader(new InputStreamReader(new URL(url).openStream()))) {
        line = in.readLine();
      } catch (IOException e) {
        e.printStackTrace();
      }
      Print.debug(line);
      return line;
    }
  }


  static void initTrustManager() throws NoSuchAlgorithmException, KeyManagementException {
    // All-trusting trust manager
    final TrustManager[] trustAllCerts = new TrustManager[]{new X509TrustManager() {
      @Override
      public X509Certificate[] getAcceptedIssuers() {
        return null;
      }
      @Override
      public void checkClientTrusted(X509Certificate[] certs, String authType) {
      }
      @Override
      public void checkServerTrusted(X509Certificate[] certs, String authType) {
      }
    }};

    // Install the all-trusting trust manager
    final SSLContext sc = SSLContext.getInstance("SSL");
    sc.init(null, trustAllCerts, new SecureRandom());
    HttpsURLConnection.setDefaultSSLSocketFactory(sc.getSocketFactory());
  }

  static class Name {
    static Name get(String prefix, JsonObject obj) {
      return new Name(
          obj.get(prefix + "_tc").getAsString(),
          obj.get(prefix + "_sc").getAsString(),
          obj.get(prefix + "_en").getAsString());
    }

    private final String tc;
    private final String sc;
    private final String en;

    Name(String tc, String sc, String en) {
      this.tc = tc;
      this.sc = sc;
      this.en = en;
    }

    @Override
    public String toString() {
      return tc;
    }
  }

  static class Coordinate {
    static Coordinate get(JsonObject obj) {
      return new Coordinate(
          obj.get("lat").getAsDouble(),
          obj.get("long").getAsDouble());
    }

    private final double latitude;
    private final double longitude;

    Coordinate(double latitude, double longitude) {
      this.latitude = latitude;
      this.longitude = longitude;
    }
  }

  static class RouteMap {
    private final Map<String, Route> map;

    RouteMap(Map<String, Route> map) {
      this.map = map;
    }

    Route get(String routeId) {
      return Objects.requireNonNull(map.get(routeId), () -> "Failed to get " + routeId);
    }

    void print(Consumer<Object> out) {
      map.values().forEach(out);
    }
  }

  static class Route {
    static class Type {
      private static ConcurrentMap<Integer, Map<Bound, Type>> TYPES = new ConcurrentHashMap<>();
      private static Map<Bound, Type> newEnumMap(int serviceType) {
        final EnumMap<Bound, Type> map = new EnumMap<>(Bound.class);
        map.put(Bound.INBOUND, new Type(Bound.INBOUND, serviceType));
        map.put(Bound.OUTBOUND, new Type(Bound.OUTBOUND, serviceType));
        return Collections.unmodifiableMap(map);
      }

      static Type valueOf(Bound bound, int serviceType) {
        return TYPES.compute(serviceType, (k, v) -> v != null? v: newEnumMap(serviceType))
            .get(bound);
      }

      static int getServiceType(JsonObject obj) {
        return Integer.parseInt(obj.get("service_type").getAsString());
      }

      private final Bound bound;
      private final int serviceType; // "1"
      private final String name;

      Type(Bound bound, int serviceType) {
        this.bound = bound;
        this.serviceType = serviceType;
        this.name = bound.name().toLowerCase() + "/" + serviceType;
      }

      @Override
      public boolean equals(Object obj) {
        if (this == obj) {
          return true;
        } else if (!(obj instanceof Type)) {
          return false;
        }
        final Type that = (Type) obj;
        return this.serviceType == that.serviceType && this.bound == that.bound;
      }

      @Override
      public int hashCode() {
        return Objects.hash(bound, serviceType);
      }

      @Override
      public String toString() {
        return name;
      }
    }

    static class OrigDest {
      private final Name orig;       // "PO LAM", "寶林", "宝林"
      private final Name dest;       // "DIAMOND HILL STATION", "鑽石山站", "钻石山站"
      private final Supplier<List<BusStop>> stops;

      OrigDest(Name orig, Name dest, Supplier<List<BusStop>> stops) {
        this.orig = orig;
        this.dest = dest;
        this.stops = stops;
      }

      boolean isLast(int seq) {
        return stops.get().size() == seq;
      }

      @Override
      public String toString() {
        return orig + "->" + dest;
      }
    }

    private final String id;    // "91M"
    private final ConcurrentMap<Type, OrigDest> types = new ConcurrentHashMap<>();

    Route(String id) {
      this.id = id;
    }

    String getId() {
      return id;
    }

    OrigDest getOrigDest(Type type) {
      return types.get(type);
    }

    void put(JsonObject json, BusStopMap stops) {
      if (!id.equalsIgnoreCase(json.get("route").getAsString())) {
        throw new IllegalArgumentException("Route mismatched: route=" + id + " but " + json);
      }

      final Type type = Type.valueOf(Bound.parse(json.get("bound").getAsString()),
          Type.getServiceType(json));
      final OrigDest origDest = new OrigDest(
          Name.get("orig", json),
          Name.get("dest", json),
          MemoizedSupplier.valueOf(() -> URLs.readRouteStop(this, type, stops)));
      types.put(type, origDest);
    }

    boolean match(Eta eta) {
      return id.equalsIgnoreCase(eta.getRoute());
    }

    @Override
    public String toString() {
      return id + types.values();
    }
  }

  static class BusStopMap {
    private final Map<String, BusStop> map;

    BusStopMap(Map<String, BusStop> map) {
      this.map = map;
    }

    BusStop get(String stopId) {
      return Objects.requireNonNull(map.get(stopId), () -> "Failed to get BusStop " + stopId);
    }
  }

  static class BusStop {
    static BusStop get(String stopId) {
      final JsonObject json = URLs.readStop(stopId);
      final JsonObject data = json.getAsJsonObject("data");
      return new BusStop(stopId, Name.get("name", data), Coordinate.get(data));
    }

    static BusStop valueOf(JsonObject json) {
      return new BusStop(json.get("stop").getAsString(), Name.get("name", json), Coordinate.get(json));
    }

    private final String id;
    private final Name name;
    private final Coordinate coordinate;

    BusStop(String id, Name name, Coordinate coordinate) {
      this.id = id;
      this.name = name;
      this.coordinate = coordinate;
    }

    String getId() {
      return id;
    }

    @Override
    public boolean equals(Object obj) {
      if (this == obj) {
        return true;
      } else if (!(obj instanceof BusStop)) {
        return false;
      }
      final BusStop that = (BusStop) obj;
      return Objects.equals(this.id, that.id);
    }

    @Override
    public int hashCode() {
      return Objects.hash(id);
    }

    @Override
    public String toString() {
      return "" + name;
    }
  }

  enum Company {KMB, LWS};
  enum Bound {
    INBOUND, OUTBOUND;

    boolean match(String symbol) {
      if (symbol == null) {
        return false;
      }
      if (symbol.length() > name().length()) {
        symbol = symbol.substring(0, name().length());
      }
      final String sub = name().substring(0, symbol.length());
      return sub.equalsIgnoreCase(symbol);
    }

    static Bound parse(String symbol) {
      for(Bound b : values()) {
        if (b.match(symbol)) {
          return b;
        }
      }
      return null;
    }
  };

  static class DateTime {
    static DateTime valueOf(JsonElement e) {
      return valueOf(e.isJsonNull()? null: e.getAsString());
    }

    static DateTime valueOf(String s) {
      return s == null || "null".equals(s)? null: new DateTime(
          LocalDate.parse(s, DateTimeFormatter.ISO_OFFSET_DATE_TIME),
          LocalTime.parse(s, DateTimeFormatter.ISO_OFFSET_DATE_TIME));
    }

    private final LocalDate date;
    private final LocalTime time;

    DateTime(LocalDate date, LocalTime time) {
      this.date = date;
      this.time = time;
    }

    @Override
    public String toString() {
      return time + " on " + date;
    }
  }

  static class Eta {
    static Eta get(BusStop stop, JsonObject eta) {
      final Route.Type type = Route.Type.valueOf(
          Bound.parse(eta.get("dir").getAsString()),
          Route.Type.getServiceType(eta));
      return new Eta(stop,
          Company.valueOf(eta.get("co").getAsString()),
          eta.get("route").getAsString(),
          type,
          Integer.parseInt(eta.get("seq").getAsString()),
          Name.get("dest", eta),
          Integer.parseInt(eta.get("eta_seq").getAsString()),
          DateTime.valueOf(eta.get("eta")),
          Name.get("rmk", eta),
          DateTime.valueOf(eta.get("data_timestamp")));
    }

    private final BusStop stop;

    private final Company company;
    private final String route;
    private final Route.Type type;
    private final int seq;
    private final Name dest;
    private final int etaSeq;
    private final DateTime eta;
    private final Name remark;
    private final DateTime timestamp;

    Eta(BusStop stop, Company company, String route, Route.Type type, int seq,
        Name dest, int etaSeq, DateTime eta, Name remark, DateTime timestamp) {
      this.stop = stop;
      this.company = company;
      this.route = route;
      this.type = type;
      this.seq = seq;
      this.dest = dest;
      this.etaSeq = etaSeq;
      this.eta = eta;
      this.remark = remark;
      this.timestamp = timestamp;
    }

    String getRoute() {
      return route;
    }

    Route.Type getType() {
      return type;
    }

    int getSeq() {
      return seq;
    }

    @Override
    public String toString() {
      final String t = stop + "(Stop " + seq  + ")";
      final String s = eta != null? " will arrive " + t + " at " + eta : " has NO eta for " + t;
      return company + " " + route + " to " + dest + s + " (" + remark + " " + etaSeq + ")";
    }
  }

  private final BusStopMap stops;
  private final RouteMap routes;

  Kmb() {
    stops = new BusStopMap(URLs.readStops());
    routes = new RouteMap(URLs.readRoutes(stops));
  }

  Route getRoute(String route) {
    return routes.get(route);
  }

  BusStop getBusStop(String stopId) {
    return stops.get(stopId);
  }

  public static void main(String[] args) throws Exception {
    initTrustManager();

    final Kmb kmb = new Kmb();
    final Route bus91M = kmb.getRoute("91M");
    final Route bus92 = kmb.getRoute("92");

    final BusStop lungPoonCourt = kmb.getBusStop("4B9D547F0F450784");
//    kmb.printEta(bus91M, lungPoonCourt, Print::ln);
    kmb.printEta(null, lungPoonCourt, Print::ln);

    final BusStop diamondHillStationBusTerminus91M = kmb.getBusStop("53889000AA9C33E2");
//    kmb.printEta(bus91M, diamondHillStationBusTerminus91M, Print::ln);
    kmb.printEta(null, diamondHillStationBusTerminus91M, Print::ln);

    final BusStop diamondHillStationBusTerminus92 = kmb.getBusStop("10B8C166D8E60F65");
//    kmb.printEta(bus92, diamondHillStationBusTerminus92, Print::ln);
    kmb.printEta(null, diamondHillStationBusTerminus92, Print::ln);

    final BusStop chiLinNunnery = kmb.getBusStop("951CE3B3EB98BA3A");
    kmb.printEta(null, chiLinNunnery, Print::ln);

    final BusStop shunLeeFireStation = kmb.getBusStop("927CE95D5C98C195");
    kmb.printEta(null, shunLeeFireStation, Print::ln);
  }

  void printEta(Route route, BusStop stop, Consumer<Object> out) {
    Print.ln();
    Print.ln("ETA for " + stop);
    final List<Eta> data = URLs.getStopEtas(stop);
    for (int i = 0; i < data.size(); i++) {
      final Eta eta = data.get(i);
      final Route r;
      if (route == null) {
        r = routes.get(eta.getRoute());
      } else if (route.match(eta)) {
        r = route;
      } else {
        r = null;
      }

      if (r != null && !r.getOrigDest(eta.getType()).isLast(eta.getSeq())) {
        out.accept(eta);
      }
    }
  }
}
