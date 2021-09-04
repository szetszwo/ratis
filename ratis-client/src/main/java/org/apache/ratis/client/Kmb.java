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
import org.apache.ratis.thirdparty.org.checkerframework.checker.units.qual.K;

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

public class Kmb {
  static final String URL_PREFIX     = "https://data.etabus.gov.hk/v1/transport/kmb/";
  static final String ROUTE           = URL_PREFIX + "route/";
  static final String STOP_ETA_PREFIX = URL_PREFIX + "stop-eta/";
  static final String STOP_PREFIX     = URL_PREFIX + "stop/";

  static final JsonParser JSON_PARSER = new JsonParser();

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

      private final Bound bound; // "O"
      private final int serviceType; // "1"

      Type(Bound bound, int serviceType) {
        this.bound = bound;
        this.serviceType = serviceType;
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
    }

    static class OrigDest {
      private final Name orig;       // "PO LAM", "寶林", "宝林"
      private final Name dest;       // "DIAMOND HILL STATION", "鑽石山站", "钻石山站"

      OrigDest(Name orig, Name dest) {
        this.orig = orig;
        this.dest = dest;
      }

      @Override
      public String toString() {
        return orig + "->" + dest;
      }
    }

    private final String route;    // "91M"
    private final ConcurrentMap<Type, OrigDest> types = new ConcurrentHashMap<>();

    Route(String route) {
      this.route = route;
    }

    void put(JsonObject json) {
      if (!route.equalsIgnoreCase(json.get("route").getAsString())) {
        throw new IllegalArgumentException("Route mismatched: route=" + route + " but " + json);
      }

      final Type type = Type.valueOf(Bound.parse(json.get("bound").getAsString()),
          Type.getServiceType(json));
      final OrigDest origDest = new OrigDest(
          Name.get("orig", json),
          Name.get("dest", json));
      types.put(type, origDest);
    }

    @Override
    public String toString() {
      return route + types.values();
    }
  }

  static class BusStop {
    static BusStop get(String stopId) {
      final JsonObject json = JSON_PARSER.parse(readLine(STOP_PREFIX + stopId)).getAsJsonObject();
      final JsonObject data = json.getAsJsonObject("data");
      return new BusStop(stopId, Name.get("name", data), Coordinate.get(data));
    }

    private final String id;
    private final Name name;
    private final Coordinate coordinate;

    BusStop(String id, Name name, Coordinate coordinate) {
      this.id = id;
      this.name = name;
      this.coordinate = coordinate;
    }

    String getStopEtaUrl() {
      return STOP_ETA_PREFIX + id;
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
      return new Eta(stop,
          Company.valueOf(eta.get("co").getAsString()),
          eta.get("route").getAsString(),
          Bound.parse(eta.get("dir").getAsString()),
          Route.Type.getServiceType(eta),
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
    private final Bound bound;
    private final int serviceType;
    private final int seq;
    private final Name dest;
    private final int etaSeq;
    private final DateTime eta;
    private final Name remark;
    private final DateTime timestamp;

    Eta(BusStop stop, Company company, String route, Bound bound, int serviceType, int seq,
        Name dest, int etaSeq, DateTime eta, Name remark, DateTime timestamp) {
      this.stop = stop;
      this.company = company;
      this.route = route;
      this.bound = bound;
      this.serviceType = serviceType;
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

    @Override
    public String toString() {
      final String s = eta != null? " will arrive " + stop + " at " + eta : "has NO eta";
      return company + " " + route + " to " + dest + s + " (" + remark + " " + etaSeq + ")";
    }
  }

  private final Map<String, Route> routes;

  Kmb() {
    final JsonObject obj = JSON_PARSER.parse(readLine(ROUTE)).getAsJsonObject();
    final JsonArray data = obj.getAsJsonArray("data");

    final Map<String, Route> map = new TreeMap<>();
    for (int i = 0; i < data.size(); i++) {
      final JsonObject json = data.get(i).getAsJsonObject();
      final String route = json.get("route").getAsString();
      map.compute(route, (k, v) -> v != null ? v : new Route(route)).put(json);
    }
    routes = Collections.unmodifiableMap(map);
  }

  Route get(String route) {
    return routes.get(route);
  }

  void print(Consumer<Object> out) {
    routes.values().forEach(out);
  }

  public static void main(String[] args) throws Exception {
    initTrustManager();

    final BusStop stop = BusStop.get("4B9D547F0F450784");
    final String route = "91M";
    readEta(route, stop, System.out::println);
    readEta(null, stop, System.out::println);

    final Kmb kmb = new Kmb();
    System.out.println(kmb.get("91M"));
    System.out.println(kmb.get("92"));
//    kmb.print(System.out::println);
  }

  static void readEta(String route, BusStop stop, Consumer<Object> out) {
    final JsonObject obj = JSON_PARSER.parse(readLine(stop.getStopEtaUrl())).getAsJsonObject();
    final JsonArray data = obj.getAsJsonArray("data");
    for (int i = 0; i < data.size(); i++) {
      final Eta eta = Eta.get(stop, data.get(i).getAsJsonObject());
      if (route == null || eta.getRoute().equalsIgnoreCase(route)) {
        out.accept(eta);
      }
    }
  }

  static String readLine(String url) {
    System.err.println("readLine " + url);
    String line = null;
    try(BufferedReader in = new BufferedReader(new InputStreamReader(new URL(url).openStream()))) {
      line = in.readLine();
    } catch (IOException e) {
      e.printStackTrace();
    }
    System.err.println(" " + line);
    return line;
  }
}
