package kfk.stat;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStreamReader;
import java.lang.management.ManagementFactory;
import java.lang.management.MemoryMXBean;
import java.lang.management.MemoryUsage;
import java.lang.management.PlatformManagedObject;
import java.lang.management.RuntimeMXBean;
import java.time.Duration;
import java.time.Instant;
import java.time.ZoneId;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.Collections;
import java.util.Comparator;
import java.util.HashMap;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Optional;
import java.util.Set;
import java.util.StringJoiner;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.function.BiFunction;
import java.util.function.Function;
import java.util.function.Predicate;
import java.util.stream.Collectors;
import java.util.stream.IntStream;
import java.util.stream.Stream;
import javax.management.MBeanServerConnection;
import javax.management.ObjectName;
import javax.management.remote.JMXConnector;
import javax.management.remote.JMXConnectorFactory;
import javax.management.remote.JMXServiceURL;
import joptsimple.OptionException;
import joptsimple.OptionParser;
import joptsimple.OptionSet;
import joptsimple.OptionSpecBuilder;

public class KfkStat {

  public static void main(String[] args) {
    mainThing(args);
  }

  static void mainThing(String[] args) {
    debug("args: " + Arrays.asList(args));
    final OptionParser parser = new OptionParser(true /* allowAbbreviations */);

    OptionSpecBuilder perf = parser.accepts("performance", "print throughput stats. q - Req/RespQueue RespSend TimeMs (default: Local/Remote/Total TimeMs)");
    perf.withOptionalArg();

    OptionSpecBuilder topic = parser.accepts("Topic", "print stats aggregated by topic name prefix. 2 - TxxNN (default: T)");
    topic.withOptionalArg().ofType(int.class);

    parser.mutuallyExclusive(
        parser.accepts("throughput", "print throughput stats"),
        perf,
        parser.accepts("availability", "print availability stats"),
        topic);

    parser.accepts("stime", "print JVM start time, YYYY-mm-ddTMM:HH:SS");
    parser.accepts("long" , "print more columns");
    parser.accepts("help" , "prints help").forHelp();

    OptionSet opts;
    try { opts = parser.parse(args); }
    catch (OptionException e) {
      System.err.println(e.getMessage());
      return;
    }

    if (opts.has("help")) {
      try {
        parser.printHelpOn(System.err);
        System.err.println();
        System.err.println("unambiguous abbreviations of options are accepted.");
        System.err.println("list of hostname and labels are specified at stdin");
        System.err.println("format:");
        System.err.println("    hostname1 lable1");
        System.err.println("    hostname2:9999 lable2");
        System.err.println("    hostname3 lable3");
      }
      catch (IOException e) { e.printStackTrace(System.err); }
      return;
    }

    List<Map<String, String>> config =
        new BufferedReader(new InputStreamReader(System.in)).lines()
            .map(s -> s.trim().split("\\s+", 2))
            .map(a -> kv("label", a.length > 1 ? a[1].substring(0, 1).toUpperCase() : "-") // T U
                .kv("endpoint", a[0]).kv("host", a[0].replaceAll("\\..*", "")).toMap())
            .collect(Collectors.toList());

    if (opts.has("Topic"))
      doTopics(opts, config);
    else
      doYourThing(opts, config);
  }

  @SuppressWarnings("unchecked")
  static void doTopics(OptionSet opts, List<Map<String, String>> config){
    debug("opts: " + opts);
    AtomicBoolean printHeader = new AtomicBoolean();
    // aggByTopicPrefix_NoStream(OptionSet opts, Map<String, String> ctx, MBeanServerConnection con, AtomicBoolean printHeader)
    BiFunction<Map<String, String>, MBeanServerConnection, Map<String, Object>> fn = (ctx, con) -> {
      List<String> lines = aggByTopicPrefix_NoStream(opts, ctx, con, printHeader);
      Map<String, Object> hm = new HashMap<>();
      hm.put("lines", lines);
      return hm;
    };

    List<String> lines = config.parallelStream()
        .map(ctx -> connect(ctx, fn))
        .filter(Optional::isPresent)
        .map(Optional::get)
        .flatMap(hm -> ((List<String>) hm.get("lines")).stream())
        .collect(Collectors.toList());

    lines.sort(String.CASE_INSENSITIVE_ORDER);
    lines.forEach(line -> System.out.println(line));
  }

  static void doYourThing(OptionSet opts, List<Map<String, String>> config) {
    debug("opts: " + opts);

    Comparator<Object> scmp = (a, b) -> String.valueOf(a).compareTo(String.valueOf(b));
    Comparator<Map<String, Object>> cmp = (a, b) -> {
      int r = scmp.compare(a.get("host"), b.get("host"));
      if (r == 0) r = scmp.compare(a.get("label"), b.get("label"));
      if (r == 0) r = scmp.compare(a.get("endpoint"), b.get("endpoint"));
      return r;
    };

    BiFunction<Map<String, String>, MBeanServerConnection, Map<String, Object>> fn;
    if (opts.has("throughput"  )) fn = (ctx, con) -> throughput  (opts, ctx, con); else
    if (opts.has("performance" )) fn = (ctx, con) -> performance (opts, ctx, con); else
    if (opts.has("availability")) fn = (ctx, con) -> availability(opts, ctx, con);
    else                          fn = (ctx, con) -> jmxout      (opts, ctx, con);

//    List<Map<String, Object>> output = config.parallelStream()
//        .map(conf -> connect(conf)) // XXX: cant close connection
//        .filter(Optional::isPresent)
//        .map(f -> f.get().apply(fn))
//        .sorted(cmp)
//        .collect(Collectors.toList());

    Stream<Map<String, Object>> sorted = config.parallelStream()
        .map(ctx -> connect(ctx, fn)) // closes connection too
        .filter(Optional::isPresent)
        .map(Optional::get)
        .sorted(cmp);

    if (!sorted.isParallel()) throw new AssertionError("stream must be parallel");

    List<Map<String, Object>> output = sorted.collect(Collectors.toList()); // execute at "terminal" op

    if (!output.isEmpty()) {
      // Set<String> headers = output.stream().map(m -> m.keySet()).flatMap(keys -> keys.stream()).distinct().sorted().collect(Collectors.toSet());
      Set<String> headers = output.get(0).keySet(); // order of insertion
      System.out.println(headers.stream().collect(Collectors.joining(" ")));
      output.forEach(row -> System.out.println(
          headers.stream()
              .map(h -> row.get(h))
              .map(a -> (a instanceof Double || a instanceof Float) ? String.format("%.02f", a) : String.valueOf(a))
              .collect(Collectors.joining(" "))));
    }

  }

  static void howManyThreadsInAParallelStream(String[] args) {
    if (args.length > 0) {
      int n = Integer.parseInt(args[0]); assert n > 0;
      System.out.println("setting parallelism=" + n);
      // ForkJoinPool.commonPool is init once in static-init, we can't reset
      // number of threads in this pool after its created.
      System.setProperty("java.util.concurrent.ForkJoinPool.common.parallelism", String.valueOf(n));
    }
    System.out.println("how many threads in parallel stream:"
        + " ncpu=" + Runtime.getRuntime().availableProcessors()
        + " parallelism=" + System.getProperty("java.util.concurrent.ForkJoinPool.common.parallelism") + " "
        + IntStream.range(1, 1_000_000).parallel()
                                       .map(i -> Thread.currentThread().hashCode())
                                       .distinct()
                                       .count());
  }

  static Optional<Map<String, Object>> connect(
      Map<String, String> context,
      BiFunction<Map<String, String>, MBeanServerConnection, Map<String, Object>> fn) {
    String endpoint = context.get("endpoint");
    if (!endpoint.contains(":")) endpoint += ":" + System.getProperty("default.port", "9999").trim();
    String url = "service:jmx:rmi:///jndi/rmi://" + endpoint + "/jmxrmi";
    try (JMXConnector connector = JMXConnectorFactory.connect(new JMXServiceURL(url))) {
      return Optional.of(fn.apply(context, connector.getMBeanServerConnection()));
    } catch (Exception ex) {
      error("E_CONNECT", ex);
      ex.printStackTrace(System.err);
      return Optional.empty();
    }
  }

  static Optional<Function<BiFunction<Map<String, String>, MBeanServerConnection, Map<String, Object>>, Map<String, Object>>>
  connect(Map<String, String> context) {
    String endpoint = context.get("endpoint");
    if (!endpoint.contains(":")) endpoint += ":" + System.getProperty("default.port", "9999").trim();
    String url = "service:jmx:rmi:///jndi/rmi://" + endpoint + "/jmxrmi";
    try {
      JMXConnector connector = JMXConnectorFactory.connect(new JMXServiceURL(url)); // XXX when do we close this ?
      MBeanServerConnection serverConnection = connector.getMBeanServerConnection();
      return Optional.of(fn -> fn.apply(context, serverConnection));
    } catch (Exception e) {
      error("E_CONNECT", e);
      return Optional.empty();
    }
  }

  static MapBuilder<String, Object> outputBuilder(Map<String, String> ctx) {
    MapBuilder<String, Object> output = new MapBuilder<>();
    output.kv("C", ctx.get("label")).kv("host", ctx.get("host"));
    return output;
  }

  static Map<String, Object> jmxout(OptionSet opts, Map<String, String> ctx, MBeanServerConnection con) {
    debug("jmxout ctx=" + ctx + " opts=" + opts);
    boolean longOutputFormat = opts.has("long");
    MapBuilder<String, Object> output = outputBuilder(ctx);

    Function<String, Function<String, Long>> longAttr = jmxAttribute(-1L).apply(con);
    Function<String, Function<String, Double>> doubleAttr = jmxAttribute(-1D).apply(con);

    Function<String, Double> fnFiveMinuteRate = doubleAttr.apply("FiveMinuteRate");
    Function<String, Long> fnValue = longAttr.apply("Value");

    Object[][] kfk_metrics = {
        // {"kafka.server:type=BrokerTopicMetrics,name=BytesInPerSec" , "FiveMinuteRate", "BytesIn" },
        // {"kafka.server:type=BrokerTopicMetrics,name=BytesOutPerSec" , "FiveMinuteRate", "BytesOut" },

        {"kafka.server:name=BrokerState,type=KafkaServer"                             , fnValue, "S"      },
        {"kafka.server:type=BrokerTopicMetrics,name=MessagesInPerSec"                 , fnFiveMinuteRate, "MsgIn/s"},
        {"kafka.server:type=ReplicaManager,name=UnderReplicatedPartitions"            , fnValue, "UndrRp" },
        {"kafka.controller:type=KafkaController,name=OfflinePartitionsCount"          , fnValue, "Offln"  },
        {"kafka.controller:type=KafkaController,name=ActiveControllerCount"           , fnValue, "A"      },
        {"kafka.server:type=ReplicaManager,name=LeaderCount"                          , fnValue, "Ldrs"   },
        {"kafka.server:type=ReplicaManager,name=PartitionCount"                       , fnValue, "Prtns"  },
        {"kafka.network:type=SocketServer,name=NetworkProcessorAvgIdlePercent"        , fnValue, "NtIdle" },
        {"kafka.server:type=KafkaRequestHandlerPool,name=RequestHandlerAvgIdlePercent", fnFiveMinuteRate, "RqIdle" },
        {"java.lang:type=OperatingSystem"                                             , doubleAttr.apply("ProcessCpuLoad"), "JvmCpu" },
        {"java.lang:type=OperatingSystem"                                             , doubleAttr.apply("SystemCpuLoad" ), "SysCpu" },

        // Returns the system load average for the last minute. The system load
        // average is the sum of the number of runnable entities queued to the
        // available processors and the number of runnable entities running on the
        // available processors averaged over a period of time. The way in which the
        // load average is calculated is operating system specific but is typically
        // a damped time-dependent average.
        {"java.lang:type=OperatingSystem", doubleAttr.apply("SystemLoadAverage"), "SysLdAvg"}
    };

    double bytesin = fnFiveMinuteRate.apply("kafka.server:type=BrokerTopicMetrics,name=BytesInPerSec");
    double bytesout= fnFiveMinuteRate.apply("kafka.server:type=BrokerTopicMetrics,name=BytesOutPerSec");
    output.kv("In/s", M(bytesin)).kv("Out/s", M(bytesout)).kv("Out/In", (bytesin > 0 ? bytesout / bytesin : -1.0));

    Stream.of(kfk_metrics).forEach(m -> {
      @SuppressWarnings("unchecked")
      Function<String, Object> fn = (Function<String, Object>) m[1];
      output.kv((String) m[2], fn.apply((String) m[0]));
    });

    Optional<RuntimeMXBean> rtMXBean = getPlatformMXBean(RuntimeMXBean.class).apply(con);
    if (opts.has("stime")) output.kv("STime", rtMXBean.map(bean -> stime(bean)).orElse("-"));
    output.kv("UPHrs", rtMXBean.map(bean -> Duration.ofMillis(bean.getUptime()).toHours()).orElse(-1L));

    long openfd = longAttr.apply("OpenFileDescriptorCount").apply("java.lang:type=OperatingSystem");
    long maxfd  = longAttr.apply("MaxFileDescriptorCount" ).apply("java.lang:type=OperatingSystem");
    if (longOutputFormat) output.kv("FdOpn", openfd).kv("FdMx", maxfd);

    BiFunction<Long, Long, String> prcntg = (num, denom) -> String.format("%.02f", ((double) num) / denom);

    output.kv("%Fd", prcntg.apply(openfd, maxfd));

    Optional<MemoryUsage> heapUsage = getPlatformMXBean(MemoryMXBean.class).apply(con).map(bean -> bean.getHeapMemoryUsage());
    long maxheap = heapUsage.map(bean -> bean.getMax()).orElse(-1L);
    long usedheap = heapUsage.map(bean -> bean.getUsed()).orElse(-1L);
    if (longOutputFormat) output.kv("MemUsd", M(maxheap)).kv("MemMx", M(usedheap));
    output.kv("%Mem", prcntg.apply(usedheap, maxheap));

    return output.toMap();
  }

  static Map<String, Object> throughput(OptionSet opts, Map<String, String> ctx, MBeanServerConnection con) {
    MapBuilder<String, Object> output = outputBuilder(ctx);
    Function<String, Double> fiveMinuteRate = jmxAttribute(-1.0D).apply(con).apply("FiveMinuteRate");
    double bytesin = fiveMinuteRate.apply("kafka.server:type=BrokerTopicMetrics,name=BytesInPerSec");
    double bytesout = fiveMinuteRate.apply("kafka.server:type=BrokerTopicMetrics,name=BytesOutPerSec");
    output.kv("In/s", M(bytesin)).kv("Out/s", M(bytesout)).kv("Out/In", String.format("%.02f", bytesout / bytesout));
    output.kv("MsgIn/s", fiveMinuteRate.apply("kafka.server:type=BrokerTopicMetrics,name=MessagesInPerSec"));
    output.kv("PrdRq/s", fiveMinuteRate.apply("kafka.network:type=RequestMetrics,name=RequestsPerSec,request=Produce"));
    output.kv("CnsRq/s", fiveMinuteRate.apply("kafka.network:type=RequestMetrics,name=RequestsPerSec,request=FetchConsumer"));
    output.kv("FlwRq/s", fiveMinuteRate.apply("kafka.network:type=RequestMetrics,name=RequestsPerSec,request=FetchFollower"));
    return output.toMap();
  }

  static Map<String, Object> performance(OptionSet opts, Map<String, String> ctx, MBeanServerConnection con) {
    MapBuilder<String, Object> output = outputBuilder(ctx);
    Function<String, Double> percentile = jmxAttribute(-1.0D).apply(con).apply("99thPercentile");

    String[][] reqst = {
        {"Produce"      , "Prd"},
        {"FetchConsumer", "Cns"},
        {"FetchFollower", "Flw"}
    };
    String[][] mbeans = {
        {"kafka.network:type=RequestMetrics,name=LocalTimeMs,request=" , "locTmMs"},
        {"kafka.network:type=RequestMetrics,name=RemoteTimeMs,request=", "remTmMs"},
        {"kafka.network:type=RequestMetrics,name=TotalTimeMs,request=" , "totTmMs"},
    };
    if ("q".equals(String.valueOf(opts.valueOf("performance")).toLowerCase())) {
      mbeans = new String[][] {
          {"kafka.network:type=RequestMetrics,name=RequestQueueTimeMs,request=" , "reqQTmMs"  },
          {"kafka.network:type=RequestMetrics,name=ResponseQueueTimeMs,request=", "rspQTmMs"  },
          {"kafka.network:type=RequestMetrics,name=ResponseSendTimeMs,request=" , "rspSndTmMs"}
      };
    }

    for (String[] req : reqst) {
      for (String[] mbean : mbeans) {
        output.kv(req[1] + "-" + mbean[1], percentile.apply(mbean[0] + req[0]));
      }
    }

    return output.toMap();
  }

  static Map<String, Object> availability(OptionSet opts, Map<String, String> ctx, MBeanServerConnection con) {
    MapBuilder<String, Object> output = outputBuilder(ctx);
    Function<String, Long> value = jmxAttribute(-1L).apply(con).apply("Value");
    Function<String, Double> fiveMinuteRate = jmxAttribute(-1D).apply(con).apply("FiveMinuteRate");
    Object[][] attributes = new Object[][] {
      { "kafka.server:type=ReplicaManager,name=UnderReplicatedPartitions"            , value         , "UndRp"      },
      { "kafka.controller:type=KafkaController,name=OfflinePartitionsCount"          , value         , "OffLn"      },
      { "kafka.server:type=ReplicaManager,name=PartitionCount"                       , value         , "Prtns"      },
      { "kafka.server:type=ReplicaManager,name=LeaderCount"                          , value         , "Ldr"        },
      { "kafka.controller:type=KafkaController,name=ActiveControllerCount"           , value         , "A"          },
      { "kafka.server:type=ReplicaManager,name=IsrExpandsPerSec"                     , fiveMinuteRate, "IsrX/s"     },
      { "kafka.server:type=ReplicaManager,name=IsrShrinksPerSec"                     , fiveMinuteRate, "IsrR/s"     },
      { "kafka.controller:type=ControllerStats,name=LeaderElectionRateAndTimeMs"     , fiveMinuteRate, "LE/s"       },
      { "kafka.controller:type=ControllerStats,name=UncleanLeaderElectionsPerSec"    , fiveMinuteRate, "ULE/s"      },
      { "kafka.network:type=SocketServer,name=NetworkProcessorAvgIdlePercent"        , value         , "NtIdle"     },
      { "kafka.server:type=KafkaRequestHandlerPool,name=RequestHandlerAvgIdlePercent", fiveMinuteRate, "RqIdle"     },
      { "kafka.log:type=LogFlushStats,name=LogFlushRateAndTimeMs"                    , fiveMinuteRate, "LogFlush/s" }
    };

    for (Object[] m : attributes) {
      @SuppressWarnings("unchecked")
      Function<String, Object> fn = (Function<String, Object>) m[1];
      output.kv((String) m[2], fn.apply((String) m[0]));
    }

    return output.toMap();
  }

  static Map<String, Object> aggByTopicPrefix(OptionSet opts, Map<String, String> ctx, MBeanServerConnection con) {
    Map<String, Object> output = new HashMap<>();
    //int groupByTopics = 2; // 1 2
    int groupByTopics = 1; // 1 2

      // this is the order of headers
    List<String> metrics = Arrays.asList(
        "BytesInPerSec",
        "BytesOutPerSec",
        "BytesRejectedPerSec",
        "MessagesInPerSec",
        "TotalFetchRequestsPerSec",
        "TotalProduceRequestsPerSec",
        "FailedFetchRequestsPerSec",
        "FailedProduceRequestsPerSec"
        );
      // translate table for `metrics` list above
    List<String> metricsLabels = Arrays.asList(
        "In/s",
        "Out/s",
        "Rej/s",
        "MsgsIn/s",
        "TotFetchReq/s",
        "TotPrdReq/s",
        "FailedFetchReq/s",
        "FailedPrdReq/s"
        );

    Function<String, String> labelFor = metric -> {
      for (int i = 0; i < metrics.size(); i++) {
        if (metric.equals(metrics.get(i))) {
          return metricsLabels.get(i);
        }
      }
      return metric;
    };

    Predicate<ObjectName> filter = bean -> bean.getKeyProperty("topic") != null && metrics.contains(bean.getKeyProperty("name"));

    Function<ObjectName, String> topicNamePrefixAndMetric =
        bean -> (groupByTopics == 1 ? bean.getKeyProperty("topic").replaceAll("(\\w)\\d{2}(\\d*)", "$1xx$2") // Exx10 Exx11 etc
            : bean.getKeyProperty("topic").substring(0, 1)) // only first char, E T C
            + " " + bean.getKeyProperty("name");

    Function<String, Double> fiveMinuteRate = jmxAttribute(0.0).apply(con).apply("FiveMinuteRate");
    try {
      Set<ObjectName> mbeans = con.queryNames(new ObjectName("kafka.server:*"), null);
      Stream<ObjectName> filtered = mbeans.parallelStream().filter(filter);
      Map<String, List<ObjectName>> byTopicAndMetric = filtered.collect(Collectors.groupingBy(topicNamePrefixAndMetric));
      Stream<Entry<String, List<ObjectName>>> byTopicAndMetricStream = byTopicAndMetric.entrySet().parallelStream();
      Map<String, List<Map<String, Object>>> byTopic =
          byTopicAndMetricStream.map(entry -> {
            List<ObjectName> mbeansByTopic = entry.getValue();
            int numOfTopics = mbeansByTopic.size();
            List<String> metricName = mbeansByTopic.parallelStream().map(bean -> bean.getKeyProperty("name")).distinct() .collect(Collectors.toList());
            if (metricName.size() != 1) throw new AssertionError(metricName);
            double agg = mbeansByTopic.parallelStream().mapToDouble(bean -> fiveMinuteRate.apply(bean.toString())).sum();
            String key = entry.getKey();
            String[] parts = key.split("\\s+");
            String topic = parts[0], metric = parts[1];
            if (!metricName.get(0).equals(metric)) throw new AssertionError(metric + "!=" + metricName);
            return outputBuilder(ctx).kv("topic", topic).kv("ntopics", numOfTopics).kv("metric", metric).kv("value", agg) .toMap();
          }).collect(Collectors.groupingBy(m -> (String) m.get("topic")));

      List<Map<String, Object>> result = byTopic.entrySet().stream().map(entry -> {
        String topic = entry.getKey();
        List<Map<String, Object>> listoftm = entry.getValue();
        Map<String, Object> hm = outputBuilder(ctx).toMap();
        String topic2 = null;
        int numOfTopics = -1;
        for (String metric : metrics) {
          for (Map<String, Object> tm : listoftm) {
            String m = (String) tm.get("metric");
            if (m.equals(metric)) {
              int n = ((Number) tm.get("ntopics")).intValue();;
              String t = (String) tm.get("topic");
              double v = ((Number) tm.get("value")).doubleValue();
              if (topic2 == null) { topic2 = t; hm.put("topic", t); }
              if (!topic2.equals(t)) throw new AssertionError(topic2 + "!=" + t);
              if (numOfTopics == -1) { numOfTopics = n; hm.put("num", n); }
              if (numOfTopics != n) throw new AssertionError(numOfTopics + "!=" + n);
              Object old = hm.put(labelFor.apply(m), v);
              if (old != null) throw new AssertionError("duplicate " + m);
              break;
            }
          }
        }
        if (!topic.equals(topic2)) throw new AssertionError(topic2 + "!=" + topic);
        return hm;
      }).collect(Collectors.toList());

      output.put("result", result);
    } catch (Exception e) {
      error("E_QUERYNAMES", e);
    }

    return output;
  }

  static List<String> aggByTopicPrefix_NoStream(
      OptionSet opts,
      Map<String, String> ctx,
      MBeanServerConnection con,
      AtomicBoolean printHeader)
  {
    debug("aggByTopicPrefix_NoStream ctx=" + ctx + " printHeader=" + printHeader);
    String[][] metricsArray = {{"BytesInPerSec"              , "In/s"            },
                               {"BytesOutPerSec"             , "Out/s"           },
                               {"BytesRejectedPerSec"        , "Rej/s"           },
                               {"MessagesInPerSec"           , "MsgsIn/s"        },
                               {"TotalFetchRequestsPerSec"   , "TotFetchReq/s"   },
                               {"TotalProduceRequestsPerSec" , "TotPrdReq/s"     },
                               {"FailedFetchRequestsPerSec"  , "FailedFetchReq/s"},
                               {"FailedProduceRequestsPerSec", "FailedPrdReq/s"  }};

    Map<String, String> metricsToLabel = Stream.of(metricsArray).collect(Collectors.toMap(a -> a[0], a -> a[1]));
    Set<String> metricsKeySet = metricsToLabel.keySet();
    String[] headers = new String[4 + metricsArray.length];
    headers[0] = "C";
    headers[1] = "host";
    headers[2] = "topic";
    headers[3] = "ntopics";
    for (int i = 0; i < metricsArray.length; i++) headers[i+4] = metricsArray[i][1];
    if (printHeader.compareAndSet(false, true))
      System.out.println(Stream.of(headers).collect(Collectors.joining(" ")));

    Function<String, String> fnTopicNamePrefix = topicName -> "2".equals(opts.valueOf("Topic"))
        ? topicName.replaceAll("(\\w)\\d{2}(\\d*)", "$1xx$2")   // TxxNN
        : topicName.substring(0, 1);                            // T

    Function<String, Double> fiveMinuteRate = jmxAttribute(0.0).apply(con).apply("FiveMinuteRate");

    try {
      Map<String, List<ObjectName>> groupByTopicAndMetrics = groupBy(
          con.queryNames(new ObjectName("kafka.server:*"), null),
          oname -> {
            String metricName = oname.getKeyProperty("name"), topicName = oname.getKeyProperty("topic");
            return (topicName != null && metricsKeySet.contains(metricName))
                ? fnTopicNamePrefix.apply(topicName) + " " + metricName
                : null;
          });

      Map<String, Map<String, Object>> records = new LinkedHashMap<>();
      for (Map.Entry<String, List<ObjectName>> entry : groupByTopicAndMetrics.entrySet()) {
        String[] a = entry.getKey().split("\\s+"); // topic prefix and metric
        String topicNamePrefix = a[0], metricName = a[1];
        List<ObjectName> counters = entry.getValue();
        double aggregate = counters.parallelStream().mapToDouble(b -> fiveMinuteRate.apply(b.toString())).sum();
        counters.size();
        Map<String, Object> rec = records.get(topicNamePrefix);
        if (rec == null) {
          rec = outputBuilder(ctx).toMap();
          records.put(topicNamePrefix, rec);
        }

        Object old = rec.get("topic");
        if (old == null)
          rec.put("topic", topicNamePrefix);
        else
          assert ((String) old).equals(topicNamePrefix) : topicNamePrefix + "!=" + old;

        old = rec.get("ntopics");
        if (old == null)
          rec.put("ntopics", counters.size());
        else // each group by topicNamePrefix must have same number of topics
          assert ((Integer) old).intValue() == counters.size();

        String label = metricsToLabel.get(metricName);
        old = rec.put(label, aggregate);
        assert old == null : "duplicate " + label;
      }

      ArrayList<String> lines = new ArrayList<>();
      for (Map<String, Object> rec : records.values()) {
        StringJoiner line = new StringJoiner(" ");
        for (int i = 0; i < headers.length; i++) {
          Object v = rec.get(headers[i]);
          line.add((v instanceof Float || v instanceof Double) ? String.format("%.02f", v) : String.valueOf(v));
        }
        lines.add(line.toString());
      }
      return lines;
    } catch (Exception e) {
      error("E_QUERYNAMES_2", e);
      e.printStackTrace(System.err);
    }
   return Collections.emptyList();
  }

  static <T> Map<String, List<T>> groupBy(Collection<T> collection, Function<T, String> fnGroupKey) {
    Map<String, List<T>> groups = new HashMap<>();
    for (T item : collection) {
      String key = fnGroupKey.apply(item);
      if (key == null) continue; // filter
      List<T> list = groups.get(key);
      if (list == null) {
        list = new ArrayList<>();
        groups.put(key, list);
      }
      list.add(item);
    }
    return groups;
  }

  static String stime(RuntimeMXBean mbean) {
    return Instant.ofEpochMilli(mbean.getStartTime())
        .atZone(ZoneId.systemDefault())
        .toLocalDateTime()
        .toString()
        .replaceAll("\\..*", ""); // // 2018-11-08T12:23:57.424 to 2018-11-08T12:23:57
  }

  @SuppressWarnings("unchecked")
  static <T> Function<MBeanServerConnection, Function<String, Function<String, T>>> jmxAttribute(T defaultval) {
    return con -> attribute -> name -> {
      try {
        return (T) con.getAttribute(new ObjectName(name), attribute);
      } catch (Exception e) {
        error("E_ATTRIBUTE", e);
        return defaultval;
      }
    };
  }

  static <T extends PlatformManagedObject> Function<MBeanServerConnection, Optional<T>>
  getPlatformMXBean(Class<T> mxbeanInterface) {
    return con -> {
      try {
        return Optional.of(ManagementFactory.getPlatformMXBean(con, mxbeanInterface));
      } catch (IOException e) {
        error("E_PLATFORMMXBEAN", e);
        return Optional.empty();
      }
    };
  }

  static String M(double nbytes) { return String.format("%.02fM", nbytes / 1024 / 1024); }
  static String M(long   nbytes) { return (nbytes / 1024 / 1024) + "M"; }

  static <K, V> MapBuilder<K, V> kv(K k, V v) { return new MapBuilder<K, V>().kv(k, v); }
  static class MapBuilder<K, V> {
    LinkedHashMap<K, V> hm = new LinkedHashMap<>();
    MapBuilder<K, V> kv(K k, V v) { hm.put(k, v); return this; }
    Map<K, V> toMap() {return hm;}
  }

  static void error(String msg, Throwable e) { System.err.println("[ERROR] " + msg + ": " + rootcause(e).getMessage()); }
  static final boolean debug = System.getProperty("verbose", "false").trim().toLowerCase().equals("true");
  static void debug(String msg) { if (debug) System.err.println("[DEBUG] |" + Thread.currentThread() + "| " + msg); }

  static Throwable rootcause(Throwable throwable) {
    Throwable cause;
    while ((cause = throwable.getCause()) != null) {
      throwable = cause;
    }
    return throwable;
  }
}
