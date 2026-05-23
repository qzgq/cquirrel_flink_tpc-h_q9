import org.apache.flink.api.common.functions.FlatMapFunction;
import org.apache.flink.api.common.state.ValueState;
import org.apache.flink.api.common.state.ValueStateDescriptor;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.KeyedProcessFunction;
import org.apache.flink.streaming.api.functions.sink.RichSinkFunction;
import org.apache.flink.util.Collector;

import java.io.BufferedWriter;
import java.io.FileOutputStream;
import java.io.OutputStreamWriter;
import java.io.Serializable;
import java.math.BigDecimal;
import java.math.RoundingMode;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.time.LocalDate;
import java.util.*;

public class CquirrelQ9Flink {
    /**
     * 本机模拟分布式的 bucket 数。
     * 可以改成 2、4、8 等。
     * env.setParallelism(NUM_BUCKETS) 会让本机启动多个并行 subtask 模拟分布式。
     */
    private static final int NUM_BUCKETS = 4;

    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        // 本机模拟分布式并行度。
        env.setParallelism(NUM_BUCKETS);
        // 如要测试 checkpoint，可打开。
        // env.enableCheckpointing(5000);
        String inputPath = args.length > 0
                ? args[0]
                : "input_data_all.csv";
        String outputPath = args.length > 1
                ? args[1]
                : "q9_flink_delta.csv";
        System.out.println("Input path: " + inputPath);
        System.out.println("Output path: " + outputPath);
        System.out.println("本机模拟分布式 bucket 数: " + NUM_BUCKETS);
        DataStream<String> source = env
                .readTextFile(inputPath)
                .name("Ordered File Source")
                .setParallelism(1);
        DataStream<PartitionedEvent> partitionedEvents = source
                .flatMap(new Q9InputRouter(NUM_BUCKETS))
                .name("Q9 Parser and Bucket Router")
                .setParallelism(1);
        DataStream<ProfitDelta> profitDeltas = partitionedEvents
                .keyBy(e -> e.bucket)
                .process(new Q9BucketJoinProcessFunction())
                .name("Q9 Bucket Join Delta Process")
                .setParallelism(NUM_BUCKETS);
        DataStream<String> resultDeltas = profitDeltas
                .keyBy(d -> d.nation + "|" + d.year)
                .process(new Q9AggregationProcessFunction())
                .name("Q9 Distributed Aggregation");
        resultDeltas
                .addSink(new CsvWithHeaderSink(outputPath))
                .name("Q9 CSV Sink With Header")
                .setParallelism(1);
        env.execute("Cquirrel Q9 Distributed Local Simulation");
    }

    // Input Parser and Router
    public static class CsvWithHeaderSink extends RichSinkFunction<String> {

        private final String outputPath;
        private transient BufferedWriter writer;

        public CsvWithHeaderSink(String outputPath) {
            this.outputPath = outputPath;
        }

        @Override
        public void open(Configuration parameters) throws Exception {
            Path path = Paths.get(outputPath);

            Path parent = path.getParent();
            if (parent != null && !Files.exists(parent)) {
                Files.createDirectories(parent);
            }
            writer = new BufferedWriter(
                    new OutputStreamWriter(
                            new FileOutputStream(outputPath, false),
                            StandardCharsets.UTF_8
                    )
            );
            writer.write("delta_sign,nation,o_year,profit_delta");
            writer.newLine();
            writer.flush();
        }

        @Override
        public void invoke(String value, Context context) throws Exception {
            writer.write(value);
            writer.newLine();
        }

        @Override
        public void close() throws Exception {
            if (writer != null) {
                writer.flush();
                writer.close();
            }
        }
    }

    public static class Q9InputRouter implements FlatMapFunction<String, PartitionedEvent> {
        private final int buckets;
        private long parsed = 0L;
        private long skipped = 0L;

        public Q9InputRouter(int buckets) {
            this.buckets = buckets;
        }

        @Override
        public void flatMap(String line, Collector<PartitionedEvent> out) {
            if (line == null) return;

            line = line.trim();
            if (line.length() < 4) return;

            String prefix = line.substring(0, 3);

            if (!(prefix.equals("+NA") || prefix.equals("-NA") ||
                    prefix.equals("+SU") || prefix.equals("-SU") ||
                    prefix.equals("+OR") || prefix.equals("-OR") ||
                    prefix.equals("+PA") || prefix.equals("-PA") ||
                    prefix.equals("+PS") || prefix.equals("-PS") ||
                    prefix.equals("+LI") || prefix.equals("-LI"))) {
                skipped++;
                if (skipped <= 20) {
                    System.err.println("!未识别前缀，跳过: " + line);
                }
                return;
            }

            String[] f = parsePayload(line);

            UpdateEvent ev;

            try {
                switch (prefix) {
                    case "+NA":
                    case "-NA":
                        if (!checkFieldCount(prefix, f, 2, line)) return;
                        ev = UpdateEvent.nation(
                                prefix.charAt(0),
                                f[0].trim(),
                                f[1].trim()
                        );
                        fanout(ev, out);
                        break;

                    case "+SU":
                    case "-SU":
                        /*
                         * TPC-H supplier:
                         * f[0] = s_suppkey
                         * f[3] = s_nationkey
                         */
                        if (!checkFieldCount(prefix, f, 4, line)) return;
                        ev = UpdateEvent.supplier(
                                prefix.charAt(0),
                                f[0].trim(),
                                f[3].trim()
                        );
                        fanout(ev, out);
                        break;

                    case "+OR":
                    case "-OR":
                        /*
                         * TPC-H orders:
                         * f[0] = o_orderkey
                         * f[4] = o_orderdate
                         */
                        if (!checkFieldCount(prefix, f, 5, line)) return;
                        ev = UpdateEvent.order(
                                prefix.charAt(0),
                                f[0].trim(),
                                f[4].trim()
                        );
                        fanout(ev, out);
                        break;

                    case "+PA":
                    case "-PA":
                        /*
                         * TPC-H part:
                         * f[0] = p_partkey
                         * f[1] = p_name
                         */
                        if (!checkFieldCount(prefix, f, 2, line)) return;
                        ev = UpdateEvent.part(
                                prefix.charAt(0),
                                f[0].trim(),
                                f[1].trim()
                        );
                        fanout(ev, out);
                        break;

                    case "+PS":
                    case "-PS":
                        /*
                         * TPC-H partsupp:
                         * f[0] = ps_partkey
                         * f[1] = ps_suppkey
                         * f[3] = ps_supplycost
                         */
                        if (!checkFieldCount(prefix, f, 4, line)) return;
                        ev = UpdateEvent.partsupp(
                                prefix.charAt(0),
                                f[0].trim(),
                                f[1].trim(),
                                f[3].trim()
                        );
                        fanout(ev, out);
                        break;

                    case "+LI":
                    case "-LI":
                        /*
                         * TPC-H lineitem:
                         * f[0] = l_orderkey
                         * f[1] = l_partkey
                         * f[2] = l_suppkey
                         * f[3] = l_linenumber
                         * f[4] = l_quantity
                         * f[5] = l_extendedprice
                         * f[6] = l_discount
                         */
                        if (!checkFieldCount(prefix, f, 7, line)) return;

                        String liKey = buildLineItemKey(
                                f[0].trim(),
                                f[1].trim(),
                                f[2].trim(),
                                f[3].trim()
                        );

                        ev = UpdateEvent.lineitem(
                                prefix.charAt(0),
                                liKey,
                                f[0].trim(),
                                f[1].trim(),
                                f[2].trim(),
                                f[3].trim(),
                                f[4].trim(),
                                f[5].trim(),
                                f[6].trim()
                        );

                        int bucket = bucketOf(liKey, buckets);
                        out.collect(new PartitionedEvent(bucket, ev));
                        break;

                    default:
                        return;
                }

                parsed++;
                if (parsed <= 10) {
                    System.out.println("parsed event #" + parsed
                            + " prefix=" + prefix
                            + " fields=" + Arrays.toString(f));
                }

                if (parsed % 100000 == 0) {
                    System.out.println("Router parsed records=" + parsed);
                }

            } catch (Exception ex) {
                skipped++;
                System.err.println("解析失败，跳过行: " + line);
                ex.printStackTrace();
            }
        }

        private String[] parsePayload(String line) {
            String payload = line.substring(3).trim();
            if (payload.startsWith("|") || payload.startsWith(",")) {
                payload = payload.substring(1);
            }
            if (payload.contains("|")) {
                return payload.split("\\|", -1);
            } else if (payload.contains(",")) {
                return payload.split(",", -1);
            } else {
                return new String[]{payload};
            }
        }

        private boolean checkFieldCount(String prefix, String[] f, int expectedAtLeast, String line) {
            if (f.length < expectedAtLeast) {
                skipped++;
                if (skipped <= 50) {
                    System.err.println("!字段数不足，跳过。prefix="
                            + prefix
                            + ", expectedAtLeast=" + expectedAtLeast
                            + ", actual=" + f.length
                            + ", fields=" + Arrays.toString(f)
                            + ", line=" + line);
                }
                return false;
            }
            if (f[0] == null || f[0].trim().isEmpty()) {
                skipped++;
                if (skipped <= 50) {
                    System.err.println("!主键字段为空，跳过。prefix="
                            + prefix
                            + ", fields=" + Arrays.toString(f)
                            + ", line=" + line);
                }
                return false;
            }

            return true;
        }

        private void fanout(UpdateEvent ev, Collector<PartitionedEvent> out) {
            for (int b = 0; b < buckets; b++) {
                out.collect(new PartitionedEvent(b, ev));
            }
        }
    }

    // Bucket-level Join Process
    public static class Q9BucketJoinProcessFunction
            extends KeyedProcessFunction<Integer, PartitionedEvent, ProfitDelta> {

        private ValueState<BucketState> state;
        private long processed = 0L;

        @Override
        public void open(Configuration parameters) {
            state = getRuntimeContext().getState(
                    new ValueStateDescriptor<>("q9-bucket-state", BucketState.class)
            );
        }

        @Override
        public void processElement(
                PartitionedEvent pe,
                Context ctx,
                Collector<ProfitDelta> out
        ) throws Exception {

            processed++;
            if (processed % 100000 == 0) {
                System.out.println("🔄 bucket=" + ctx.getCurrentKey()
                        + " processed=" + processed
                        + " time=" + new Date());
            }

            BucketState app = state.value();
            if (app == null) {
                app = new BucketState();
            }

            UpdateEvent ev = pe.event;

            switch (ev.table) {
                case "NA":
                    handleNation(app, ev, out);
                    break;

                case "SU":
                    handleSupplier(app, ev, out);
                    break;

                case "OR":
                    handleOrder(app, ev, out);
                    break;

                case "PA":
                    handlePart(app, ev, out);
                    break;

                case "PS":
                    handlePartSupp(app, ev, out);
                    break;

                case "LI":
                    handleLineItem(app, ev, out);
                    break;

                default:
                    break;
            }

            state.update(app);
        }

        private void handleNation(
                BucketState app,
                UpdateEvent ev,
                Collector<ProfitDelta> out
        ) {
            String nationKey = ev.nationKey;

            Set<String> affectedLi = collectLineItemsByNationKey(app, nationKey);

            if (ev.op == '+') {
                app.nations.put(nationKey, new Nation(nationKey, ev.nationName));
            } else {
                app.nations.remove(nationKey);
            }

            reevaluateAll(app, affectedLi, out);
        }

        private void handleSupplier(
                BucketState app,
                UpdateEvent ev,
                Collector<ProfitDelta> out
        ) {
            String suppKey = ev.suppKey;

            Set<String> affectedLi = new HashSet<>();
            addAllFromIndex(affectedLi, app.liBySupp, suppKey);

            if (ev.op == '+') {
                app.suppliers.put(suppKey, new Supplier(suppKey, ev.nationKey));
            } else {
                app.suppliers.remove(suppKey);
            }

            reevaluateAll(app, affectedLi, out);
        }

        private void handleOrder(
                BucketState app,
                UpdateEvent ev,
                Collector<ProfitDelta> out
        ) {
            String orderKey = ev.orderKey;

            Set<String> affectedLi = new HashSet<>();
            addAllFromIndex(affectedLi, app.liByOrder, orderKey);

            if (ev.op == '+') {
                app.orders.put(orderKey, new Order(orderKey, ev.orderDate));
            } else {
                app.orders.remove(orderKey);
            }

            reevaluateAll(app, affectedLi, out);
        }

        private void handlePart(
                BucketState app,
                UpdateEvent ev,
                Collector<ProfitDelta> out
        ) {
            String partKey = ev.partKey;

            Set<String> affectedLi = new HashSet<>();
            addAllFromIndex(affectedLi, app.liByPart, partKey);

            if (ev.op == '+') {
                app.parts.put(partKey, new Part(partKey, ev.partName));
            } else {
                app.parts.remove(partKey);
            }

            reevaluateAll(app, affectedLi, out);
        }

        private void handlePartSupp(
                BucketState app,
                UpdateEvent ev,
                Collector<ProfitDelta> out
        ) {
            String psKey = buildPartSuppKey(ev.partKey, ev.suppKey);

            Set<String> affectedLi = new HashSet<>();
            addAllFromIndex(affectedLi, app.liByPartSupp, psKey);

            if (ev.op == '+') {
                app.partsupps.put(
                        psKey,
                        new PartSupp(psKey, ev.partKey, ev.suppKey, ev.supplyCost)
                );
            } else {
                app.partsupps.remove(psKey);
            }

            reevaluateAll(app, affectedLi, out);
        }

        private void handleLineItem(
                BucketState app,
                UpdateEvent ev,
                Collector<ProfitDelta> out
        ) {
            String liKey = ev.liKey;

            if (ev.op == '+') {
                LineItem li = new LineItem(
                        ev.liKey,
                        ev.orderKey,
                        ev.partKey,
                        ev.suppKey,
                        ev.lineNumber,
                        ev.quantity,
                        ev.extPrice,
                        ev.discount
                );

                app.lineitems.put(liKey, li);

                addIndex(app.liByOrder, li.orderKey, liKey);
                addIndex(app.liBySupp, li.suppKey, liKey);
                addIndex(app.liByPart, li.partKey, liKey);
                addIndex(app.liByPartSupp, buildPartSuppKey(li.partKey, li.suppKey), liKey);

                reevaluateOne(app, liKey, out);

            } else {
                ActiveRecord old = app.activeLineitems.remove(liKey);
                if (old != null) {
                    out.collect(ProfitDelta.negative(old.nation, old.year, old.amount));
                }
                LineItem li = app.lineitems.remove(liKey);
                if (li != null) {
                    removeIndex(app.liByOrder, li.orderKey, liKey);
                    removeIndex(app.liBySupp, li.suppKey, liKey);
                    removeIndex(app.liByPart, li.partKey, liKey);
                    removeIndex(app.liByPartSupp, buildPartSuppKey(li.partKey, li.suppKey), liKey);
                }
            }
        }

        /**
         * 重新判断一个 lineitem是否alive，并输出它相对于旧active状态的delta。
         */
        private void reevaluateOne(
                BucketState app,
                String liKey,
                Collector<ProfitDelta> out
        ) {
            ActiveRecord oldActive = app.activeLineitems.get(liKey);
            ActiveRecord newActive = buildActiveRecordIfValid(app, liKey);

            if (oldActive == null && newActive == null) {
                return;
            }

            if (oldActive == null) {
                app.activeLineitems.put(liKey, newActive);
                out.collect(ProfitDelta.positive(newActive.nation, newActive.year, newActive.amount));
                return;
            }

            if (newActive == null) {
                app.activeLineitems.remove(liKey);
                out.collect(ProfitDelta.negative(oldActive.nation, oldActive.year, oldActive.amount));
                return;
            }

            if (!sameActive(oldActive, newActive)) {
                out.collect(ProfitDelta.negative(oldActive.nation, oldActive.year, oldActive.amount));
                out.collect(ProfitDelta.positive(newActive.nation, newActive.year, newActive.amount));
                app.activeLineitems.put(liKey, newActive);
            }
        }

        private void reevaluateAll(
                BucketState app,
                Set<String> liKeys,
                Collector<ProfitDelta> out
        ) {
            for (String liKey : liKeys) {
                reevaluateOne(app, liKey, out);
            }
        }

        /**
         * Q9 Join 条件检查：
         * lineitem 必须同时 join 到：
         * orders(orderkey)
         * supplier(suppkey)
         * nation(supplier.nationkey)
         * part(partkey) 且 p_name contains green
         * partsupp(partkey, suppkey)
         * 如果全部满足，返回该 lineitem 对 group 的贡献：
         * nation = n_name
         * year = extract(year from o_orderdate)
         * amount = l_extendedprice * (1 - l_discount) - ps_supplycost * l_quantity
         */
        private ActiveRecord buildActiveRecordIfValid(BucketState app, String liKey) {
            LineItem li = app.lineitems.get(liKey);
            if (li == null) return null;

            Order order = app.orders.get(li.orderKey);
            if (order == null) return null;

            String year = getYear(order.orderDate);
            if (year == null) return null;

            Supplier supplier = app.suppliers.get(li.suppKey);
            if (supplier == null) return null;

            Nation nation = app.nations.get(supplier.nationKey);
            if (nation == null) return null;

            Part part = app.parts.get(li.partKey);
            if (part == null) return null;

            if (part.name == null || !part.name.contains("green")) {
                return null;
            }

            PartSupp ps = app.partsupps.get(buildPartSuppKey(li.partKey, li.suppKey));
            if (ps == null) return null;

            BigDecimal ep = new BigDecimal(li.extPrice);
            BigDecimal disc = new BigDecimal(li.discount);
            BigDecimal qty = new BigDecimal(li.quantity);
            BigDecimal supplyCost = new BigDecimal(ps.supplyCost);

            BigDecimal amount = ep.multiply(BigDecimal.ONE.subtract(disc))
                    .subtract(supplyCost.multiply(qty));

            return new ActiveRecord(liKey, nation.name, year, amount);
        }

        private Set<String> collectLineItemsByNationKey(BucketState app, String nationKey) {
            Set<String> result = new HashSet<>();

            for (Supplier s : app.suppliers.values()) {
                if (nationKey.equals(s.nationKey)) {
                    addAllFromIndex(result, app.liBySupp, s.suppKey);
                }
            }

            return result;
        }

        private void addAllFromIndex(
                Set<String> target,
                Map<String, List<String>> index,
                String key
        ) {
            List<String> list = index.get(key);
            if (list != null) {
                target.addAll(list);
            }
        }

        private void addIndex(Map<String, List<String>> index, String key, String liKey) {
            index.computeIfAbsent(key, k -> new ArrayList<>()).add(liKey);
        }

        private void removeIndex(Map<String, List<String>> index, String key, String liKey) {
            List<String> list = index.get(key);
            if (list != null) {
                list.remove(liKey);
                if (list.isEmpty()) {
                    index.remove(key);
                }
            }
        }

        private boolean sameActive(ActiveRecord a, ActiveRecord b) {
            if (a == b) return true;
            if (a == null || b == null) return false;

            return Objects.equals(a.nation, b.nation)
                    && Objects.equals(a.year, b.year)
                    && a.amount.compareTo(b.amount) == 0;
        }
    }

    // Aggregation Process
    public static class Q9AggregationProcessFunction
            extends KeyedProcessFunction<String, ProfitDelta, String> {

        private ValueState<BigDecimal> sumState;

        @Override
        public void open(Configuration parameters) {
            sumState = getRuntimeContext().getState(
                    new ValueStateDescriptor<>("q9-sum-profit", BigDecimal.class)
            );
        }

        @Override
        public void processElement(
                ProfitDelta delta,
                Context ctx,
                Collector<String> out
        ) throws Exception {

            BigDecimal oldSum = sumState.value();
            if (oldSum == null) oldSum = BigDecimal.ZERO;

            BigDecimal signedDelta = delta.amount.multiply(BigDecimal.valueOf(delta.sign));
            BigDecimal newSum = oldSum.add(signedDelta);

            sumState.update(newSum);

            String deltaSign = signedDelta.compareTo(BigDecimal.ZERO) >= 0 ? "+" : "-";

            out.collect(deltaSign
                    + "," + delta.nation
                    + "," + delta.year
                    + "," + signedDelta.abs().setScale(2, RoundingMode.HALF_UP));
        }
    }

    // Data Classes
    public static class PartitionedEvent implements Serializable {
        public int bucket;
        public UpdateEvent event;

        public PartitionedEvent() {
        }

        public PartitionedEvent(int bucket, UpdateEvent event) {
            this.bucket = bucket;
            this.event = event;
        }
    }

    public static class UpdateEvent implements Serializable {
        public char op;       // '+' or '-'
        public String table;  // NA, SU, OR, PA, PS, LI

        public String nationKey;
        public String nationName;

        public String suppKey;

        public String orderKey;
        public String orderDate;

        public String partKey;
        public String partName;

        public String supplyCost;

        public String liKey;
        public String lineNumber;
        public String quantity;
        public String extPrice;
        public String discount;

        public UpdateEvent() {
        }

        public static UpdateEvent nation(char op, String nationKey, String nationName) {
            UpdateEvent e = new UpdateEvent();
            e.op = op;
            e.table = "NA";
            e.nationKey = nationKey;
            e.nationName = nationName;
            return e;
        }

        public static UpdateEvent supplier(char op, String suppKey, String nationKey) {
            UpdateEvent e = new UpdateEvent();
            e.op = op;
            e.table = "SU";
            e.suppKey = suppKey;
            e.nationKey = nationKey;
            return e;
        }

        public static UpdateEvent order(char op, String orderKey, String orderDate) {
            UpdateEvent e = new UpdateEvent();
            e.op = op;
            e.table = "OR";
            e.orderKey = orderKey;
            e.orderDate = orderDate;
            return e;
        }

        public static UpdateEvent part(char op, String partKey, String partName) {
            UpdateEvent e = new UpdateEvent();
            e.op = op;
            e.table = "PA";
            e.partKey = partKey;
            e.partName = partName;
            return e;
        }

        public static UpdateEvent partsupp(
                char op,
                String partKey,
                String suppKey,
                String supplyCost
        ) {
            UpdateEvent e = new UpdateEvent();
            e.op = op;
            e.table = "PS";
            e.partKey = partKey;
            e.suppKey = suppKey;
            e.supplyCost = supplyCost;
            return e;
        }

        public static UpdateEvent lineitem(
                char op,
                String liKey,
                String orderKey,
                String partKey,
                String suppKey,
                String lineNumber,
                String quantity,
                String extPrice,
                String discount
        ) {
            UpdateEvent e = new UpdateEvent();
            e.op = op;
            e.table = "LI";
            e.liKey = liKey;
            e.orderKey = orderKey;
            e.partKey = partKey;
            e.suppKey = suppKey;
            e.lineNumber = lineNumber;
            e.quantity = quantity;
            e.extPrice = extPrice;
            e.discount = discount;
            return e;
        }
    }

    public static class ProfitDelta implements Serializable {
        public String nation;
        public String year;
        public BigDecimal amount;
        public int sign; // +1 or -1

        public ProfitDelta() {
        }

        public ProfitDelta(String nation, String year, BigDecimal amount, int sign) {
            this.nation = nation;
            this.year = year;
            this.amount = amount;
            this.sign = sign;
        }

        public static ProfitDelta positive(String nation, String year, BigDecimal amount) {
            return new ProfitDelta(nation, year, amount, 1);
        }

        public static ProfitDelta negative(String nation, String year, BigDecimal amount) {
            return new ProfitDelta(nation, year, amount, -1);
        }
    }

    public static class BucketState implements Serializable {
        public Map<String, Nation> nations = new HashMap<>();
        public Map<String, Supplier> suppliers = new HashMap<>();
        public Map<String, Order> orders = new HashMap<>();
        public Map<String, Part> parts = new HashMap<>();
        public Map<String, PartSupp> partsupps = new HashMap<>();
        public Map<String, LineItem> lineitems = new HashMap<>();
        public Map<String, List<String>> liByOrder = new HashMap<>();
        public Map<String, List<String>> liBySupp = new HashMap<>();
        public Map<String, List<String>> liByPart = new HashMap<>();
        public Map<String, List<String>> liByPartSupp = new HashMap<>();
        public Map<String, ActiveRecord> activeLineitems = new HashMap<>();
        public BucketState() {
        }
    }

    public static class ActiveRecord implements Serializable {
        public String liKey;
        public String nation;
        public String year;
        public BigDecimal amount;

        public ActiveRecord() {
        }

        public ActiveRecord(String liKey, String nation, String year, BigDecimal amount) {
            this.liKey = liKey;
            this.nation = nation;
            this.year = year;
            this.amount = amount;
        }
    }

    public static class Nation implements Serializable {
        public String nationKey;
        public String name;

        public Nation() {
        }

        public Nation(String nationKey, String name) {
            this.nationKey = nationKey;
            this.name = name;
        }
    }

    public static class Supplier implements Serializable {
        public String suppKey;
        public String nationKey;

        public Supplier() {
        }

        public Supplier(String suppKey, String nationKey) {
            this.suppKey = suppKey;
            this.nationKey = nationKey;
        }
    }

    public static class Order implements Serializable {
        public String orderKey;
        public String orderDate;

        public Order() {
        }

        public Order(String orderKey, String orderDate) {
            this.orderKey = orderKey;
            this.orderDate = orderDate;
        }
    }

    public static class Part implements Serializable {
        public String partKey;
        public String name;

        public Part() {
        }

        public Part(String partKey, String name) {
            this.partKey = partKey;
            this.name = name;
        }
    }

    public static class PartSupp implements Serializable {
        public String psKey;
        public String partKey;
        public String suppKey;
        public String supplyCost;

        public PartSupp() {
        }

        public PartSupp(String psKey, String partKey, String suppKey, String supplyCost) {
            this.psKey = psKey;
            this.partKey = partKey;
            this.suppKey = suppKey;
            this.supplyCost = supplyCost;
        }
    }

    public static class LineItem implements Serializable {
        public String liKey;
        public String orderKey;
        public String partKey;
        public String suppKey;
        public String lineNumber;
        public String quantity;
        public String extPrice;
        public String discount;

        public LineItem() {
        }

        public LineItem(
                String liKey,
                String orderKey,
                String partKey,
                String suppKey,
                String lineNumber,
                String quantity,
                String extPrice,
                String discount
        ) {
            this.liKey = liKey;
            this.orderKey = orderKey;
            this.partKey = partKey;
            this.suppKey = suppKey;
            this.lineNumber = lineNumber;
            this.quantity = quantity;
            this.extPrice = extPrice;
            this.discount = discount;
        }
    }
    private static String buildLineItemKey(
            String orderKey,
            String partKey,
            String suppKey,
            String lineNumber
    ) {
        return orderKey + "\n" + partKey + "\n" + suppKey + "\n" + lineNumber;
    }

    private static String buildPartSuppKey(String partKey, String suppKey) {
        return partKey + "\n" + suppKey;
    }

    private static int bucketOf(String key, int buckets) {
        return Math.floorMod(key.hashCode(), buckets);
    }

    private static String getYear(String dateStr) {
        try {
            return LocalDate.parse(dateStr).toString().substring(0, 4);
        } catch (Exception e) {
            return null;
        }
    }
}