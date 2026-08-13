package verglas.fixture;

import java.io.IOException;
import java.io.UncheckedIOException;
import java.math.BigDecimal;
import java.nio.file.Files;
import java.nio.file.Paths;
import java.time.LocalDateTime;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.atomic.AtomicLong;

import org.apache.hadoop.conf.Configuration;
import org.apache.iceberg.AppendFiles;
import org.apache.iceberg.DataFile;
import org.apache.iceberg.FileFormat;
import org.apache.iceberg.HasTableOperations;
import org.apache.iceberg.OverwriteFiles;
import org.apache.iceberg.PartitionKey;
import org.apache.iceberg.PartitionSpec;
import org.apache.iceberg.Schema;
import org.apache.iceberg.Snapshot;
import org.apache.iceberg.Table;
import org.apache.iceberg.CatalogProperties;
import org.apache.iceberg.catalog.Namespace;
import org.apache.iceberg.catalog.TableIdentifier;
import org.apache.iceberg.data.GenericAppenderFactory;
import org.apache.iceberg.data.GenericRecord;
import org.apache.iceberg.data.Record;
import org.apache.iceberg.encryption.EncryptedOutputFile;
import org.apache.iceberg.exceptions.AlreadyExistsException;
import org.apache.iceberg.expressions.Expressions;
import org.apache.iceberg.io.DataWriter;
import org.apache.iceberg.io.OutputFileFactory;
import org.apache.iceberg.jdbc.JdbcCatalog;
import org.apache.iceberg.types.Types;

/**
 * Builds a spec-complete Iceberg table whose <em>data</em> files are Avro (or,
 * for the mixed variant, a genuine mix of Avro and Parquet across snapshots).
 *
 * <p>Why plain Java: pyiceberg and DuckDB both write Parquet data files only
 * (verified against pyiceberg 0.11.1, which silently ignores
 * {@code write.format.default=avro} and writes Parquet anyway, and against
 * DuckDB-Iceberg, whose writes are Parquet). Emitting Avro data files with
 * correct manifests needs Iceberg's own writers. This uses iceberg-core plus
 * iceberg-data's {@link GenericAppenderFactory} — no Spark.
 *
 * <p>The table shape mirrors the Parquet leg's pyiceberg fixture exactly (same
 * schema, same deterministic values, same four-snapshot history) so the
 * engine-matrix harness runs identically across all three variants; only the
 * data-file format changes. The matrix reads tables by metadata location, so
 * this writes a JSON sidecar with the final and time-travel metadata.json S3
 * paths, matching the Parquet leg's sidecar contract.
 */
public final class FixtureBuilder {

    /** Epoch the timestamp column counts minutes from (matches the Python leg). */
    private static final LocalDateTime EPOCH = LocalDateTime.of(2024, 1, 1, 0, 0, 0);

    /** Unique task ids for output files across every snapshot. */
    private static final AtomicLong TASK_ID = new AtomicLong(0);

    private FixtureBuilder() {}

    /**
     * The fixture schema: a long key, the identity-partition category, and mixed
     * types including nullable strings/doubles, a float, a microsecond timestamp,
     * a boolean, and a decimal(12,2) — the same columns the Parquet leg writes.
     */
    private static Schema schema() {
        return new Schema(
                Types.NestedField.required(1, "id", Types.LongType.get()),
                Types.NestedField.required(2, "category", Types.StringType.get()),
                Types.NestedField.optional(3, "name", Types.StringType.get()),
                Types.NestedField.optional(4, "amount", Types.DoubleType.get()),
                Types.NestedField.optional(5, "ratio", Types.FloatType.get()),
                Types.NestedField.optional(6, "ts", Types.TimestampType.withoutZone()),
                Types.NestedField.optional(7, "flag", Types.BooleanType.get()),
                Types.NestedField.optional(8, "price", Types.DecimalType.of(12, 2)));
    }

    /** One deterministic record; every value is a pure function of {@code id}. */
    private static Record record(Schema schema, long id, String category) {
        GenericRecord r = GenericRecord.create(schema);
        r.setField("id", id);
        r.setField("category", category);
        r.setField("name", (id % 7 == 0) ? null : String.format("name-%05d", id));
        Double amount =
                (id % 5 == 0) ? null : Math.round((id * 1.5 + 0.25) * 100.0) / 100.0;
        r.setField("amount", amount);
        r.setField("ratio", (float) ((id % 100) / 100.0));
        r.setField("ts", EPOCH.plusMinutes(id));
        r.setField("flag", id % 2 == 0);
        r.setField(
                "price",
                (id % 11 == 0) ? null : new BigDecimal(String.format("%d.%02d", id, id % 100)));
        return r;
    }

    /** Category assignment functions, one per snapshot, matching the Parquet leg. */
    private interface CategoryOf {
        String apply(long id);
    }

    /**
     * Writes one partition's worth of records to a single data file in the given
     * format and returns its {@link DataFile} for the pending commit.
     */
    private static DataFile writePartitionFile(
            Table table,
            Schema schema,
            PartitionSpec spec,
            GenericAppenderFactory appenders,
            FileFormat format,
            String category,
            List<Record> records) {
        OutputFileFactory files =
                OutputFileFactory.builderFor(table, 1, TASK_ID.incrementAndGet())
                        .format(format)
                        .build();
        PartitionKey key = new PartitionKey(spec, schema);
        key.partition(records.get(0));
        EncryptedOutputFile out = files.newOutputFile(key);
        DataWriter<Record> writer = appenders.newDataWriter(out, format, key);
        try {
            for (Record r : records) {
                writer.write(r);
            }
        } finally {
            try {
                writer.close();
            } catch (IOException e) {
                throw new UncheckedIOException(e);
            }
        }
        return writer.toDataFile();
    }

    /** Groups a snapshot's records by category (one data file per partition). */
    private static Map<String, List<Record>> byCategory(
            Schema schema, long from, long to, CategoryOf categoryOf) {
        Map<String, List<Record>> groups = new LinkedHashMap<>();
        for (long id = from; id < to; id++) {
            String category = categoryOf.apply(id);
            groups.computeIfAbsent(category, k -> new ArrayList<>())
                    .add(record(schema, id, category));
        }
        return groups;
    }

    /** Appends one snapshot: every partition's file written in {@code format}. */
    private static void appendSnapshot(
            Table table,
            Schema schema,
            PartitionSpec spec,
            GenericAppenderFactory appenders,
            FileFormat format,
            long from,
            long to,
            CategoryOf categoryOf) {
        AppendFiles append = table.newAppend();
        for (Map.Entry<String, List<Record>> e :
                byCategory(schema, from, to, categoryOf).entrySet()) {
            append.appendFile(
                    writePartitionFile(
                            table, schema, spec, appenders, format, e.getKey(), e.getValue()));
        }
        append.commit();
    }

    /**
     * Genuine partial overwrite of the {@code category=A} partition with fresh
     * rows written in {@code format} — a real delete-and-replace snapshot.
     */
    private static void overwriteCategoryA(
            Table table,
            Schema schema,
            PartitionSpec spec,
            GenericAppenderFactory appenders,
            FileFormat format,
            long from,
            long to) {
        OverwriteFiles overwrite = table.newOverwrite();
        overwrite.overwriteByRowFilter(Expressions.equal("category", "A"));
        for (Map.Entry<String, List<Record>> e :
                byCategory(schema, from, to, id -> "A").entrySet()) {
            overwrite.addFile(
                    writePartitionFile(
                            table, schema, spec, appenders, format, e.getKey(), e.getValue()));
        }
        overwrite.commit();
    }

    /** The current metadata.json location (an s3:// path) after a commit. */
    private static String metadataLocation(Table table) {
        table.refresh();
        return ((HasTableOperations) table).operations().current().metadataFileLocation();
    }

    public static void main(String[] args) throws Exception {
        Map<String, String> a = parseArgs(args);
        String variant = require(a, "variant"); // avro | mixed
        String bucket = require(a, "bucket");
        String prefix = require(a, "prefix");
        String namespace = require(a, "namespace");
        String tableName = a.getOrDefault("table", "events");
        String catalogDb = require(a, "catalog-db");
        String sidecarPath = require(a, "sidecar");
        String endpoint = require(a, "endpoint");
        String accessKey = require(a, "access-key-id");
        String secretKey = require(a, "secret-access-key");
        String region = a.getOrDefault("region", "us-east-1");

        // Per-snapshot data-file formats. The mixed variant alternates so the
        // final table (and the time-travel snapshot) each read across both
        // formats at once — exactly the Parquet-only assumption #142 worried
        // about, now exercised below the engines.
        FileFormat[] formats;
        if ("avro".equals(variant)) {
            formats = new FileFormat[] {FileFormat.AVRO, FileFormat.AVRO, FileFormat.AVRO, FileFormat.AVRO};
        } else if ("mixed".equals(variant)) {
            formats = new FileFormat[] {FileFormat.AVRO, FileFormat.PARQUET, FileFormat.AVRO, FileFormat.PARQUET};
        } else {
            throw new IllegalArgumentException("variant must be 'avro' or 'mixed', got: " + variant);
        }

        Map<String, String> props = new HashMap<>();
        props.put(CatalogProperties.URI, "jdbc:sqlite:" + catalogDb);
        props.put(CatalogProperties.WAREHOUSE_LOCATION, "s3://" + bucket + "/" + prefix);
        props.put(CatalogProperties.FILE_IO_IMPL, "org.apache.iceberg.aws.s3.S3FileIO");
        props.put("s3.endpoint", endpoint);
        props.put("s3.path-style-access", "true");
        props.put("s3.access-key-id", accessKey);
        props.put("s3.secret-access-key", secretKey);
        props.put("client.region", region);

        JdbcCatalog catalog = new JdbcCatalog();
        catalog.setConf(new Configuration());
        catalog.initialize("matrix", props);

        try {
            catalog.createNamespace(Namespace.of(namespace));
        } catch (AlreadyExistsException ignored) {
            // Namespace may already exist from a previous run; that is fine.
        }

        TableIdentifier ident = TableIdentifier.of(namespace, tableName);
        if (catalog.tableExists(ident)) {
            catalog.dropTable(ident, true);
        }

        Schema schema = schema();
        PartitionSpec spec = PartitionSpec.builderFor(schema).identity("category").build();
        Table table = catalog.createTable(ident, schema, spec);
        GenericAppenderFactory appenders = new GenericAppenderFactory(schema, spec);

        // S1: ids 0..199 across A/B.
        appendSnapshot(table, schema, spec, appenders, formats[0], 0, 200,
                id -> (id % 2 == 0) ? "A" : "B");
        // S2: ids 200..399 across B/C — the time-travel target.
        appendSnapshot(table, schema, spec, appenders, formats[1], 200, 400,
                id -> (id % 2 == 0) ? "B" : "C");
        String timetravelMetadata = metadataLocation(table);
        long timetravelSnapshot = table.currentSnapshot().snapshotId();

        // S3: genuine partial overwrite of category A with fresh rows (ids 1000..1099).
        overwriteCategoryA(table, schema, spec, appenders, formats[2], 1000, 1100);
        // S4: append category D (ids 400..499) — the final snapshot.
        appendSnapshot(table, schema, spec, appenders, formats[3], 400, 500, id -> "D");
        String finalMetadata = metadataLocation(table);
        long finalSnapshot = table.currentSnapshot().snapshotId();

        int snapshotCount = 0;
        for (Snapshot ignored : table.snapshots()) {
            snapshotCount++;
        }
        String totalRecords = table.currentSnapshot().summary().getOrDefault("total-records", "?");

        String json = sidecarJson(
                namespace + "." + tableName,
                finalMetadata,
                finalSnapshot,
                timetravelMetadata,
                timetravelSnapshot,
                snapshotCount,
                totalRecords,
                variant);
        Files.writeString(Paths.get(sidecarPath), json);

        System.out.printf(
                "[fixture-jvm] %s.%s: %d snapshots, %s final rows, partitioned by category, %s data files%n",
                namespace, tableName, snapshotCount, totalRecords, variant);
        System.out.printf("[fixture-jvm] wrote sidecar -> %s%n", sidecarPath);
        catalog.close();
    }

    /** Builds the sidecar JSON the matrix phase reads (matches the Parquet leg). */
    private static String sidecarJson(
            String tableIdent,
            String finalMetadata,
            long finalSnapshot,
            String timetravelMetadata,
            long timetravelSnapshot,
            int snapshotCount,
            String finalRowCount,
            String variant) {
        StringBuilder sb = new StringBuilder();
        sb.append("{\n");
        sb.append("  \"table_ident\": \"").append(tableIdent).append("\",\n");
        sb.append("  \"final_metadata\": \"").append(finalMetadata).append("\",\n");
        sb.append("  \"final_snapshot\": ").append(finalSnapshot).append(",\n");
        sb.append("  \"timetravel_metadata\": \"").append(timetravelMetadata).append("\",\n");
        sb.append("  \"timetravel_snapshot\": ").append(timetravelSnapshot).append(",\n");
        sb.append("  \"snapshot_count\": ").append(snapshotCount).append(",\n");
        sb.append("  \"final_row_count\": ").append(finalRowCount).append(",\n");
        sb.append("  \"data_file_format\": \"").append(variant).append("\"\n");
        sb.append("}\n");
        return sb.toString();
    }

    /** Minimal {@code --key value} parser; every input arrives as an explicit flag. */
    private static Map<String, String> parseArgs(String[] args) {
        Map<String, String> out = new HashMap<>();
        for (int i = 0; i < args.length; i++) {
            String arg = args[i];
            if (!arg.startsWith("--")) {
                throw new IllegalArgumentException("expected --flag, got: " + arg);
            }
            if (i + 1 >= args.length) {
                throw new IllegalArgumentException("flag missing value: " + arg);
            }
            out.put(arg.substring(2), args[++i]);
        }
        return out;
    }

    /** Fetches a required flag or fails with a readable message. */
    private static String require(Map<String, String> args, String key) {
        String v = args.get(key);
        if (v == null || v.isEmpty()) {
            throw new IllegalArgumentException("missing required flag: --" + key);
        }
        return v;
    }
}
