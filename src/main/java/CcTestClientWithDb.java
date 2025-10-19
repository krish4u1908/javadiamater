import java.io.FileInputStream;
import java.io.InputStream;
import java.sql.*;
import java.util.ArrayList;
import java.util.List;
import java.util.Properties;

import dk.i1.diameter.*;
import dk.i1.diameter.node.*;

/**
 * Combined Diameter client and JDBC helper.
 * Usage:
 *   java -cp .:path/to/jdbc-driver.jar:path/to/jdiameter.jar CcTestClientWithDb [path/to/config.properties]
 *
 * config.properties defaults to ./config.properties when no arg supplied.
 */
public class CcTestClientWithDb {
    public static void main(String[] args) throws Exception {
        String propsPath = args.length >= 1 ? args[0] : "config.properties";

        Properties cfg = new Properties();
        try (InputStream in = new FileInputStream(propsPath)) {
            cfg.load(in);
        } catch (Exception e) {
            System.err.println("Failed to load configuration from " + propsPath + ": " + e.getMessage());
            return;
        }

        // Read diameter properties
        String host_id = cfg.getProperty("host_id");
        String realm = cfg.getProperty("realm");
        String dest_host = cfg.getProperty("peer_host");
        int dest_port = Integer.parseInt(cfg.getProperty("peer_port", "3868"));

        // Read jdbc properties
        String jdbcDriver = cfg.getProperty("jdbc.driver");
        String jdbcUrl = cfg.getProperty("jdbc.url");
        String jdbcUser = cfg.getProperty("jdbc.user");
        String jdbcPassword = cfg.getProperty("jdbc.password");
        String jdbcTable = cfg.getProperty("jdbc.table", "usage_record");

        if (host_id == null || realm == null || dest_host == null || jdbcDriver == null || jdbcUrl == null) {
            System.err.println("Configuration missing required properties. Check host_id, realm, peer_host, jdbc.driver, jdbc.url.");
            return;
        }

        // Initialize DB helper
        DBHelper db = new DBHelper(jdbcDriver, jdbcUrl, jdbcUser, jdbcPassword, jdbcTable);

        // Example DB operations
        try {
            db.connect();

            // 1) Read existing usage records
            System.out.println("Existing usage records:");
            List<UsageRecord> list = db.selectAll();
            for (UsageRecord r : list)
                System.out.println(r);

            // 2) Insert a sample usage record
            UsageRecord newRec = new UsageRecord();
            newRec.sessionId = "sess-" + System.currentTimeMillis();
            newRec.username = "user@example.net";
            newRec.unitsUsed = 42L;
            long newId = db.insert(newRec);
            System.out.println("Inserted usage_record id=" + newId);

            // 3) Update the units_used for the new record
            db.updateUnitsUsed(newId, 100L);
            System.out.println("Updated usage_record id=" + newId + " units_used=100");

            // 4) Select single row
            UsageRecord fetched = db.selectById(newId);
            System.out.println("Fetched: " + fetched);

            // 5) Optionally delete the test row (uncomment if desired)
            // db.delete(newId);
            // System.out.println("Deleted usage_record id=" + newId);

        } catch (Exception ex) {
            System.err.println("DB error: " + ex.getMessage());
            ex.printStackTrace();
        } finally {
            db.disconnect();
        }

        // Now initialize Diameter stack
        SimpleSyncClient ssc = initDiameter(host_id, realm, dest_host, dest_port);
        if (ssc == null) {
            System.err.println("Failed to initialize Diameter client.");
            return;
        }

        // build and send CCR
        Message cca = sendCCR(ssc);
        if (cca == null) {
            System.out.println("No response from Diameter peer.");
        } else {
            // Check result-code
            AVP result_code = cca.find(ProtocolConstants.DI_RESULT_CODE);
            if (result_code == null) {
                System.out.println("No result code returned in CCA.");
            } else {
                try {
                    AVP_Unsigned32 result_code_u32 = new AVP_Unsigned32(result_code);
                    int rc = result_code_u32.queryValue();
                    System.out.println("CCA result-code = " + rc);
                } catch (InvalidAVPLengthException ex) {
                    System.out.println("result-code was malformed");
                }
            }
        }

        // Stop the Diameter client
        ssc.stop();
    }

    // --- Diameter helpers (adapted from your original code) ---
    private static SimpleSyncClient initDiameter(String host_id, String realm, String dest_host, int dest_port) throws EmptyHostNameException {
        Capability capability = new Capability();
        capability.addAuthApp(ProtocolConstants.DIAMETER_APPLICATION_CREDIT_CONTROL);

        NodeSettings node_settings;
        try {
            node_settings = new NodeSettings(
                    host_id, realm,
                    99999, // vendor-id
                    capability,
                    0,
                    "cc_test_client", 0x01000000);
        } catch (InvalidSettingException e) {
            System.out.println(e.toString());
            return null;
        }

        Peer peers[] = new Peer[]{
                new Peer(dest_host, dest_port)
        };

        SimpleSyncClient ssc = new SimpleSyncClient(node_settings, peers);
        try {
            ssc.start();
            ssc.waitForConnection(); // allow connection to be established
        } catch (Exception e) {
            System.err.println("Error while starting SimpleSyncClient: " + e.getMessage());
            return null;
        }
        return ssc;
    }

    private static Message sendCCR(SimpleSyncClient ssc) {
        try {
            Message ccr = new Message();
            ccr.hdr.command_code = ProtocolConstants.DIAMETER_COMMAND_CC;
            ccr.hdr.application_id = ProtocolConstants.DIAMETER_APPLICATION_CREDIT_CONTROL;
            ccr.hdr.setRequest(true);
            ccr.hdr.setProxiable(true);

            // Session-Id
            ccr.add(new AVP_UTF8String(ProtocolConstants.DI_SESSION_ID, ssc.node().makeNewSessionId()));
            // Origin host/realm
            ssc.node().addOurHostAndRealm(ccr);
            // Destination-Realm
            ccr.add(new AVP_UTF8String(ProtocolConstants.DI_DESTINATION_REALM, "example.net"));
            // Auth-Application-Id
            ccr.add(new AVP_Unsigned32(ProtocolConstants.DI_AUTH_APPLICATION_ID, ProtocolConstants.DIAMETER_APPLICATION_CREDIT_CONTROL));
            // Service-Context-Id
            ccr.add(new AVP_UTF8String(ProtocolConstants.DI_SERVICE_CONTEXT_ID, "cc_test@example.net"));
            // CC-Request-Type
            ccr.add(new AVP_Unsigned32(ProtocolConstants.DI_CC_REQUEST_TYPE, ProtocolConstants.DI_CC_REQUEST_TYPE_EVENT_REQUEST));
            // CC-Request-Number
            ccr.add(new AVP_Unsigned32(ProtocolConstants.DI_CC_REQUEST_NUMBER, 0));
            // User-Name
            ccr.add(new AVP_UTF8String(ProtocolConstants.DI_USER_NAME, "user@example.net"));
            // Origin-State-Id
            ccr.add(new AVP_Unsigned32(ProtocolConstants.DI_ORIGIN_STATE_ID, ssc.node().stateId()));
            // Event-Timestamp
            ccr.add(new AVP_Time(ProtocolConstants.DI_EVENT_TIMESTAMP, (int) (System.currentTimeMillis() / 1000)));
            // Requested-Service-Unit (example)
            ccr.add(new AVP_Grouped(ProtocolConstants.DI_REQUESTED_SERVICE_UNIT,
                    new AVP[]{new AVP_Unsigned64(ProtocolConstants.DI_CC_SERVICE_SPECIFIC_UNITS, 42)}
            ));
            // Requested-Action
            ccr.add(new AVP_Unsigned32(ProtocolConstants.DI_REQUESTED_ACTION, ProtocolConstants.DI_REQUESTED_ACTION_DIRECT_DEBITING));
            // Service-Parameter-Info (example)
            ccr.add(new AVP_Grouped(ProtocolConstants.DI_SERVICE_PARAMETER_INFO,
                    new AVP[]{new AVP_Unsigned32(ProtocolConstants.DI_SERVICE_PARAMETER_TYPE, 42),
                            new AVP_UTF8String(ProtocolConstants.DI_SERVICE_PARAMETER_VALUE, "Hovercraft")}
            ));

            Utils.setMandatory_RFC3588(ccr);
            Utils.setMandatory_RFC4006(ccr);

            // Send it and wait for answer
            Message cca = ssc.sendRequest(ccr);
            return cca;
        } catch (Exception e) {
            System.err.println("Error building/sending CCR: " + e.getMessage());
            e.printStackTrace();
            return null;
        }
    }

    // --- Simple data holder for usage_record ---
    public static class UsageRecord {
        public long id;
        public String sessionId;
        public String username;
        public long unitsUsed;
        public Timestamp createdAt;

        @Override
        public String toString() {
            return "UsageRecord{id=" + id + ", sessionId='" + sessionId + '\'' +
                    ", username='" + username + '\'' + ", unitsUsed=" + unitsUsed +
                    ", createdAt=" + createdAt + '}';
        }
    }

    // --- DB helper: connect, select, insert, update, delete ---
    public static class DBHelper {
        private final String driver;
        private final String url;
        private final String user;
        private final String password;
        private final String table;
        private Connection conn;

        public DBHelper(String driver, String url, String user, String password, String table) {
            this.driver = driver;
            this.url = url;
            this.user = user;
            this.password = password;
            this.table = table;
        }

        public void connect() throws Exception {
            Class.forName(driver);
            this.conn = DriverManager.getConnection(url, user, password);
            this.conn.setAutoCommit(true);
            System.out.println("Connected to DB: " + url);
        }

        public void disconnect() {
            if (this.conn != null) {
                try {
                    this.conn.close();
                } catch (SQLException ignore) {
                }
            }
        }

        // selects all rows (simple example)
        public List<UsageRecord> selectAll() throws SQLException {
            String sql = "SELECT id, session_id, username, units_used, created_at FROM " + table + " ORDER BY id DESC LIMIT 100";
            List<UsageRecord> rows = new ArrayList<>();
            try (PreparedStatement ps = conn.prepareStatement(sql);
                 ResultSet rs = ps.executeQuery()) {
                while (rs.next()) {
                    UsageRecord r = new UsageRecord();
                    r.id = rs.getLong("id");
                    r.sessionId = rs.getString("session_id");
                    r.username = rs.getString("username");
                    r.unitsUsed = rs.getLong("units_used");
                    r.createdAt = rs.getTimestamp("created_at");
                    rows.add(r);
                }
            }
            return rows;
        }

        // select by id
        public UsageRecord selectById(long id) throws SQLException {
            String sql = "SELECT id, session_id, username, units_used, created_at FROM " + table + " WHERE id = ?";
            try (PreparedStatement ps = conn.prepareStatement(sql)) {
                ps.setLong(1, id);
                try (ResultSet rs = ps.executeQuery()) {
                    if (rs.next()) {
                        UsageRecord r = new UsageRecord();
                        r.id = rs.getLong("id");
                        r.sessionId = rs.getString("session_id");
                        r.username = rs.getString("username");
                        r.unitsUsed = rs.getLong("units_used");
                        r.createdAt = rs.getTimestamp("created_at");
                        return r;
                    }
                }
            }
            return null;
        }

        // insert, returns generated id
        public long insert(UsageRecord r) throws SQLException {
            String sql = "INSERT INTO " + table + " (session_id, username, units_used) VALUES (?, ?, ?)";
            try (PreparedStatement ps = conn.prepareStatement(sql, Statement.RETURN_GENERATED_KEYS)) {
                ps.setString(1, r.sessionId);
                ps.setString(2, r.username);
                ps.setLong(3, r.unitsUsed);
                int updated = ps.executeUpdate();
                if (updated == 0) throw new SQLException("Insert failed, no rows affected.");
                try (ResultSet keys = ps.getGeneratedKeys()) {
                    if (keys.next()) return keys.getLong(1);
                }
            }
            return -1;
        }

        // update units_used
        public int updateUnitsUsed(long id, long newUnits) throws SQLException {
            String sql = "UPDATE " + table + " SET units_used = ? WHERE id = ?";
            try (PreparedStatement ps = conn.prepareStatement(sql)) {
                ps.setLong(1, newUnits);
                ps.setLong(2, id);
                return ps.executeUpdate();
            }
        }

        // delete by id
        public int delete(long id) throws SQLException {
            String sql = "DELETE FROM " + table + " WHERE id = ?";
            try (PreparedStatement ps = conn.prepareStatement(sql)) {
                ps.setLong(1, id);
                return ps.executeUpdate();
            }
        }
    }
}
