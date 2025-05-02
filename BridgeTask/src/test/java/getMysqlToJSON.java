import org.example.specialDataSource;

import java.io.BufferedWriter;
import java.io.FileWriter;
import java.io.IOException;
import java.sql.*;
import java.util.Random;

public class getMysqlToJSON {
    private static final String SERVER_IP =
            "192.168.31.128";
    //            "127.0.0.1"; // 目标服务器IP地址
    private static final int SERVER_PORT = 40126; // 目标服务器端口号
    //mysql账号
    private static final String USER = "root";
    private static final String PASSWORD = "123456";
    private static final String DATABASES = "fanlidatabase";
    private static final String URL = "jdbc:mysql://" + SERVER_IP + ":3306/" + DATABASES + "?useSSL=false";

    public static class Forcetable1 {
        private int forceid;
        private String abutmentno;
        private String bridgeid;
        private String pierno;
        private float forcevalue;
        private String timeStamp;

        public Forcetable1(int forceid, String abutmentno, String bridgeid, String pierno, float forcevalue, String timeStamp) {
            this.forceid = forceid;
            this.abutmentno = abutmentno;
            this.bridgeid = bridgeid;
            this.pierno = pierno;
            this.forcevalue = forcevalue;
            this.timeStamp = timeStamp;
        }

        public int getForceid() {
            return forceid;
        }

        public void setForceid(int forceid) {
            this.forceid = forceid;
        }

        public String getAbutmentno() {
            return abutmentno;
        }

        public void setAbutmentno(String abutmentno) {
            this.abutmentno = abutmentno;
        }

        public String getBridgeid() {
            return bridgeid;
        }

        public void setBridgeid(String bridgeid) {
            this.bridgeid = bridgeid;
        }

        public String getPierno() {
            return pierno;
        }

        public void setPierno(String pierno) {
            this.pierno = pierno;
        }

        public float getForcevalue() {
            return forcevalue;
        }

        public void setForcevalue(float forcevalue) {
            this.forcevalue = forcevalue;
        }

        public String getTimeStamp() {
            return timeStamp;
        }

        public void setTimeStamp(String timeStamp) {
            this.timeStamp = timeStamp;
        }
    }
    private static Random random = new Random();
    public static void main(String[] args) throws SQLException {

        Connection conn = DriverManager.getConnection(URL, USER, PASSWORD);
        ResultSet rs = readMysqlTable(conn, "forcetablesp");
        while (rs.next()) {
            try (FileWriter fw = new FileWriter("input", true); // true表示追加内容
                 BufferedWriter bw = new BufferedWriter(fw)) {
                bw.newLine();
                bw.write(rs.getInt("forceid") + "," +
                        rs.getString("abutmentno") + "," +
                        rs.getString("bridgeid") + "," +
                        rs.getString("pierno") + "," +
                        rs.getFloat("forcevalue") + "," +
                        rs.getString("time_stamp") + "," +
                        rs.getFloat("temperature") + "," + 
                        rs.getFloat("humidness") + "," +
                        rs.getByte("overload") + "," +
                        rs.getByte("voidwarning")
                );

            } catch (IOException e) {
                e.printStackTrace();
            }

//
//            Forcetable1 forceTable = new Forcetable1(
//                    rs.getInt("yujingid"),
//                    rs.getString("abutmentno"),
//                    rs.getString("bridgeid"),
//                    rs.getString("pierno"),
//                    rs.getFloat("forcevalue"),
//                    rs.getString("time_stamp")
//            );
//            if (forceTable.getPierno().equals("02") & forceTable.getAbutmentno().equals("32") & forceTable.getBridgeid().equals("03")) {
//                System.out.println(forceTable.getPierno() + forceTable.getBridgeid() + forceTable.getAbutmentno());
////                insertForceTable(conn, forceTable, "forcetable");//mysql输出反力表
//                insertyujing(conn, forceTable, "yujing", 0);//mysql输出预警表}
//            }
        }conn.close();
    }
        public static void insertForceTable (Connection conn, Forcetable1 forceTable, String tbName){
            String sql = "UPDATE " + tbName + " SET pierno = '04' where forceid = ?";
            try (PreparedStatement pstmt = conn.prepareStatement(sql)) {
                pstmt.setInt(1, forceTable.getForceid());
                pstmt.executeUpdate(); // 执行更新，不关心影响的行数
            } catch (SQLException e) {
                e.printStackTrace();
                try {
                    if (conn != null) {
                        conn.rollback(); // 如果插入失败，回滚事务
                    }
                } catch (SQLException ex) {
                    ex.printStackTrace();
                }
            }
        }

        /**
         * @param conn       mysql链接
         * @param forceTable 反力表数据
         * @param tbName     表名
         * @param flag       墩台flag
         * @throws
         * @work 向mysql插入预警表数据
         * @input 反力表数据
         */
        public static void insertyujing (Connection conn, Forcetable1 forceTable, String tbName,int flag){
            String sql = "UPDATE " + tbName + " SET pierno = '03' where yujingid = ? ";
            try (PreparedStatement pstmt = conn.prepareStatement(sql)) {
                pstmt.setInt(1, forceTable.getForceid());
                pstmt.executeUpdate(); // 执行更新，不关心影响的行数
            } catch (SQLException e) {
                e.printStackTrace();
                try {
                    if (conn != null) {
                        conn.rollback(); // 如果插入失败，回滚事务
                    }
                } catch (SQLException ex) {
                    ex.printStackTrace();
                }
            }
        }

        public static ResultSet readMysqlTable (Connection conn, String tbName) throws SQLException {
            String sql = "SELECT * FROM " + tbName;
            ResultSet rs = null;
            try {
                // 确保传入的Connection是打开的
                if (!conn.isClosed()) {
                    Statement stmt = conn.createStatement();
                    rs = stmt.executeQuery(sql);
                    // 注意：不要在这里关闭Connection或Statement，让调用者来管理它们
                }
            } catch (SQLException e) {
                e.printStackTrace();
                // 如果出现异常，确保关闭ResultSet（如果它已经打开）
                if (rs != null && !rs.isClosed()) {
                    rs.close();
                }
                throw e; // 抛出异常，让调用者知道发生了错误
            }
            return rs;
        }

    }
