package org.example;

import com.google.gson.Gson;

import java.io.IOException;
import java.net.*;
import java.sql.*;
import java.text.SimpleDateFormat;
import java.util.ArrayList;
import java.util.Date;
import java.util.Random;

public class specialDataSource {
    //基础数据
    private static final String SERVER_IP =
            "192.168.31.128";
    //            "127.0.0.1"; // 目标服务器IP地址
    private static final int SERVER_PORT = 40126; // 目标服务器端口号
    //mysql账号
    private static final String USER = "root";
    private static final String PASSWORD = "123456";
    private static final String DATABASES = "fanlidatabase";
    private static final String URL = "jdbc:mysql://" + SERVER_IP + ":3306/" + DATABASES + "?useSSL=false";


//TODO 模拟时间段,未实施
//    private static final int SIMULATION_DURATION = 24 * 60; // 模拟24小时，以分钟为单位
//    private static final int TIME_INTERVAL = 5; // 每隔5分钟记录一次数据
//    private static final int PEAK_HOURS_START = 7 * 60; // 高峰时间开始于早上7点
//    private static final int PEAK_HOURS_END = 9 * 60; // 高峰时间结束于早上9点
//    private static final int PEAK_HOURS_START_EVENING = 17 * 60; // 晚高峰时间开始于下午5点
//    private static final int PEAK_HOURS_END_EVENING = 19 * 60; // 晚高峰时间结束于下午7点

    //阈值 范围
    private static final int FORCE_MIN = 0;

    static class Abutment {

        String abutmentno;
        String bridgeid;
        String pierno;
        Double force;
        int flag;

        public Abutment() {
        }

        public Abutment(String abutmentno, String bridgeid, String pierno, Double force, int flag) {
            this.abutmentno = abutmentno;
            this.bridgeid = bridgeid;
            this.pierno = pierno;
            this.force = force;
            this.flag = flag;
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

        public Double getForce() {
            return force;
        }

        public int getFlag() {
            return flag;
        }

        public void setFlag(int flag) {
            this.flag = flag;
        }

        public void setForce(Double force) {
            this.force = force;
        }

    }


    private static Random random = new Random();

    public static void main(String[] args) throws UnknownHostException {
        DatagramSocket socket = null;
        try {
            socket = new DatagramSocket();
            ArrayList<Abutment> abutments = new ArrayList<>();//支座队列
            String[][] specialids = {
                    {"24", "02", "02"},
                    {"32", "03", "03"},
                    {"41", "04", "04"}
            };


            while (true) {
                SimpleDateFormat dateFormat = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss");
                for (String[] specialid : specialids) {
                    int flag = 0;
                    Abutment abutment = new Abutment(specialid[0], specialid[1], specialid[2], random.nextDouble()*10+5000, flag);
                    abutments.add(abutment);
                }

                double[] rateList = new double[abutments.size()];
                int i = 0;
                for (Abutment abutment : abutments) {

                    //数据规整
                    String abutmentno = abutment.abutmentno;
                    String bridgeid = abutment.bridgeid;
                    String pierno = abutment.pierno;
                    String time_stamp = dateFormat.format(new Date());
                    float forcevalue = abutment.force.floatValue();
                    //填入
                    ForceTable forceTable = new ForceTable(abutmentno, bridgeid, pierno, forcevalue, time_stamp);
                    //改为json数据
                    Gson gson = new Gson();
                    String data = gson.toJson(forceTable) + "\n";
                    byte[] buffer = data.getBytes();
                    InetAddress address = InetAddress.getByName(SERVER_IP);
                    DatagramPacket packet = new DatagramPacket(buffer, buffer.length, address, SERVER_PORT);
                    System.out.printf(data);
                    Connection conn1 = DriverManager.getConnection(URL, USER, PASSWORD);
                    insertForceTable(conn1, forceTable, "forcetable");//mysql输出反力表
                    insertyujing(conn1, forceTable, "yujing", 0);//mysql输出预警表
                    conn1.close();
                }
//                    socket.send(packet);//端口输出
                abutments.clear();

                System.out.printf("发送数据: %d 条 \n", abutments.size());
                Thread.sleep(1000);

            }
        } catch (InterruptedException e) {
            e.printStackTrace();
        } catch (SocketException e) {
            throw new RuntimeException(e);
        } catch (IOException e) {
            throw new RuntimeException(e);
        } catch (SQLException e) {
            throw new RuntimeException(e);
        } finally {
            if (socket != null) {
                socket.close();
            }
        }
    }

    /**
     * @param forceTable 反力表
     * @throws
     * @work 向mysql插入反力表数据
     * @input 反力表数据
     */
    public static void insertForceTable(Connection conn, ForceTable forceTable, String tbName) {
        String sql = "INSERT INTO " + tbName + " (abutmentno, bridgeid, pierno, forcevalue, time_stamp) VALUES (?, ?, ?, ?, ?)";
        try (PreparedStatement pstmt = conn.prepareStatement(sql)) {
            pstmt.setString(1, forceTable.getAbutmentno());
            pstmt.setString(2, forceTable.getBridgeid());
            pstmt.setString(3, forceTable.getPierno());
            pstmt.setFloat(4, forceTable.getForcevalue());
            pstmt.setString(5, forceTable.getTime_stamp());
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
    public static void insertyujing(Connection conn, ForceTable forceTable, String tbName, int flag) {
        String sql = "INSERT INTO " + tbName + " (abutmentno, bridgeid, pierno, forcevalue, time_stamp,flag) VALUES (?, ?, ?, ?, ?,?)";
        try (PreparedStatement pstmt = conn.prepareStatement(sql)) {
            pstmt.setString(1, forceTable.getAbutmentno());
            pstmt.setString(2, forceTable.getBridgeid());
            pstmt.setString(3, forceTable.getPierno());
            pstmt.setFloat(4, forceTable.getForcevalue());
            pstmt.setString(5, forceTable.getTime_stamp());
            pstmt.setInt(6, flag);
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

    public static ResultSet readMysqlTable(Connection conn, String tbName) throws SQLException {
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

    /**
     * @param abutments 压力值
     * @param flag      超载0，脱空预警1，脏数据2，脱空3
     * @return 脏数据率
     * @throws
     * @work 计算超载、脱空预警、脏数据、脱空率
     */
    private static double rateWorker(ArrayList<Abutment> abutments, int flag, double threshold, int Size) {
        int valueCount = 0;
        for (Abutment abutment : abutments) {
            if (flag == 0) {
                if (abutment.force > threshold) {
                    valueCount++;
                }
            } else if (flag == 1) {
                if (abutment.force > FORCE_MIN && abutment.force <= threshold) {
                    valueCount++;
                }
            } else if (flag == 2) {
                if (abutment.force < FORCE_MIN) {
                    valueCount++;
                }
            } else {
                if (abutment.force == 0)
                    valueCount++;
            }
        }
        return (double) valueCount / Size;
    }

    public static class ForceTable {
        private String abutmentno;
        private String bridgeid;
        private String pierno;
        private float forcevalue;
        private String time_stamp;

        //        public ForceTable() {}
        public ForceTable(String abutmentno, String bridgeid, String pierno, float forcevalue, String time_stamp) {
            this.abutmentno = abutmentno;
            this.bridgeid = bridgeid;
            this.pierno = pierno;
            this.forcevalue = forcevalue;
            this.time_stamp = time_stamp;
        }

        public String getAbutmentno() {
            return abutmentno;
        }

        public String getBridgeid() {
            return bridgeid;
        }

        public String getPierno() {
            return pierno;
        }

        public float getForcevalue() {
            return forcevalue;
        }

        public String getTime_stamp() {
            return time_stamp;
        }

    }

}
