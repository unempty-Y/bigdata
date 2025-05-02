package org.example;

import com.google.gson.Gson;

import java.io.IOException;
import java.net.*;
import java.sql.*;
import java.text.SimpleDateFormat;
import java.util.ArrayList;
import java.util.Date;
import java.util.Random;

public class dataSource {
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
    private static final double CarWeight = 500;


    //概率相关参数
    private static final double VehicleUp = 0.6; //新车上桥概率
    private static final double DIRTY_RATE = 0.0; //脏数据占比
    private static final double DAMAGE_ACC = 0.001; // 桥梁损伤积累速率，自定义参数
    private static final double DAMAGE_THRESHOLD = 0.5; // 脱空开始阈值，自定义参数
    private static final double DAMAGE_V1 = 0.165; // 初始损伤基数，自定义参数
    private static final double DAMAGE_V2 = 0.2; // 初始损伤变动范围，自定义参数 V1+V2*随机
    private static final double VOIDRATE = 0.5; // 超过阈值后脱空概率

    //阈值 范围
    private static final int FORCE_MIN = 0;

    static class Abutment {

        Double damageCount;//损伤大小
        String abutmentno;
        String bridgeid;
        String pierno;
        String kongno;
        float elastic;
        String slabno;
        Double force;

        public Abutment(Double damageCount, String abutmentno, String bridgeid, String pierno, String kongno, float elastic, String slabno) {
            this.damageCount = damageCount;
            this.abutmentno = abutmentno;
            this.bridgeid = bridgeid;
            this.pierno = pierno;
            this.kongno = kongno;
            this.elastic = elastic;
            this.slabno = slabno;
        }

        public void setForce(Double force) {
            this.force = force;
        }

        public void Damage() {
            this.damageCount = this.damageCount + DAMAGE_ACC;
        }
    }

    static class Slab {
        String slabno;
        float weight;
        String bridgeid;

        public Slab(String slabno, float weight, String bridgeid) {
            this.slabno = slabno;
            this.weight = weight;
            this.bridgeid = bridgeid;
        }
    }

    static class Pier {
        String pierno;
        String bridgeid;
        int flag;

        public Pier(String pierno, String bridgeid, int flag) {
            this.pierno = pierno;
            this.bridgeid = bridgeid;
            this.flag = flag;
        }
    }

    static class Bridge {
        String bridgeid;
        String bridgename;
        String address;
        int beamlines;

        public Bridge(String bridgeid, String bridgename, String address) {
            this.bridgeid = bridgeid;
            this.bridgename = bridgename;
            this.address = address;
        }

        public void setBeamlines(int beamlines) {
            this.beamlines = beamlines;
        }
    }

    static class Vehicle {
        String bridgeid;//车辆在哪个桥上
        int abutmentno; // 车辆在桥梁上的位置
        double weight; // 车辆的重量
        int beamlines;//梁的片数

        public Vehicle(String bridgeid, int abutmentno, double weight, int beamlines) {
            this.bridgeid = bridgeid;
            this.abutmentno = abutmentno;
            this.weight = weight;
            this.beamlines = beamlines;
        }

        // 移动车辆到下一个支座
        public void move() {
            abutmentno = abutmentno + beamlines * 2;
        }
    }

    private static Random random = new Random();

    public static void main(String[] args) throws UnknownHostException {
        DatagramSocket socket = null;
        try {
            socket = new DatagramSocket();
            ArrayList<Vehicle> vehicles = new ArrayList<>(); // 车辆队列
            ArrayList<Abutment> abutments = new ArrayList<>();//支座队列
            ArrayList<Slab> slabs = new ArrayList<>();//梁板队列
            ArrayList<Pier> piers = new ArrayList<>();//梁板队列
            ArrayList<Bridge> bridges = new ArrayList<>();//桥梁队列
            Connection conn = null;
            float overload_threshold = 0;
            float void_threshold = 0;
            try {//读取数据
                conn = DriverManager.getConnection(URL, USER, PASSWORD);
                ResultSet rs = readMysqlTable(conn, "abutment");
                while (rs.next()) {
                    if (!rs.getString("bridgeid").equals("01"))
                        abutments.add(new Abutment(
                                random.nextDouble() * DAMAGE_V2 + DAMAGE_V1,
                                rs.getString("abutmentno"),
                                rs.getString("bridgeid"),
                                rs.getString("pierno"),
                                rs.getString("kongno"),
                                rs.getFloat("elastic"),
                                rs.getString("slabno")
                        ));
                }
                rs = readMysqlTable(conn, "slab");
                while (rs.next()) {
                    if (!rs.getString("bridgeid").equals("01"))
                        slabs.add(new Slab(
                                rs.getString("slabno"),
                                rs.getFloat("weight"),
                                rs.getString("bridgeid")
                        ));
                }
                rs = readMysqlTable(conn, "pier");
                while (rs.next()) {
                    if (!rs.getString("bridgeid").equals("01"))
                        piers.add(new Pier(
                                rs.getString("pierno"),
                                rs.getString("bridgeid"),
                                rs.getInt("flag")
                        ));
                }
                rs = readMysqlTable(conn, "bridge");
                while (rs.next()) {
                    if (!rs.getString("bridgeid").equals("01"))
                        bridges.add(new Bridge(
                                rs.getString("bridgeid"),
                                rs.getString("bridgename"),
                                rs.getString("address")
                        ));
                }
                rs = readMysqlTable(conn, "threshold");
                while (rs.next()) {
                    if (rs.getString("thresholdname").equals("脱空阈值"))
                        void_threshold = rs.getFloat("thresholdvalue");
                    if (rs.getString("thresholdname").equals("超载阈值"))
                        overload_threshold = rs.getFloat("thresholdvalue");
                }
            } catch (SQLException e) {
                e.printStackTrace();
            } finally {
                // 关闭连接
                if (conn != null) {
                    try {
                        conn.close();
                    } catch (SQLException e) {
                        e.printStackTrace();
                    }
                }
            }//读取数据

            while (true) {
                SimpleDateFormat dateFormat = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss");

                //生成空桥压力数据
                for (Abutment abutment : abutments) {
                    double offset = abutment.damageCount;
                    double v = random.nextDouble();
                    if (offset >= DAMAGE_THRESHOLD && v < VOIDRATE) {
                        abutment.setForce(0.0);
                    } else {
                        double damageCount = (1 - offset + 2 * offset * random.nextDouble());
                        double weight = 0;
                        for (Slab slab : slabs)
                            if (slab.slabno.equals(abutment.slabno) && slab.bridgeid.equals(abutment.bridgeid)) {
                                weight = slab.weight;
                            }
                        abutment.setForce(weight * damageCount);
                    }
                }

                // 计算梁的片数
                for (Bridge bridge : bridges) {
                    int beamlines = 0;
                    for (Abutment abutment : abutments) {
                        if (abutment.bridgeid.equals(bridge.bridgeid) && abutment.pierno.equals("01")) {
                            beamlines++;
                        }
                    }
                    bridge.setBeamlines(beamlines / 2);
                }


                // 每秒对入桥支座进行一次车辆判定
                for (Bridge bridge : bridges) {
                    for (Abutment abutment : abutments) {
                        if (abutment.bridgeid.equals(bridge.bridgeid) && abutment.pierno.equals("01")) {
                            if (random.nextDouble() < VehicleUp) { // 新车上桥概率
                                double weight = 100 + random.nextDouble() * CarWeight; // 随机生成车辆的重量
                                vehicles.add(new Vehicle(bridge.bridgeid, Integer.parseInt(abutment.abutmentno), weight, bridge.beamlines));
                            }
                        }
                    }
                }

                // 更新车辆位置和压力
                ArrayList<Vehicle> vehiclesToRemove = new ArrayList<>();
                for (Vehicle vehicle : vehicles) {
                    for (Abutment abutment : abutments)
                        if (abutment.bridgeid.equals(vehicle.bridgeid) && abutment.abutmentno.equals(String.format("%02d", vehicle.abutmentno))) {
                            double offset = abutments.get(vehicle.abutmentno - 1).damageCount;
                            double damage = (1 - offset + 2 * offset * random.nextDouble());
                            abutment.setForce(vehicle.weight * damage + abutment.force); // 减1因为数组索引从0开始
                            vehicle.move(); // 移动到下一个支座
                        }
                    // 如果车辆下桥，则将其从队列中移除
                    if (vehicle.abutmentno / 4 >= abutments.size() / 4 - 1) {
                        vehiclesToRemove.add(vehicle);
                    }

                }
                vehicles.removeAll(vehiclesToRemove); // 移除已经离开桥梁的车辆

                double[] rateList = new double[abutments.size()];
                int i = 0;
                for (Abutment abutment : abutments) {

                    //生产脏数据
                    if (random.nextDouble() < DIRTY_RATE) {
                        abutment.setForce((-100 * random.nextDouble()));
                    }
                    //超载造成损伤
                    if (abutment.force >= overload_threshold) {
                        abutment.Damage();
                    }
                    if (random.nextDouble() < 0.01) {//环境因素随机损伤
                        abutment.Damage();
                    }

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
                    try (Connection conn1 = DriverManager.getConnection(URL, USER, PASSWORD)) {
//                        insertForceTable(conn1, forceTable, "forcetablesp");//mysql输出反力表
                        if (forcevalue >= overload_threshold || (forcevalue <= void_threshold && forcevalue > 0)) {
                            for (Pier pier : piers)
                                if (pier.pierno.equals(abutment.slabno) && pier.bridgeid.equals(abutment.bridgeid)) {
//                                    insertyujing(conn1, forceTable, "yujing", pier.flag);//mysql输出预警表
                                }
                        }
                    } catch (SQLException e) {
                        e.printStackTrace();
                        try {
                            if (conn != null) {
                                conn.rollback(); // 发生异常时回滚
                            }
                        } catch (SQLException ex) {
                            ex.printStackTrace();
                        }
                    }
                    socket.send(packet);//端口输出
                }

                System.out.printf("发送数据: %d 条 \n", abutments.size());
                System.out.printf("超载率: %.2f%%\n", rateWorker(abutments, 0, overload_threshold, abutments.size()) * 100);
                System.out.printf("脱空预警率: %.2f%%\n", rateWorker(abutments, 1, void_threshold, abutments.size()) * 100);
                System.out.printf("脏数据率: %.2f%%\n", rateWorker(abutments, 2, 0, abutments.size()) * 100);
                System.out.printf("脱空率: %.2f%%\n", rateWorker(abutments, 3, 0, abutments.size()) * 100);

                Thread.sleep(1000);

            }
        } catch (InterruptedException e) {
            e.printStackTrace();
        } catch (SocketException e) {
            throw new RuntimeException(e);
        } catch (IOException e) {
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
