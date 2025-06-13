package cn.edu.xjtu.dblab;

import com.opencsv.CSVReader;
import com.opencsv.exceptions.CsvValidationException;
import java.io.FileReader;
import java.io.IOException;
import java.sql.*;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.Collections;

public class App {

    public static final String driver = "org.opengauss.Driver";
    public static final String url = "jdbc:opengauss://10.0.2.15:26000/mydb";
    public static final String user = "joe";
    public static final String password = "OpenGauss@1234";
    public static final Connection conn = getConnect();
    
    private static final int THREAD_POOL_SIZE = 8;
    private static final int BATCH_SIZE = 2000; // 插入小规模数据时：100;
    private static final int DELETE_BATCH_SIZE = 50;
    
    public static void main(String[] args) {
        /* 这是插入小规模数据的导入文件
        String sFilePath = "students_1000.csv";
        String cFilePath = "courses_100.csv";
        String scFilePath = "stu_course_20000.csv";
        */
        // 这是插入大规模数据的导入文件
        String sFilePath = "students_5000.csv";
        String cFilePath = "courses_1000.csv";
        String scFilePath = "stu_course_200000.csv";
        try {
            // 读取CSV文件
            List<String[]> sData = readCsvFile(sFilePath);
            System.out.println("读取到 " + sData.size() + " 条记录");
            insertData(sData, "S348");

            List<String[]> cData = readCsvFile(cFilePath);
            System.out.println("读取到 " + cData.size() + " 条记录");
            insertData(cData, "C348");

            List<String[]> scData = readCsvFile(scFilePath);
            System.out.println("读取到 " + scData.size() + " 条记录");
            insertData(scData, "SC348");

            // 连接同学数据库删除用的函数
            // deleteNullRecords(DELETE_TARGET_COUNT);
            
        } catch (Exception e) {
            e.printStackTrace();
        }
    }

    public static List<String[]> readCsvFile(String filePath) throws IOException , CsvValidationException {
        List<String[]> records = new ArrayList<>();
        
        try (CSVReader csvReader = new CSVReader(new FileReader(filePath))) {
            // 跳过表头
            csvReader.readNext();
            
            String[] record;
            while ((record = csvReader.readNext()) != null) {
                records.add(record);
            }
        }
        
        return records;
    }

     
    /**
     * 多线程批量插入数据
     */
    public static void insertData(List<String[]> csvData, String table) {
        ExecutorService executor = Executors.newFixedThreadPool(THREAD_POOL_SIZE);
        
        // 将数据分批
        List<List<String[]>> batches = splitIntoBatches(csvData, BATCH_SIZE);
        
        CountDownLatch latch = new CountDownLatch(batches.size());
        
        long startTime = System.currentTimeMillis();
        
        for (List<String[]> batch : batches) {
            executor.submit(new InsertTask(batch, latch, conn, table));
        }
        
        try {
            // 等待所有任务完成
            latch.await();
            
            long endTime = System.currentTimeMillis();
            System.out.println("所有数据插入完成，耗时: " + (endTime - startTime) + "ms");
            
        } catch (InterruptedException e) {
            e.printStackTrace();
        } finally {
            executor.shutdown();
        }
    }

     /**
     * 查询并随机删除低分记录
     */
    public static void deleteLowRecord() {

        try {
            // 查询所有低分记录
            List<StudentCourse> records = queryLowRecords();
            
            if (records.isEmpty()) {
                System.out.println("没有找到符合删除条件的记录");
                return;
            }
            
            List<StudentCourse> recordsToDelete = selectRandomRecords(records, 1);
            System.out.println("随机选择了1条记录进行删除");
            
            // 多线程删除
            deleteRecords(recordsToDelete, "S348");
            
        } catch (SQLException e) {
            System.err.println("查询或删除操作失败: " + e.getMessage());
            e.printStackTrace();
        }
    }

    /**
     * 查询合作同学数据库并随机删除空成绩记录
     */
    public static void deleteNullRecords(int target_count) {
        try {
            // 查询所有低分记录
            List<StudentCourse> records = queryNullRecords();
            
            if (records.isEmpty()) {
                System.out.println("没有找到符合删除条件的记录");
                return;
            }
            
            // 随机选择记录
            List<StudentCourse> recordsToDelete = selectRandomRecords(records, target_count);
            
            // 多线程删除
            deleteRecords(recordsToDelete, "S544");
            
        } catch (SQLException e) {
            System.err.println("查询或删除操作失败: " + e.getMessage());
            e.printStackTrace();
        }
    }

    
    /**
     * 查询所有低分记录
     */
    public static List<StudentCourse> queryLowRecords() throws SQLException {
        List<StudentCourse> records = new ArrayList<>();
        
        String sql = "SELECT * FROM SC348 WHERE GRADE IS NULL OR GRADE < 60";
        
        try (PreparedStatement pstmt = conn.prepareStatement(sql);
             ResultSet rs = pstmt.executeQuery()) {
            
            while (rs.next()) {
                StudentCourse record = new StudentCourse();
                record.sno = rs.getString("S#");
                record.cno = rs.getString("C#");
                record.grade = rs.getBigDecimal("GRADE");
                records.add(record);
            }
        }
        
        return records;
    }


    /**
     * 查询合作同学数据库所有null成绩记录
     */
    public static List<StudentCourse> queryNullRecords() throws SQLException {
        List<StudentCourse> records = new ArrayList<>();
        
        String sql = "SELECT * FROM SC544 WHERE GRADE IS NULL";
        
        try (PreparedStatement pstmt = conn.prepareStatement(sql);
             ResultSet rs = pstmt.executeQuery()) {
            
            while (rs.next()) {
                StudentCourse record = new StudentCourse();
                record.sno = rs.getString("S#");
                record.cno = rs.getString("C#");
                record.grade = rs.getBigDecimal("GRADE");
                records.add(record);
            }
        }
        
        return records;
    }
    
    /**
     * 随机选择指定数量的记录
     */
    public static List<StudentCourse> selectRandomRecords(List<StudentCourse> allRecords, int targetCount) {
        List<StudentCourse> shuffledRecords = new ArrayList<>(allRecords);
        Collections.shuffle(shuffledRecords); 
        
        int actualCount = Math.min(targetCount, shuffledRecords.size());
        return shuffledRecords.subList(0, actualCount);
    }
    
    /**
     * 多线程删除记录
     */
    public static void deleteRecords(List<StudentCourse> records, String table) {
        System.out.println("开始删除记录...");
        ExecutorService executor = Executors.newFixedThreadPool(THREAD_POOL_SIZE);
        
        // 将要删除的记录分批
        List<List<StudentCourse>> deleteBatches = splitRecordsIntoBatches(records, DELETE_BATCH_SIZE);
        CountDownLatch latch = new CountDownLatch(deleteBatches.size());
        
        long startTime = System.currentTimeMillis();
        
        for (List<StudentCourse> batch : deleteBatches) {
            executor.submit(new DeleteTask(batch, latch, conn, table));
        }
        
        try {
            latch.await();
            long endTime = System.currentTimeMillis();
            System.out.println("记录删除完成，耗时: " + (endTime - startTime) + "ms");
            
        } catch (InterruptedException e) {
            e.printStackTrace();
        } finally {
            executor.shutdown();
        }
    }
    
    
    /**
     * 将数据分批
     */
    public static List<List<String[]>> splitIntoBatches(List<String[]> data, int batchSize) {
        List<List<String[]>> batches = new ArrayList<>();
        
        for (int i = 0; i < data.size(); i += batchSize) {
            int end = Math.min(i + batchSize, data.size());
            batches.add(data.subList(i, end));
        }
        
        return batches;
    }

    /**
     * 将记录分批
     */
    public static List<List<StudentCourse>> splitRecordsIntoBatches(List<StudentCourse> records, int batchSize) {
        List<List<StudentCourse>> batches = new ArrayList<>();
        
        for (int i = 0; i < records.size(); i += batchSize) {
            int end = Math.min(i + batchSize, records.size());
            batches.add(records.subList(i, end));
        }
        
        return batches;
    }

    /**
     * 插入任务
     */
    static class InsertTask implements Runnable {
        private final List<String[]> batch;
        private final CountDownLatch latch;
        private final Connection conn;
        private final String table;
        
        public InsertTask(List<String[]> batch, CountDownLatch latch, Connection conn, String table) {
            this.batch = batch;
            this.latch = latch;
            this.conn = conn;
            this.table = table;
        }
        
        @Override
        public void run() {
            try {
                conn.setAutoCommit(false);
                if (table.equals("S348")) {
                    String sql = "INSERT INTO S348 (S#, SNAME, SEX, BDATE, HEIGHT, DORM) VALUES (?, ?, ?, ?, ?, ?)";
                    
                    try (PreparedStatement pstmt = conn.prepareStatement(sql)) {
                        
                        for (String[] row : batch) {
                            pstmt.setString(1, row[1]);
                            pstmt.setString(2, row[2]);
                            pstmt.setString(3, row[3]);
                            pstmt.setString(4, row[4]);
                            pstmt.setString(5, row[5]);
                            pstmt.setString(6, row[6]);
                            pstmt.addBatch();
                        }
                        
                        // 执行批量插入
                        int[] results = pstmt.executeBatch();
                        conn.commit();
                        
                        System.out.println(Thread.currentThread().getName() + 
                            " 完成插入 " + results.length + " 条记录");
                        
                    } catch (SQLException e) {
                        conn.rollback();
                        throw e;
                    }
                } else if (table.equals("C348")) {
                    String sql = "INSERT INTO C348 (C#, CNAME, PERIOD, CREDIT, TEACHER) VALUES (?, ?, ?, ?, ?)";
                    try (PreparedStatement pstmt = conn.prepareStatement(sql)) {
                        for (String[] row : batch) {
                            int period = Integer.parseInt(row[3]);
                            period = period == 0 ? 12 : period;
                            pstmt.setString(1, row[1]);
                            pstmt.setString(2, row[2]);
                            pstmt.setInt(3, period);
                            pstmt.setString(4, row[4]);
                            pstmt.setString(5, row[5]);
                            pstmt.addBatch();
                        }
                        
                        // 执行批量插入
                        int[] results = pstmt.executeBatch();
                        conn.commit();
                        
                        System.out.println(Thread.currentThread().getName() + 
                            " 完成插入 " + results.length + " 条记录");
                    } catch (SQLException e) {
                        conn.rollback();
                        throw e;
                    }
                } else if (table.equals("SC348")) {
                    String sql = "INSERT INTO SC348 (S#, C#, GRADE) VALUES (?, ?, ?)";
                    try (PreparedStatement pstmt = conn.prepareStatement(sql)) {
                        
                        for (String[] row : batch) {
                            pstmt.setString(1, row[1]);
                            pstmt.setString(2, row[2]);
                            pstmt.setString(3, row[3]);
                            pstmt.addBatch();
                        }
                        
                        // 执行批量插入
                        int[] results = pstmt.executeBatch();
                        conn.commit();
                        
                        System.out.println(Thread.currentThread().getName() + 
                            " 完成插入 " + results.length + " 条记录");
                    } catch (Exception e) {
                        System.out.println(e);
                        conn.rollback();
                        throw e;
                    }
                }
            
            } catch (SQLException e) {
                System.err.println("插入数据失败: " + e.getMessage());
                e.printStackTrace();
            } finally {
                latch.countDown();
            }
            /* 这是插入选课表数据时删除数据的代码
            if (table.equals("SC348")) {
                deleteLowRecord();
            }
            */
        }
    }

    /**
     * 删除任务
     */
    static class DeleteTask implements Runnable {
        private final List<StudentCourse> batch;
        private final CountDownLatch latch;
        private final Connection conn;
        private final String table;
        
        public DeleteTask(List<StudentCourse> batch, CountDownLatch latch, Connection conn, String table) {
            this.batch = batch;
            this.latch = latch;
            this.conn = conn;
            this.table = table;
        }
        
        @Override
        public void run() {
            try {
                conn.setAutoCommit(false);
                
                String sql = String.format("DELETE FROM %s WHERE S# = ? AND C# = ?", table);
                
                try (PreparedStatement pstmt = conn.prepareStatement(sql)) {
                    
                    for (StudentCourse record : batch) {
                        pstmt.setString(1, record.sno);
                        pstmt.setString(2, record.cno);
                        pstmt.addBatch();
                    }
                    
                    pstmt.executeBatch();
                    conn.commit();
                    System.out.println(Thread.currentThread().getName() + "完成删除记录");
                    
                } catch (SQLException e) {
                    conn.rollback();
                    throw e;
                }
                
            } catch (SQLException e) {
                System.err.println("删除数据失败: " + e.getMessage());
                e.printStackTrace();
            } finally {
                latch.countDown();
            }
        }
    }
    
    /**
     * 学生选课记录类
     */
    static class StudentCourse {
        String sno;
        String cno;
        java.math.BigDecimal grade;
        
        @Override
        public String toString() {
            return "StudentCourse[S#='" + sno + "', C#='" + cno + "', GRADE=" + grade + "]";
        }
    }

    private static Connection getConnect() {
        Connection conn = null;
        try {
            Class.forName(driver);
        } catch (Exception var9) {
            var9.printStackTrace();
            return null;
        }

        try {
            conn = DriverManager.getConnection(url, user, password);
            System.out.println("连接成功！");
            return conn;
        } catch (Exception var8) {
            var8.printStackTrace();
            return null;
        }
    }
}
