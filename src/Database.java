import java.sql.*;
import java.util.*;

/**
 * Create by : liu
 * Create on : 2018/8/20
 * Create for : mysql语句的学习
 */

public class Database {

    private String driver = "com.mysql.jdbc.Driver";
    private String url = "jdbc:mysql://localhost:3306/company?useUnicode=true&characterEncoding=utf8&useSSL=false";
    private String name = "root";
    private String password = "15213698256";
    private Connection con;
    private PreparedStatement pst;
    private ResultSet rs;

    public Database() {
        try {
            Class.forName(driver);
            con = DriverManager.getConnection(url, name, password);
        } catch (ClassNotFoundException e) {
            System.out.println("加载数据库驱动失败");
        } catch (SQLException e) {
            e.printStackTrace();
        }
    }

    /**
     * 获取一个部门员工的最大工资和最小工资
     *
     * @return Map中String是部门名, List<Interger> 中前一个为最小工资,后一个为最大工资
     */
    public Map<String, List<Integer>> getMaxAndMinWages() {
        Map<String, List<Integer>> wagesInfo = new HashMap<>();
        try {
            pst = con.prepareStatement("select min(emp.sal), max(emp.sal), dept.dname from company.emp join company.dept on emp.deptno = dept.deptno group by dname");
            rs = pst.executeQuery();
            while (rs.next()) {
                List<Integer> wages = new LinkedList<>();
                wages.add(rs.getInt(1));
                wages.add(rs.getInt(2));
                wagesInfo.put(rs.getString(3), wages);
            }
        } catch (SQLException e) {
            e.printStackTrace();
        }
        return wagesInfo;
    }

    /**
     * 按照各个部门的平均工资由高到低对，获取部门名，平均工资
     *
     * @return map中String为部门名, Interger为平均工资
     */
    public Map<String, Integer> getAvgWages() {
        Map<String, Integer> wagesInfo = new LinkedHashMap<>();
        try {
            pst = con.prepareStatement("select dept.dname, avg(emp.sal) from company.emp join company.dept on emp.deptno = dept.deptno group by dept.dname order by avg(emp.sal) desc");
            rs = pst.executeQuery();
            while (rs.next()) {
                wagesInfo.put(rs.getString(1), rs.getInt(2));
            }
        } catch (SQLException e) {
            e.printStackTrace();
        }
        return wagesInfo;
    }

    /**
     * 根据工资由低而高获取每个员工的姓名，部门名，工资
     *
     * @return 获取数据保存在List中, Employee为保存员工信息的内部类
     */

    public List<Employee> getEmployeeInfo() {
        List<Employee> employeInfo = new LinkedList<>();
        try {
            // 这里踩了个坑,sal的字段是varchar,所以是根据第一个字母排序  要按照数字大小排序加个0
            pst = con.prepareStatement("select emp.ename, emp.sal, dept.dname from company.emp join company.dept on emp.deptno = dept.deptno order by emp.sal+0 asc");
            rs = pst.executeQuery();
            while (rs.next()) {
                employeInfo.add(new Employee(rs.getString(1), rs.getString(3), rs.getString(2)));
            }
        } catch (SQLException e) {

            e.printStackTrace();
        }
        return employeInfo;
    }

    /**
     * 对于工资高于本部门平均水平的员工，获取部门号，姓名，工资，按部门号排序
     *
     * @return 获取数据保存在List中, Employee为保存员工信息的内部类
     */
    public List<Employee> getCoreEmploye() {
        List<Employee> employeInfo = new LinkedList<>();
        try {
            pst = con.prepareStatement("select ename, deptno, sal from emp v where v.sal>(select AVG(c.sal) from emp c where c.deptno = v.deptno)");
            rs = pst.executeQuery();
            while (rs.next()) {
                employeInfo.add(new Employee(rs.getString(1), rs.getInt(2), rs.getString(3)));
            }
        } catch (SQLException e) {
            e.printStackTrace();
        }
        return employeInfo;
    }

    class Employee {
        String name;
        String dname;
        String sal;
        int id;

        public Employee(String name, String dname, String sal) {
            this.name = name;
            this.dname = dname;
            this.sal = sal;
        }

        public Employee(String name, int id, String sal) {
            this.name = name;
            this.id = id;
            this.sal = sal;
        }

        @Override
        public String toString() {
            if (id == 0) return name + "---" + sal + "---" + dname;
            else return name + "---" + sal + "---" + id;
        }
    }

    public static void main(String[] args) {
        Database database = new Database();
        //（1）列出各部门的最高工资，最低工资
        System.out.println("#（1）列出各部门的最高工资，最低工资");
        Map<String, List<Integer>> map1 = database.getMaxAndMinWages();
        for (String s : map1.keySet()) {
            System.out.println(s + ":" + map1.get(s));
        }
        // (2）按照各个部门的平均工资由高到低对，列出部门名，平均工资
        System.out.println("#（2）按照各个部门的平均工资由高到低对，列出部门名，平均工资");
        Map<String, Integer> map2 = database.getAvgWages();
        for (String s : map2.keySet()) {
            System.out.println(s + ":" + map2.get(s));
        }
        //（3）根据工资由低而高列出每个员工的姓名，部门名，工资
        System.out.println("#（3）根据工资由低而高列出每个员工的姓名，部门名，工资");
        List<Employee> list = database.getEmployeeInfo();
        for (Employee e : list) {
            System.out.println(e);
        }
        //（4）对于工资高于本部门平均水平的员工，列出部门号，姓名，工资，按部门号排序
        System.out.println("#（4）对于工资高于本部门平均水平的员工，列出部门号，姓名，工资，按部门号排序");
        list = database.getCoreEmploye();
        for (Employee e : list) {
            System.out.println(e);
        }
        //（5）创建表emp时，最后一行foreign key(deptno) references dept(deptno) 代表什么意思？
        System.out.println("#（5）创建表emp时，最后一行foreign key(deptno) references dept(deptno) 代表什么意思？");
        System.out.println("表emp中deptno作为外键与表dept建立联系,dept为主表,emp为次表,并且emp表中的deptno取值必须在与dept表中deptno已有");
    }
}
