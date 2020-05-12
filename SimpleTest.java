package corona;

import corona.business.Employee;
import corona.business.Lab;
import corona.business.ReturnValue;
import org.junit.Test;

import static corona.business.ReturnValue.OK;
import static org.junit.Assert.assertEquals;

public class SimpleTest extends AbstractTest{

    @Test
    public void simpleTestCreateLab()
    {
        Lab a = new Lab();
        a.setId(1);
        a.setName("Technion");
        a.setCity("Haifa");
        a.setIsActive(true);
        ReturnValue ret = Solution.addLab(a);
        assertEquals(OK, ret);
    }

    @Test
    public void testDeleteLab() {
        Lab a = new Lab();
        a.setId(3);
        a.setName("aaa");
        a.setCity("Esshhhhhhhhhhhhhh");
        a.setIsActive(true);

        Employee e = new Employee();
        e.setId(1);
        e.setName("Fattttt");
        e.setCity("asdasdasd");

        Employee e2 = new Employee();
        e2.setId(2);
        e2.setName("Fattttt2");
        e2.setCity("aaaaaaaa");

        Employee e3 = new Employee();
        e3.setId(3);
        e3.setName("Fattttt3");
        e3.setCity("bbbbbbbbbbbb");

        ReturnValue f = Solution.addLab(a);
        Solution.addEmployee(e);
        Solution.addEmployee(e2);
        Solution.deleteLab(a);
        Solution.addEmployee(e3);
        Solution.employeeJoinLab(e.getId(), a.getId(), 5);
        Solution.employeeJoinLab(e2.getId(), a.getId(), 6);
        Lab b = Solution.getLabProfile(3);
        Integer x = Solution.getTotalWages(10);
        Solution.employeeLeftLab(3, 7);
        ReturnValue ret = Solution.deleteLab(a);
        // assertEquals(NOT_EXISTS , ret);
        Solution.dropTables();
    }
}