import chengpeng.midproj.emulator.Attribute;
import chengpeng.midproj.emulator.Relation;
import chengpeng.midproj.emulator.Schema;
import chengpeng.midproj.emulator.Tuple;
import org.junit.jupiter.api.Test;

import java.util.ArrayList;
import java.util.List;

public class SchemaTest {

    @Test
    void test01() {

        Schema schema = Schema.getInstance("demo_case_schema");

        Relation orders;
        {
            List<Attribute> attributes = new ArrayList<>(6);
            attributes.add(new Attribute("ORD_NUM", Integer.class));
            attributes.add(new Attribute("ORD_AMOUNT", Integer.class));
            attributes.add(new Attribute("ADVANCE_AMOUNT", Integer.class));
            attributes.add(new Attribute("ORD_DATE", String.class));
            attributes.add(new Attribute("CUST_CODE", String.class));
            attributes.add(new Attribute("AGENT_CODE", String.class));
            orders = schema.create("ORDERS", attributes, attributes.get(0));
        }

        Relation customers;
        {
            List<Attribute> attributes = new ArrayList<>(6);
            attributes.add(new Attribute("CUST_CODE", String.class));
            attributes.add(new Attribute("CUST_NAME", String.class));
            attributes.add(new Attribute("CUST_CITY", String.class));
            attributes.add(new Attribute("CUST_COUNTRY", String.class));
            attributes.add(new Attribute("GRADE", Integer.class));
            attributes.add(new Attribute("BALANCE", Integer.class));
            customers = schema.create("CUSTOMERS", attributes, attributes.get(0));
        }

        Relation agents;
        {
            List<Attribute> attributes = new ArrayList<>(5);
            attributes.add(new Attribute("AGENT_CODE", String.class));
            attributes.add(new Attribute("AGENT_NAME", String.class));
            attributes.add(new Attribute("WORKING_AREA", String.class));
            attributes.add(new Attribute("COMMISSION_PER", Integer.class));
            attributes.add(new Attribute("PHONE_NO", Integer.class));
            agents = schema.create("AGENTS", attributes, attributes.get(0));
        }

        schema.addForeignKey(orders.getName(), "CUST_CODE", customers.getName());
        schema.addForeignKey(orders.getName(), "AGENT_CODE", agents.getName());

        agents.insert(makeAgentTuple("A001", "Hugo", "Paris", 14, 12346674));
        agents.insert(makeAgentTuple("A002", "Mukesh", "Mumbai", 11, 12358964));
        agents.insert(makeAgentTuple("A003", "Alex", "London", 13, 12458969));
        agents.insert(makeAgentTuple("A004", "Ivan", "Toronto", 15, 22544166));
        agents.insert(makeAgentTuple("A005", "Anderson", "Brisbane", 13, 21447739));
        agents.insert(makeAgentTuple("A006", "McDenny", "London", 15, 22255588));
        agents.insert(makeAgentTuple("A007", "Ramasundar", "Bangalore", 15, 25814763));
        agents.insert(makeAgentTuple("A008", "Alfred", "NewYork", 12, 25874365));
        agents.insert(makeAgentTuple("A009", "Benjamin", "Hampshire", 11, 22536178));
        agents.insert(makeAgentTuple("A010", "Sanchez", "Madrid", 14, 22388644));
        try {
            agents.insert(makeAgentTuple("A001", "Stevens", "Dublin", 15, 45625874));
        } catch (Exception exception) {
            System.err.println(exception.getMessage());
        }
        agents.insert(makeAgentTuple("A011", "Stevens", "Dublin", 15, 45625874));
        agents.insert(makeAgentTuple("A012", "Lucida", "San Jose", 12, 52981425));
        try {
            agents.insert(makeAgentTuple("A005", "Anderson", "Brisbane", 13, 21447739));
        } catch (Exception exception) {
            System.err.println(exception.getMessage());
        }
        agents.printTable("Agents table");

        customers.insert(makeCustomerTuple("C00014", "Victor", "Paris", "France", 2, 8000));
        customers.insert(makeCustomerTuple("C00005", "Sasikant", "Mumbai", "India", 1, 7000));
        customers.insert(makeCustomerTuple("C00009", "Ramesh", "Mumbai", "India", 3, 8000));
        customers.insert(makeCustomerTuple("C00022", "Avinash", "Mumbai", "India", 2, 7000));
        customers.insert(makeCustomerTuple("C00013", "Holmes", "London", "UK", 2, 6000));
        customers.insert(makeCustomerTuple("C00015", "Stuart", "London", "UK", 1, 6000));
        customers.insert(makeCustomerTuple("C00003", "Martin", "Toronto", "Canada", 2, 8000));
        customers.insert(makeCustomerTuple("C00006", "Shilton", "Toronto", "Canada", 1, 10000));
        customers.insert(makeCustomerTuple("C00008", "Karolina", "Toronto", "Canada", 1, 7000));
        customers.insert(makeCustomerTuple("C00004", "Winston", "Brisbane", "Australia", 1, 5000));
        customers.insert(makeCustomerTuple("C00018", "Fleming", "Brisbane", "Australia", 2, 7000));
        try {
            customers.insert(
                Tuple.builder()
                    .add("CUST_CODE", "C01011")
                    .add("CUST_NAME", "Salvador")
                    .add("CUST_CITY", "Madrid")
                    .add("CUST_COUNTRY", 0)
                    .add("GRADE", "Spain")
                    .add("BALANCE", 1000)
                    .build()
            );
        } catch (Exception exception) {
            System.err.println(exception.getMessage());
        }
        customers.insert(makeCustomerTuple("C00021", "Jacks", "Brisbane", "Australia", 1, 7000));
        customers.insert(makeCustomerTuple("C00023", "Karl", "London", "UK", 0, 4000));
        customers.insert(makeCustomerTuple("C00024", "Cook", "London", "UK", 2, 4000));
        customers.insert(makeCustomerTuple("C00016", "Venkatpati", "Bangalore", "India", 2, 8000));
        customers.insert(makeCustomerTuple("C00017", "Srinivas", "Bangalore", "India", 2, 8000));
        customers.insert(makeCustomerTuple("C00001", "Micheal", "NewYork", "USA", 2, 3000));
        customers.insert(makeCustomerTuple("C00002", "Bolt", "NewYork", "USA", 3, 5000));
        try {
            customers.insert(makeCustomerTuple("C00013", "Erin", "LosAngeles", "USA", 5, 7000));
        } catch (Exception exception) {
            System.err.println(exception.getMessage());
        }
        customers.insert(makeCustomerTuple("C00020", "Albert", "NewYork", "USA", 3, 5000));
        customers.insert(makeCustomerTuple("C00010", "Charles", "Hampshire", "UK", 3, 6000));
        customers.insert(makeCustomerTuple("C00007", "Oscar", "Madrid", "Spain", 1, 7000));
        customers.insert(makeCustomerTuple("C00011", "Sergio", "Madrid", "Spain", 3, 7000));
        customers.insert(makeCustomerTuple("C00019", "Alberto", "Madrid", "Spain", 1, 8000));
        try {
            customers.insert(makeCustomerTuple("C00011", "Tara", "London", "UK", 2, 1000));
        } catch (Exception exception) {
            System.err.println(exception.getMessage());
        }
        customers.insert(makeCustomerTuple("C00025", "Gary", "Dublin", "Ireland", 2, 5000));
        customers.insert(makeCustomerTuple("C00012", "Steven", "SanJose", "USA", 1, 5000));
        customers.printTable("Customers table");

        orders.insert(makeOrderTuple(200117, 800, 200, "10/20/2008", "C00014", "A001"));
        orders.insert(makeOrderTuple(200106, 2500, 700, "04/20/2008", "C00005", "A002"));
        orders.insert(makeOrderTuple(200113, 4000, 600, "06/10/2008", "C00022", "A002"));
        orders.insert(makeOrderTuple(200120, 500, 100, "07/20/2008", "C00009", "A002"));
        orders.insert(makeOrderTuple(200123, 500, 100, "09/16/2008", "C00022", "A002"));
        orders.insert(makeOrderTuple(200126, 500, 100, "06/24/2008", "C00022", "A002"));
        orders.insert(makeOrderTuple(200128, 3500, 1500, "07/20/2008", "C00009", "A002"));
        orders.insert(makeOrderTuple(200133, 1200, 400, "06/29/2008", "C00009", "A002"));
        try {
            orders.insert(makeOrderTuple(200117, 1200, 400, "06/29/2008", "C00009", "A002"));
        } catch (Exception exception) {
            System.err.println(exception.getMessage());
        }
        orders.insert(makeOrderTuple(200127, 2500, 400, "07/20/2008", "C00015", "A003"));
        orders.insert(makeOrderTuple(200104, 1500, 500, "03/13/2008", "C00006", "A004"));
        orders.insert(makeOrderTuple(200108, 4000, 600, "02/15/2008", "C00008", "A004"));
        orders.insert(makeOrderTuple(200121, 1500, 600, "09/23/2008", "C00008", "A004"));
        orders.insert(makeOrderTuple(200122, 2500, 400, "09/16/2008", "C00003", "A004"));
        orders.insert(makeOrderTuple(200222, 2500, 400, "09/16/2008", "C00004", "A004"));//
        orders.insert(makeOrderTuple(200103, 1500, 700, "05/15/2008", "C00021", "A005"));
        orders.insert(makeOrderTuple(200125, 2000, 600, "10/10/2008", "C00018", "A005"));
        orders.insert(makeOrderTuple(200134, 4200, 1800, "09/25/2008", "C00004", "A005"));
        try {
            orders.insert(makeOrderTuple(200136, 4200, 1800, "09/25/2008", "C40004", "A005"));
        } catch (Exception exception) {
            System.err.println(exception.getMessage());
        }
        orders.insert(makeOrderTuple(200118, 500, 100, "07/20/2008", "C00023", "A006"));
        orders.insert(makeOrderTuple(200129, 2500, 500, "07/20/2008", "C00024", "A006"));
        orders.insert(makeOrderTuple(200112, 2000, 400, "05/30/2008", "C00016", "A007"));
        orders.insert(makeOrderTuple(200124, 500, 100, "06/20/2008", "C00017", "A007"));
        orders.insert(makeOrderTuple(200101, 3000, 1000, "07/15/2008", "C00001", "A008"));
        orders.insert(makeOrderTuple(200111, 1000, 300, "07/10/2008", "C00020", "A008"));
        orders.insert(makeOrderTuple(200114, 3500, 2000, "08/15/2008", "C00002", "A008"));
        orders.insert(makeOrderTuple(200116, 500, 100, "07/13/2008", "C00010", "A009"));
        orders.insert(makeOrderTuple(200107, 4500, 900, "08/30/2008", "C00007", "A010"));
        orders.insert(makeOrderTuple(200109, 3500, 800, "07/30/2008", "C00011", "A010"));
        orders.insert(makeOrderTuple(200110, 3000, 500, "04/15/2008", "C00019", "A010"));
        orders.insert(makeOrderTuple(200119, 4000, 700, "09/16/2008", "C00007", "A010"));
        orders.insert(makeOrderTuple(200135, 2000, 800, "09/16/2008", "C00007", "A010"));
        orders.insert(makeOrderTuple(200105, 2500, 500, "07/18/2008", "C00025", "A011"));
        orders.insert(makeOrderTuple(200130, 2500, 400, "07/30/2008", "C00025", "A011"));
        orders.insert(makeOrderTuple(200102, 2000, 300, "05/25/2008", "C00012", "A012"));
        orders.insert(makeOrderTuple(200131, 900, 150, "08/26/2008", "C00012", "A012"));
        try {
            orders.insert(makeOrderTuple(200137, 200, 800, "09/16/2008", "C00007", "A110"));
        } catch (Exception exception) {
            System.err.println(exception.getMessage());
        }
        orders.printTable("Orders table");

        orders
            .selection("ORD_AMOUNT", "=4000")
            .naturalJoin(agents)
            .naturalJoin(customers)
            .projection("CUST_NAME", "AGENT_NAME")
            .printTable("Demo 0 - Retrieve the names of all UK customers, who have an order greater than $4,000, together with the agent names");

        customers
            .projection("CUST_NAME")
            .printTable("Demo 1 - Retrieve the names of all customers");

        agents
            .selection("WORKING_AREA", "Bangalore")
            .projection("AGENT_NAME", "PHONE_NO")
            .printTable("Demo 2 - Retrieve the names and phone numbers of all agents in Bangalore.");

        orders
            .naturalJoin(customers)
            .selection("CUST_COUNTRY", "USA")
            .projection(orders.getAllColumnsName())
            .printTable("Demo 3 - Retrieve the orders of all customers who are from the USA.");

        Relation leftJoin = orders.naturalJoin(customers).leftJoin(agents, "AGENT_CODE");
        Relation groupingCount1 = leftJoin.groupingCount("AGENT_NAME");
        Relation groupingCount2 = leftJoin.groupingSum("AGENT_NAME", "ORD_AMOUNT");
        agents
            .naturalJoin(groupingCount1)
            .naturalJoin(groupingCount2)
            .projection("AGENT_NAME", "PHONE_NO", "group_count_AGENT_NAME", "group_by_AGENT_NAME_sum_ORD_AMOUNT")
            .printTable("Demo 4 - Retrieve the total number of customers and total order amount (ORD_AMOUNT) for each agent. List names and phone numbers for agents.")
            .sort("group_count_AGENT_NAME", true)
            .printTable("Demo 4 - Extra function order by count(AGENT_NAME) desc")
            .sort("group_by_AGENT_NAME_sum_ORD_AMOUNT", true)
            .printTable("Demo 4 - Extra function order by sum(ORD_AMOUNT) desc")
            .sort("AGENT_NAME", false)
            .printTable("Demo 5 - Extra function order by ANSI ASC");

        // please see test05 and test 06 for insert delete and update

    }

    private static Tuple makeAgentTuple(String agentCode, String agentName, String workingArea, Integer commissionPer, Integer phoneNo) {
        return Tuple.builder()
                   .add("AGENT_CODE", agentCode)
                   .add("AGENT_NAME", agentName)
                   .add("WORKING_AREA", workingArea)
                   .add("COMMISSION_PER", commissionPer)
                   .add("PHONE_NO", phoneNo)
                   .build();
    }

    private static Tuple makeCustomerTuple(String custCode, String custName, String custCity, String custCountry, Integer grade, Integer balance) {
        return Tuple.builder()
                   .add("CUST_CODE", custCode)
                   .add("CUST_NAME", custName)
                   .add("CUST_CITY", custCity)
                   .add("CUST_COUNTRY", custCountry)
                   .add("GRADE", grade)
                   .add("BALANCE", balance)
                   .build();
    }

    private static Tuple makeOrderTuple(Integer ordNum, Integer ordAmount, Integer advanceAmount, String ordDate, String custCode, String agentCode) {
        return Tuple.builder()
                   .add("ORD_NUM", ordNum)
                   .add("ORD_AMOUNT", ordAmount)
                   .add("ADVANCE_AMOUNT", advanceAmount)
                   .add("ORD_DATE", ordDate)
                   .add("CUST_CODE", custCode)
                   .add("AGENT_CODE", agentCode)
                   .build();
    }

    @Test
    void test02() {

        Schema schema = Schema.getInstance("schema_01");

        List<Attribute> attributes = new ArrayList<>(2);

        Attribute id;
        attributes.add(id = new Attribute("id", Integer.class));
        attributes.add(new Attribute("name", String.class));

        Relation people1 = schema.create("people1", attributes, id);
        people1.insert(Tuple.builder().add("id", 1).add("name", "John").build());
        people1.insert(Tuple.builder().add("id", 2).add("name", "Nick").build());
        people1.insert(Tuple.builder().add("id", 3).add("name", "Alic").build());

        Relation people2 = schema.create("people2", attributes, id);
        people2.insert(Tuple.builder().add("id", 1).add("name", "John").build());
        people2.insert(Tuple.builder().add("id", 2).add("name", "Nick").build());
        people2.insert(Tuple.builder().add("id", 3).add("name", "Jose").build());

        people1.printTable("Table 1");
        people1.printTable("Table 2");

        people1.union(people2).printTable("Union - I wont check primary duplication in this situation");
        people1.intersection(people2).printTable("Intersection");
        people1.difference1(people2).printTable("Difference implement 1 - By loop");
        people1.difference2(people2).printTable("Difference implement 2 - By Union remove Intersection");
    }

    @Test
    void test03() {

        Schema schema = Schema.getInstance("schema_02");

        List<Attribute> attributes = new ArrayList<>(1);
        Attribute number = new Attribute("number", Integer.class);
        attributes.add(number);
        attributes.add(new Attribute("group", Integer.class));

        Relation relation = schema.create("aggregate", attributes, number);
        relation.insert(Tuple.builder().add("number", 11).add("group", 1).build());
        relation.insert(Tuple.builder().add("number", 12).add("group", 1).build());
        relation.insert(Tuple.builder().add("number", 13).add("group", 1).build());
        relation.insert(Tuple.builder().add("number", 14).add("group", 1).build());
        relation.insert(Tuple.builder().add("number", 15).add("group", 1).build());
        relation.insert(Tuple.builder().add("number", 21).add("group", 2).build());
        relation.insert(Tuple.builder().add("number", 22).add("group", 2).build());
        relation.insert(Tuple.builder().add("number", 23).add("group", 2).build());
        relation.insert(Tuple.builder().add("number", 24).add("group", 2).build());
        relation.insert(Tuple.builder().add("number", 25).add("group", 2).build());

        relation.printTable("Table of number");

        relation.min("number").printTable("Min");
        relation.max("number").printTable("Max");
        relation.average("number").printTable("Average");
        relation.sum("number").printTable("Sum");
        relation.count("number").printTable("Count");
        relation.groupingCount("group").printTable("Group count");
        relation.groupingMin("group", "number").printTable("Grouping and min");
        relation.groupingMax("group", "number").printTable("Grouping and max");
        relation.groupingSum("group", "number").printTable("Grouping and sum");
        relation.groupingAverage("group", "number").printTable("Grouping and average");
    }

    @Test
    void test04() {

        Schema schema = Schema.getInstance("schema_03");

        Relation people;
        {
            List<Attribute> attributes = new ArrayList<>(3);
            Attribute id;
            attributes.add(id = new Attribute("name", String.class));
            attributes.add(new Attribute("phone", String.class));
            attributes.add(new Attribute("company_id", Integer.class));
            people = schema.create("people", attributes, id);
        }

        Relation company;
        {
            List<Attribute> attributes = new ArrayList<>(3);
            Attribute id;
            attributes.add(id = new Attribute("company_id", Integer.class));
            attributes.add(new Attribute("address", String.class));
            attributes.add(new Attribute("company_name", String.class));
            company = schema.create("company", attributes, id);
        }

        people.insert(Tuple.builder().add("name", "John").add("phone", "111-111-1111").add("company_id", 1).build());
        people.insert(Tuple.builder().add("name", "Alic").add("phone", "222-222-2222").add("company_id", 2).build());
        people.insert(Tuple.builder().add("name", "Nick").add("phone", "333-333-3333").add("company_id", 2).build());
        people.insert(Tuple.builder().add("name", "Jobs").add("phone", "444-444-4444").add("company_id", 2).build());
        people.insert(Tuple.builder().add("name", "Eddy").add("phone", "555-555-5555").add("company_id", new Relation.NullValue()).build());
        people.printTable("Table of people - Eddy has no job so insert null");

        company.insert(Tuple.builder().add("company_name", "Cudis RD Inc.").add("address", "Los Ang US.").add("company_id", 1).build());
        company.insert(Tuple.builder().add("company_name", "Atlas IT Inc.").add("address", "Atlanta US.").add("company_id", 2).build());
        company.printTable("Table of company");

        people.equiJoin(company, "company_id").printTable("Equals Join");
        people.leftJoin(company, "company_id").printTable("Left Join");
        people.cross(company).printTable("Cross Join");
    }

    @Test
    void test05() {

        Schema schema = Schema.getInstance("schema_04");

        Relation r1;
        {
            List<Attribute> attributes = new ArrayList<>();
            Attribute id;
            attributes.add(id = new Attribute("id1", String.class));
            attributes.add(new Attribute("fk1", String.class));
            r1 = schema.create("r1", attributes, id);
        }

        Relation r2;
        {
            List<Attribute> attributes = new ArrayList<>();
            Attribute id;
            attributes.add(id = new Attribute("id2", String.class));
            attributes.add(new Attribute("value", Integer.class));
            r2 = schema.create("r2", attributes, id);
        }

        Relation r3;
        {
            List<Attribute> attributes = new ArrayList<>();
            Attribute id;
            attributes.add(id = new Attribute("id3", String.class));
            r3 = schema.create("r3", attributes, id);
        }

        Relation r4;
        {
            List<Attribute> attributes = new ArrayList<>();
            Attribute id;
            attributes.add(id = new Attribute("id4", String.class));
            attributes.add(new Attribute("fk4", String.class));
            r4 = schema.create("r4", attributes, id);
        }

        r1.insert(Tuple.builder().add("id1", "1").add("fk1", "2").build());
        r2.insert(Tuple.builder().add("id2", "2").add("value", 1).build());
        r3.insert(Tuple.builder().add("id3", "2").build());
        r3.insert(Tuple.builder().add("id3", "4").build());
        r4.insert(Tuple.builder().add("id4", "1").add("fk4", "2").build());
        try {
            r1.insert(Tuple.builder().add("id1", "2").build());
        } catch (Exception exception) {
            System.err.println("Mismatch tuple: " + exception.getMessage());
        }
        try {
            r1.insert(Tuple.builder().add("id1", "2").add("fk1", 2).build());
        } catch (Exception exception) {
            System.err.println("Mismatch domain: " + exception.getMessage());
        }

        schema.addForeignKey("r1", "fk1", "r2");
        schema.addForeignKey("r2", "id2", "r3");
        schema.addForeignKey("r4", "fk4", "r3");

        r1.printTable("Table");
        r2.printTable("Table");
        r3.printTable("Table");
        r4.printTable("Table");

        r2.update("id2", "3", "value", "=1");

        r1.printTable("Cascade update");
        r2.printTable("Cascade update");
        r3.printTable("Cascade update");
        r4.printTable("Cascade update");

        try {
            r2.update("id2", "4", "value", "=1");
        } catch (Exception exception) {
            System.err.println(exception.getMessage());
        }

        try {
            r2.delete("id2", "3");
        } catch (Exception exception) {
            System.err.println("Delete when using by foreign: " + exception.getMessage());
        }

    }

    @Test
    void test06() {

        Schema schema = Schema.getInstance("schema_05");

        Relation r1;
        {
            List<Attribute> attributes = new ArrayList<>();
            Attribute id;
            attributes.add(id = new Attribute("id1", String.class));
            r1 = schema.create("r1", attributes, id);
        }

        Relation r2;
        {
            List<Attribute> attributes = new ArrayList<>();
            Attribute id;
            attributes.add(id = new Attribute("id2", String.class));
            r2 = schema.create("r2", attributes, id);
        }

        Relation r3;
        {
            List<Attribute> attributes = new ArrayList<>();
            Attribute id;
            attributes.add(id = new Attribute("id3", String.class));
            r3 = schema.create("r3", attributes, id);
        }

        r1.insert(Tuple.builder().add("id1", "1").build());
        r2.insert(Tuple.builder().add("id2", "1").build());
        r3.insert(Tuple.builder().add("id3", "1").build());

        schema.addForeignKey("r1", "id1", "r2");
        schema.addForeignKey("r2", "id2", "r3");
        schema.addForeignKey("r3", "id3", "r1");

        r1.printTable("Table");
        r2.printTable("Table");
        r3.printTable("Table");

        r1.update("id1", "2", "id1", "1");

        r1.printTable("Jormungand relation update");
        r2.printTable("Jormungand relation update");
        r3.printTable("Jormungand relation update");

    }
}