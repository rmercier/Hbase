package mercierDaoud.socialNetwork;

import java.util.Scanner;
import java.io.ByteArrayInputStream;
import java.io.DataInputStream;
import java.io.IOException;
import java.io.InterruptedIOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
 
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.MasterNotRunningException;
import org.apache.hadoop.hbase.ZooKeeperConnectionException;
import org.apache.hadoop.hbase.client.Delete;
import org.apache.hadoop.hbase.client.Get;
import org.apache.hadoop.hbase.client.HBaseAdmin;
import org.apache.hadoop.hbase.client.HTable;
import org.apache.hadoop.hbase.client.Result;
import org.apache.hadoop.hbase.client.RetriesExhaustedWithDetailsException;
import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.hadoop.io.ArrayWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.Writable;
import org.apache.hadoop.io.WritableUtils;


public class App 
{
	private static Configuration conf = null;
	private static HTable table = null; 
	private static HBaseAdmin admin = null;

    static {
        conf = HBaseConfiguration.create();
    }
    
    //connection to the table 
    public static void connectTable(String tableName) 
    {	
    	conf = HBaseConfiguration.create();
    	try {
			admin = new HBaseAdmin(conf);
			table = new HTable(conf, tableName);
		} catch (MasterNotRunningException e1) {
			// TODO Auto-generated catch block
			e1.printStackTrace();
		} catch (ZooKeeperConnectionException e1) {
			// TODO Auto-generated catch block
			e1.printStackTrace();
		} catch (IOException e1) {
			// TODO Auto-generated catch block
			e1.printStackTrace();
		}
    	
    }
	
    //Once connected, launch general loop
    public static void connectedToTable()  {
    	Scanner scanner = new Scanner(System.in);
		int choice = 0;

		boolean exit = false;
		System.out.println("You are connected to the DB");

		do {
	        System.out.println("-------------------------\n");
	        System.out.println("1 - Create User");
	        System.out.println("2 - Search User");
	        System.out.println("3 - Previous\n");
	        System.out.println("-------------------------\n");
	        
			System.out.print("Enter your choice: ");
			choice = scanner.nextInt();
			scanner.nextLine();
			
			switch (choice) {
			case 1:
		        System.out.println("\n-- Create User");
		        createUser();
				System.out.println("\n");
				break;

			case 2:
				System.out.println("\n-- Show User");
				searchUser();
				System.out.println("\n");
				break;
				
			case 3:
				exit = true;
				break;
			}
		} while (!exit);
		
    }
    
    //Adds a row to hbase db
    private static void createUser(){
    	
    	Scanner scanner = new Scanner(System.in);
		System.out.println("Enter the firstname");
    	String firstName = scanner.nextLine().toLowerCase();
    	Get g = new Get(Bytes.toBytes(firstName));
    	Result r = null;
		try {
			r = table.get(g);
		} catch (IOException e1) {
			e1.printStackTrace();
		}
		
    	if(!r.isEmpty()) {
    		System.out.println("User already exists");
    		return ;
    	}
    		
    	
    	Put p = new Put(Bytes.toBytes(firstName));
    	System.out.println("*** Classical Infos");
    	System.out.print("Enter the age: ");
    	int age = scanner.nextInt();
    	
    	
    	p.add(Bytes.toBytes("infos"), Bytes.toBytes("age"), Bytes.toBytes(age));

    	scanner.nextLine();

    	
    	System.out.print("Enter the sex (M/F): ");
    	String sex = scanner.nextLine();
    	
    	p.add(Bytes.toBytes("infos"), Bytes.toBytes("sex"), Bytes.toBytes(sex));
    	
    	System.out.print("Enter the lastname: ");
    	String lastname = scanner.nextLine().toLowerCase();
    	
    	p.add(Bytes.toBytes("infos"), Bytes.toBytes("lastname"), Bytes.toBytes(lastname));

    	System.out.println("*** Friends Infos");
    	
    	System.out.print("Enter the BFF firstname: ");
    	String bff = scanner.nextLine().toLowerCase();
    	p.add(Bytes.toBytes("friends"), Bytes.toBytes("bff"), Bytes.toBytes(bff));
    	
    	System.out.println("Enter your list of friends separeted by commas (','):");
    	String friendsList = scanner.nextLine();
    	String[] list = friendsList.toLowerCase().trim().split(",");
    	List<String> arrList = Arrays.<String>asList(list);
    	ArrayList<String> arrayList = new ArrayList<String>(arrList);
    	p.add(Bytes.toBytes("friends"), Bytes.toBytes("list"), WritableUtils.toByteArray(toWritable(arrayList)));
    	
    	
    	try {
			table.put(p);
		} catch (RetriesExhaustedWithDetailsException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		} catch (InterruptedIOException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
    }

    //Shows info about user searched by firstname 
    private static void searchUser() {
    	
    	Scanner scanner = new Scanner(System.in);
		System.out.print("Enter the firstname: ");
    	String firstName = scanner.nextLine();
    	Result r = null;
    	int choice = 0;
		Get g = new Get(Bytes.toBytes(firstName));
		
		try {
			r = table.get(g);
		} catch (IOException e) {
			e.printStackTrace();
		}
		
		if(r.isEmpty()) {
    		System.out.println("User doesn't exist");
    		return ;
    	}
		
		showUser(r);
		
		System.out.println("\n");
		System.out.println("Action possible");
        System.out.println("-------------------------\n");
        System.out.println("1 - Modify User");
        System.out.println("2 - Delete User");
        System.out.println("3 - Previous\n");
        System.out.println("-------------------------\n");


		System.out.println("Enter your choice: ");
		choice = scanner.nextInt();
		scanner.nextLine();
		switch (choice) {
		case 1:
	        System.out.println("\n-- Modify ");
	        modifyUser(firstName);
			System.out.println("\n");
			break;

		case 2:
			System.out.println("\n-- Delete ");
			deleteUser(firstName);
			System.out.println("\n");
			break;
		}
		
		
    }
    
    //Modifies an attribute of a user
    private static void modifyUser(String firstname) {
    	
    	Scanner scanner = new Scanner(System.in);
    	int choice = 0;
    	
    	Put p = new Put(Bytes.toBytes(firstname));
    	
    	System.out.println("Modify User");
        System.out.println("-------------------------\n");
        System.out.println("1 - Age");
        System.out.println("2 - Sex");
        System.out.println("3 - Lastname");
        System.out.println("4 - BFF");
        System.out.println("5 - Previous\n");
        System.out.println("-------------------------\n");


		System.out.print("Enter your choice: ");
		choice = scanner.nextInt();
		scanner.nextLine();
		
		switch (choice) {
		case 1:
			System.out.print("Enter the age: ");
	    	int age = scanner.nextInt();
	    	scanner.nextLine();
	    	p.add(Bytes.toBytes("infos"), Bytes.toBytes("age"), Bytes.toBytes(age));
			break;

		case 2:
			System.out.print("Enter the sex (M/F): ");
	    	String sex = scanner.nextLine();
	    	p.add(Bytes.toBytes("infos"), Bytes.toBytes("sex"), Bytes.toBytes(sex));
			break;
		case 3:
			System.out.print("Enter the lastname : ");
	    	String lastname = scanner.nextLine();
	    	p.add(Bytes.toBytes("infos"), Bytes.toBytes("lastname"), Bytes.toBytes(lastname));
			break;
		case 4:
			System.out.print("Enter the BFF : ");
	    	String bff = scanner.nextLine();
	    	p.add(Bytes.toBytes("friends"), Bytes.toBytes("bff"), Bytes.toBytes(bff));
			break;
		case 5:
			break;
		}
		
		
    	
    	try {
			table.put(p);
		} catch (RetriesExhaustedWithDetailsException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		} catch (InterruptedIOException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
    }
    
    //Deletes a user 
    private static void deleteUser(String firstname) {
    	Delete d = new Delete(Bytes.toBytes(firstname));
			try {
				table.delete(d);
			} catch (IOException e) {
				// TODO Auto-generated catch block
				e.printStackTrace();
			}
		
    }
    
    //Prints attributes of a user
    private static void showUser(Result r) {
    	byte[] ageBytes = r.getValue(Bytes.toBytes("infos"), Bytes.toBytes("age"));
		byte[] sexBytes = r.getValue(Bytes.toBytes("infos"), Bytes.toBytes("sex"));
		byte[] lastnameBytes = r.getValue(Bytes.toBytes("infos"), Bytes.toBytes("lastname"));
		
		int age = Bytes.toInt(ageBytes);
		String sex = Bytes.toString(sexBytes);
		String lastname = Bytes.toString(lastnameBytes);
		
		System.out.println("*** Classical infos");
		System.out.println("Lastname : " + lastname);
		System.out.println("Age : " + age);
		System.out.println("Sex : " + sex);
		  
		System.out.println("*** Friends infos");
		byte[] bffBytes = r.getValue(Bytes.toBytes("friends"), Bytes.toBytes("bff"));
		String bff = Bytes.toString(bffBytes);
		
		System.out.println("BFF : " + bff);
		
		byte[] listBytes = r.getValue(Bytes.toBytes("friends"), Bytes.toBytes("list"));
		ArrayWritable w = new ArrayWritable(Text.class);
        try {
			w.readFields(
			        new DataInputStream(
			                new ByteArrayInputStream(
			                        r.getValue(Bytes.toBytes("friends"), Bytes.toBytes("list"))
			                )
			        )
			);
		} catch (IOException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
        ArrayList<String> listOfFriends = fromWritable(w);
		System.out.println("List Of Friend : ");
		for(String f : listOfFriends) {
			System.out.println("\t" + f);
		}
    }
    
    //Serialize array to bytes
    public static Writable toWritable(ArrayList<String> list) {
        Writable[] content = new Writable[list.size()];
        for (int i = 0; i < content.length; i++) {
            content[i] = new Text(list.get(i));
        }
        return new ArrayWritable(Text.class, content);
    }
    
    //Unserialize array from bytes 
    public static ArrayList<String> fromWritable(ArrayWritable writable) {
        Writable[] writables = ((ArrayWritable) writable).get();
        ArrayList<String> list = new ArrayList<String>(writables.length);
        for (Writable wrt : writables) {
            list.add(((Text)wrt).toString());
        }
        return list;
    }
    
    
    public static void main( String[] args )
    {
    	Scanner scanner = new Scanner(System.in);
		int choice = 0;

		boolean exit = false;
		System.out.println("##########################");
		System.out.println("##### Social Network #####");
		System.out.println("##########################\n\n");

		do {
			System.out.println("Choose from these choices");
	        System.out.println("-------------------------\n");
	        System.out.println("1 - Connect The Table");
	        System.out.println("2 - Quit");
	        System.out.println("\n-------------------------\n");

			System.out.print("Enter your choice: ");
			choice = scanner.nextInt();
			scanner.nextLine();
			
			
			switch (choice) {
			case 1:
		        System.out.println("-- Create The Table");
		        connectTable("ndaoud");
		        try {
					Boolean bool = admin.isTableDisabled("ndaoud");
					if(!bool)
					{
						System.out.println("> Successfully connected!");
						connectedToTable();
					} else {
						System.out.println("> Connection Error !");
						break;
					}
				} catch (IOException e) {
					// TODO Auto-generated catch block
					e.printStackTrace();
				}
				System.out.println("\n");
				break;

			case 2:
				exit = true;
				break;
			}
		} while (!exit);
		
		scanner.close();
    }
    
  
}
