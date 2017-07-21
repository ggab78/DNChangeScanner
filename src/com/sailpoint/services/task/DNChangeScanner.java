package com.sailpoint.services.task;

import java.io.PrintWriter;
import java.io.StringWriter;
import java.sql.Connection;
import java.sql.DatabaseMetaData;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.Statement;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.Callable;
import java.util.concurrent.CompletionService;
import java.util.concurrent.ExecutorCompletionService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.ThreadPoolExecutor;

import javax.naming.ldap.LdapName;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;

import sailpoint.api.SailPointContext;
import sailpoint.api.SailPointFactory;
import sailpoint.object.Application;
import sailpoint.object.Attributes;
import sailpoint.object.Filter;
import sailpoint.object.Link;
import sailpoint.object.ManagedAttribute;
import sailpoint.object.QueryOptions;
import sailpoint.object.Schema;
import sailpoint.object.TaskResult;
import sailpoint.object.TaskSchedule;
import sailpoint.object.TaskResult.CompletionStatus;
import sailpoint.task.AbstractTaskExecutor;
import sailpoint.tools.GeneralException;
import sailpoint.tools.Util;

public class DNChangeScanner extends AbstractTaskExecutor {
	
	public static String TYPE_ACCOUNT = "Account";
	public static String TYPE_GROUP = "Group";
	private int pruneAccountCount = 0;
	private int pruneGroupCount = 0;
	private String applicationID = null;
	private String applicationName = null;
	private Map<String, Integer> resultMap = new HashMap<String, Integer>();
	
	private Log logger = LogFactory.getLog(DNChangeScanner.class);
	
	boolean terminated = false;
	private SailPointContext ctx = null;

	@Override
	public void execute(SailPointContext context, TaskSchedule tasksch,
			TaskResult taskResult, Attributes<String, Object> args) throws Exception {
		
		if(logger.isDebugEnabled()) logger.debug("Starting DNChangeScanner execute : ");
		
		this.ctx = context;
		
		List<String> applicationList = null;
		List<String> uuidOverride = null;
		
		Iterator<Object[]> accountsIt = null;
		Iterator<Object[]> groupsIt = null;
		
		//Write Debug what args we got
		
		if(logger.isDebugEnabled()){
			List<String> keys = args.getKeys();
			
			for(String key : keys){
				logger.debug(" args key : " +key +" Value : " +args.get(key));
			}
			
		}
		
		int numberOfThreads;
		try{
			numberOfThreads=Integer.parseInt(args.get("numberThreads").toString());
			if(logger.isDebugEnabled())logger.debug("numberThreads from GUI: "+numberOfThreads);
		}catch(Exception e){
			numberOfThreads=5;
			if(logger.isDebugEnabled())logger.debug("No numberThreads defined in GUI, default to 5");
		}
		
		ThreadPoolExecutor threadPool = (ThreadPoolExecutor) Executors.newFixedThreadPool(numberOfThreads);
		CompletionService executorCreate = new ExecutorCompletionService(threadPool);
		Set<Future<String>> futuresCreate = new HashSet<Future<String>>();
		
		//if Database table custom_immutable does not exist then create it
		createTableIfNotExist();
		
		//check if immutableid is manually defined
		if(args.containsKey("uuidOverride")){
			logger.debug("uuidOverride : " +args.getString("uuidOverride"));
			uuidOverride = Util.csvToList(args.getString("uuidOverride").replace("\r\n", ",").replace("\n", ","), true);
		}
		
		if(args.containsKey("applications")){
			applicationList = Util.csvToList(args.getString("applications"), true);
			applicationList = validateApplications(applicationList,uuidOverride);
		} else {
			throw new Exception("applications cannot be null");
		}
		
		
		/* Might try to preserver group attributes as they are lost on a rename
		List grpAttributesToPreserver = new ArrayList();
		
		if(args.containsKey("groupAttributesToPreserve")){
			grpAttributesToPreserver = Util.csvToList((String) args.get("groupAttributesToPreserve"), true);
		}
		*/


		try{
			//Iterate all applications
			for(String applicationID : applicationList){

				if(logger.isDebugEnabled()) logger.debug("Starting Processing : " +applicationID );
				
				setApplicationDetails(applicationID);

				//Process Accounts if needed
				if(args.containsKey("processAccounts") && "true".equals(args.getString("processAccounts")) ){
					//Get Iterator of Accounts
					accountsIt = getAccountsIterator(applicationID, uuidOverride);
					
					List<String> immutableIds = new ArrayList<String>();

					while(accountsIt.hasNext() && !terminated){
						Object[] accountEntry = accountsIt.next();
						immutableIds.add((String) accountEntry[1]);
						futuresCreate.add(executorCreate.submit(new ScanEntry(accountEntry, TYPE_ACCOUNT, applicationID, args, logger)));
					}
					
					if(!terminated){
						boolean processDelete = false;
						if(args.containsKey("processDeleteAccounts") && "true".equals(args.getString("processDeleteAccounts"))){
							processDelete=true;
						}
						pruneCustomImmutable(immutableIds, TYPE_ACCOUNT, applicationID, processDelete);
					}
				}
				
				//Process Groups if needed
				if(args.containsKey("processGroups") && "true".equals(args.getString("processGroups"))){
					//Get Iterator of Groups
					groupsIt = getGroupsIterator(applicationID, uuidOverride);
					
					List<String> immutableIds = new ArrayList<String>();

					while(groupsIt.hasNext() && !terminated){
						Object[] groupEntry = groupsIt.next();
						immutableIds.add((String) groupEntry[1]);
						futuresCreate.add(executorCreate.submit(new ScanEntry(groupEntry, TYPE_GROUP, applicationID, args,  logger)));
					}
					
					if(!terminated){
						boolean processDelete = false;
						if(args.containsKey("processDeleteGroups") && "true".equals(args.getString("processDeleteGroups"))){
							processDelete=true;
						}
						pruneCustomImmutable(immutableIds, TYPE_GROUP, applicationID, processDelete);
					}
				}
				pruneCustomImmutable(applicationList);
			}
			
			
			int counter;
			
			while(futuresCreate.size()>0){
				
				String exitValue="";

				Future completedFuture = executorCreate.take();

				futuresCreate.remove(completedFuture);

	
				exitValue = (String) completedFuture.get();
				


				if(null != exitValue){
					if(exitValue!="" && resultMap.containsKey(exitValue)){
						counter = resultMap.get(exitValue);
						resultMap.put(exitValue, ++counter);
					}else if(exitValue!="" && !resultMap.containsKey(exitValue)){
						resultMap.put(exitValue, 1);
					}
				}
			}

			logger.trace("resultMap: "+resultMap.toString());

			threadPool.shutdown();

			//return result
			//if there is no result or the result is "" because WF has been launched already
			if(resultMap.isEmpty()){
				taskResult.addMessage("No Entries was processed");
			}else{
				for (Map.Entry<String, Integer> entry : resultMap.entrySet()){
					taskResult.addMessage(entry.getKey()+" : "+entry.getValue());
					logger.trace("resultMap entry: "+entry.toString());
				}	
			}
			
			if(pruneAccountCount != 0){
				taskResult.addMessage("Prune Account entries from custom_immutable : "+pruneAccountCount);
			}
			if(pruneGroupCount != 0){
				taskResult.addMessage("Prune Group entries from custom_immutable : "+pruneGroupCount);
			}
			
			
			if(resultMap.containsKey("Exception see log")){
				throw new Exception("Exception occurred please see log");
			} else {
				taskResult.setCompletionStatus(CompletionStatus.Success);
			}
			
			
		}catch (Exception e){
			logger.error(stack2string(e));
			throw e;
			
		} finally {
			Util.flushIterator(accountsIt);
			Util.flushIterator(groupsIt);
		}

	}

	private void setApplicationDetails(String applicationID2) throws Exception {
		
		Application app = this.ctx.getObject(Application.class, applicationID2);
		if(null != app){
			this.applicationID = app.getId();
			this.applicationName = app.getName();
		} else {
			throw new Exception("Did not find Application " +applicationID2);
		}
		
	}

	private void pruneCustomImmutable(List<String> applicationList) throws Exception {
		
		Connection conn = ctx.getJdbcConnection();
		
		Statement stmt = conn.createStatement(ResultSet.TYPE_FORWARD_ONLY,ResultSet.CONCUR_UPDATABLE);

		String sql = "select immutableid, application from custom_immutable";
		ResultSet rs = stmt.executeQuery(sql);
		
		while(rs.next()){
			if(!applicationList.contains(rs.getString(2))){
				rs.deleteRow();
			}
		}
		
		if(!rs.isClosed()){
			rs.close();
		}
		if(!stmt.isClosed()){
			stmt.close();
		}
		
	}

	private void pruneCustomImmutable(List<String> immutableIds, String type, String application, boolean processDelete) throws Exception {
		
		if(logger.isDebugEnabled()) logger.debug("Starting pruneCustomImmutable with type : " +type +" application " +application);
		
		int count = 0;
		
		Connection conn = ctx.getJdbcConnection();
		
		Statement stmt = conn.createStatement(ResultSet.TYPE_FORWARD_ONLY,ResultSet.CONCUR_UPDATABLE);

		String sql = "select immutableid, dn, identity from custom_immutable where type = '" +type +"' and application='" +application +"'";
		ResultSet rs = stmt.executeQuery(sql);
		
		while(rs.next()){
			String immutableID = rs.getString(1);
			String dn = rs.getString(2);
			String identityID = rs.getString(3);
			if(!immutableIds.contains(immutableID)){
				if(logger.isDebugEnabled()) logger.debug("Did not find immutableID " +immutableID);
				rs.deleteRow();
				count ++;
				
				//Deal with the deleted Object
				if(type.equals(DNChangeScanner.TYPE_ACCOUNT) && processDelete){
					DNChangeHandler ch = new DNChangeHandler(this.ctx, application, null, dn);
					ch.processDeletedObject(identityID, DNChangeScanner.TYPE_ACCOUNT);
					
					if(resultMap.containsKey("Account Deleted")){
						int counter = resultMap.get("Account Deleted");
						resultMap.put("Account Deleted", ++counter);
					}else if(!resultMap.containsKey("Account Deleted")){
						resultMap.put("Account Deleted", 1);
					}
					
				} else if(type.equals(DNChangeScanner.TYPE_GROUP) && processDelete){
					DNChangeHandler ch = new DNChangeHandler(this.ctx, application, null, dn);
					ch.processDeletedObject(null, DNChangeScanner.TYPE_GROUP);
					
					if(resultMap.containsKey("Group Deleted")){
						int counter = resultMap.get("Group Deleted");
						resultMap.put("Group Deleted", ++counter);
					}else if(!resultMap.containsKey("Group Deleted")){
						resultMap.put("Group Deleted", 1);
					}
				}
				
				AuditWriter.auditDNChange(this.ctx, dn, null, application, "Delete from custom_immutable", "Delete", "Deleted entry from custom_immutable : " +immutableID +" : " +dn +" if was no longer found in IIQ", null, null);
			} 
		}
		
		if(!rs.isClosed())
			rs.close();
		
		if(!stmt.isClosed())
			stmt.close();
		
		if(TYPE_ACCOUNT.equals(type)){
			pruneAccountCount = count;
		} else if(TYPE_GROUP.equals(type)){
			pruneGroupCount=count;
		}
		
	}

	private List<String> validateApplications(
			List<String> applicationList, List<String> uuidOverride) throws Exception {
		
		List<String> returnList = new ArrayList<String>();
		
		if(logger.isDebugEnabled()) logger.debug("Starting validateApplications with : " +applicationList);
		
		for(String appNameID: applicationList){
			
			Application app = ctx.getObject(Application.class, appNameID);
			
			if(null == app){
				throw new Exception("Application " +appNameID +" is not a valid application in IIQ");
			} else {
				returnList.add(app.getId());
				ctx.decache(app);
			}
			
			setApplicationDetails(appNameID);
			
			String[] applicationUuid = null;
			
			applicationUuid = getApplicationUuid(uuidOverride);
			
			String appName = app.getName();
			
			QueryOptions qo = new QueryOptions();
			qo.addFilter(Filter.eq("application.name", appName));
			qo.setResultLimit(1);
			
			List<String> attributes = new ArrayList<String>();
			
			Iterator<Object[]> links = ctx.search(Link.class, qo, "id");
			
			while(links.hasNext()){
				Object[] result = links.next();
				
				String ni = null;
				String uuid = null;
				
				Link link = this.ctx.getObject(Link.class, (String)result[0]);
				
				if(null == applicationUuid[0]){
					ni = link.getNativeIdentity();
					uuid = link.getUuid();
				} else {
					ni = link.getNativeIdentity();
					uuid = (String) link.getAttribute((String)applicationUuid[1]);
				}
				
				if(null == ni || null == uuid){
					throw new Exception("Application " +appName +" does not have both nativeIdentity and uuid");
				}
				try{
					LdapName ln = new LdapName((String) ni);
				}catch (Exception e){
					throw new Exception("Application nativeIdentity does not apear to be a valid DN");
				}
			}
		}
		
		if(returnList.size()==0){
			throw new Exception("No Application was found");
		}
		return returnList;
	}

	private Iterator<Object[]> getAccountsIterator(String application, List<String> uuidOverride) throws Exception {
		
		if(logger.isDebugEnabled()) logger.debug("Starting getAccountsIterator application " +application );
		
		QueryOptions qo = new QueryOptions();
		
		qo.addFilter(Filter.eq("application.id", application));
		
		String[] applicationUuid = null;
		
		applicationUuid = getApplicationUuid(uuidOverride);
		
		if(logger.isDebugEnabled()) logger.debug("Application custom immutable is  " +applicationUuid[0] +" " +applicationUuid[1] +" " +applicationUuid[2] );
		
		List<String> returnValues = new ArrayList<String>();
		
		if(null == applicationUuid[0]){
			returnValues.add("nativeIdentity");
			returnValues.add("uuid");
			returnValues.add("identity.id");
			return ctx.search(Link.class, qo, returnValues);
		} else {
			returnValues.add("id");
			returnValues.add("uuid");
			returnValues.add("identity.id");
			
			List<Object[]> returnObjects= new ArrayList();
			
			Iterator<Object[]> searchIt = ctx.search(Link.class, qo, returnValues);
			
			while (searchIt.hasNext()){
				Object[] line = searchIt.next();
				
				Link link = this.ctx.getObject(Link.class, (String)line[0]);
				
				Object[] accountEntry = new Object[3];
				accountEntry[0]=link.getNativeIdentity();
				
				String immutable = ((String) link.getAttribute(applicationUuid[1])).trim();
				if(null != immutable){
					accountEntry[1]=immutable;
				} else {
					throw new Exception("Did not find immutable value for attribute " +applicationUuid[1] +" on Account " +link.getNativeIdentity());
				}
				accountEntry[2]=line[2];
				
				returnObjects.add(accountEntry);
				
				this.ctx.decache(link);
			}
			
			return returnObjects.iterator();
		}
	
	}
	
	private String[] getApplicationUuid(List<String> uuidOverride) throws Exception {
		
		String[] applicationUuid = new String[3];
		
		if(null != uuidOverride){
			for(int i=0; i<uuidOverride.size(); i++){
				String uuidOverrideString = uuidOverride.get(i);
				if(null != uuidOverrideString && uuidOverrideString.toLowerCase().startsWith(this.applicationName.toLowerCase())){
					applicationUuid = uuidOverrideString.split(";");
					if(applicationUuid.length != 3){
						throw new Exception("Make sure manually defined immutable is in the format <application_name>;<account_immutable_attribute>;<group_immutable_attribute>");
					}
				}
			}
		}
		return applicationUuid;
	}

	private Iterator<Object[]> getGroupsIterator(String application, List<String> uuidOverride) throws Exception {
		if(logger.isDebugEnabled()) logger.debug("Starting getGroupsIterator application " +application );

		QueryOptions qo = new QueryOptions();

		qo.addFilter(Filter.eq("application.id", application));
		qo.addFilter(Filter.eq("attribute", getGroupAttribute(application)));
		
		String[] applicationUuid = null;
		
		applicationUuid = getApplicationUuid(uuidOverride);
		
		if(logger.isDebugEnabled()) logger.debug("Application custom immutable is  " +applicationUuid[0] +" " +applicationUuid[1] +" " +applicationUuid[2] );

		List<String> returnValues = new ArrayList<String>();
		if(null == applicationUuid[0]){
			returnValues.add("value");
			returnValues.add("uuid");
			
			return ctx.search(ManagedAttribute.class, qo, returnValues);
			
		} else {
			returnValues.add("id");
			returnValues.add("value");
			
			List<Object[]> returnObjects= new ArrayList();
			
			Iterator<Object[]> searchIt = ctx.search(ManagedAttribute.class, qo, returnValues);
			
			while (searchIt.hasNext()){
				Object[] line = searchIt.next();
				
				ManagedAttribute ma = this.ctx.getObject(ManagedAttribute.class, (String)line[0]);
				
				Object[] groupEntry = new Object[2];
				groupEntry[0]=ma.getValue();
				
				String immutable = ((String) ma.getAttribute(applicationUuid[2])).trim();
				if(null != immutable){
					groupEntry[1]=immutable;
				} else {
					throw new Exception("Did not find immutable value for attribute " +applicationUuid[2] +" on Account " +ma.getValue());
				}
				
				groupEntry[1]=ma.getAttribute(applicationUuid[2]);
				
				returnObjects.add(groupEntry);
				
				this.ctx.decache(ma);
			}
			
			return returnObjects.iterator();
		}

	}

	private String getGroupAttribute(String application) throws Exception {
		
		if(logger.isDebugEnabled()) logger.debug("Starting getGroupAttribute");
		
		Application app = this.ctx.getObject(Application.class, application);
		
		if(null != app){
			Schema accSchema = app.getAccountSchema();
			String grpattr = accSchema.getGroupAttribute("group");
			if(logger.isDebugEnabled()) logger.debug("returning grpattr : " +grpattr);
			return grpattr;
		}else {
			if(logger.isDebugEnabled()) logger.debug("returning grpattr : null");
			return null;
		}
		
	}

	private void createTableIfNotExist() throws Exception {
		
		if(logger.isDebugEnabled()) logger.debug("Starting createTableIfNotExist");
		
		Connection conn = ctx.getJdbcConnection();
		
		DatabaseMetaData dbm = conn.getMetaData();
		
		ResultSet rs = dbm.getTables(null, null, "CUSTOM_IMMUTABLE", null);
		
		if (!rs.next()) {
			if(logger.isDebugEnabled()) logger.debug("Database table custom_immutable does not exist we'll create it");
            PreparedStatement create = conn.prepareStatement("create table custom_immutable(immutableid varchar(100) not null, dn varchar(1024) not null, identity varchar(100), type varchar(20) not null, application varchar(100) not null, primary key (immutableid))");
            create.executeUpdate();
            /*Not needed because declaring a primary key creates an index and Oracle will bail out
            create = conn.prepareStatement("CREATE INDEX immutable_id ON custom_immutable(immutableid)");
            create.executeUpdate();
            */
            create = conn.prepareStatement("CREATE INDEX immutable_dn ON custom_immutable(dn)");
            create.executeUpdate();
            create = conn.prepareStatement("CREATE INDEX immutable_application ON custom_immutable(application)");
            create.executeUpdate();
        } else {
        	if(logger.isDebugEnabled()) logger.debug("Database table custom_immutable exists");
        }
	
		if(!rs.isClosed())
			rs.close();
	}

	@Override
	public boolean terminate() {
		terminated = true;
		return false;
	}
	
	private static String stack2string(Exception e) {
        try {
            StringWriter sw = new StringWriter();
            PrintWriter pw = new PrintWriter(sw);
            e.printStackTrace(pw);
            return "------\r\n" + sw.toString() + "------\r\n";
        }
        catch(Exception e2) {
            return "bad stack2string";
        }
    }

}

class ScanEntry implements Callable<String>{
	
	private SailPointContext context=null;
	private String immutable = null;
	private String dn = null;
	private String entryType = null;
	private String application = null;
	private String identityID = null;
	private Log logger = null;
	private Attributes<String, Object> args = null; 

	public ScanEntry(Object[] entry, String type, String appId, Attributes<String, Object> args, Log log) {
		this.dn = (String) entry[0];
		this.immutable = (String) entry[1];
		this.application = appId;
		this.entryType = type;
		this.identityID = (String) ((DNChangeScanner.TYPE_ACCOUNT.equals(type)) ? entry[2] : null);
		this.logger = log;
		this.args = args;
	}

	@Override
	public String call() throws Exception {
		
		context = SailPointFactory.createContext("scanEntry");
		
		Connection conn = context.getJdbcConnection();

		try{

			Statement stmt = conn.createStatement();

			String sql = "SELECT immutableid, dn FROM custom_immutable where immutableid = '" +immutable +"' and type = '" +entryType +"' and application='" +application +"'";
			ResultSet rs = stmt.executeQuery(sql);

			try{

				if(rs.next()){
					String oldDN = rs.getString(2);
					if(dn.equals(oldDN)){
						
						return "Validated no change";
					}else{

						try{

							DNChangeHandler ch = new DNChangeHandler(context, application, dn, oldDN);

							//set the variables for what we need to run
							if(args.containsKey("certificationCheck") && true == args.containsKey("certificationCheck")){
								ch.setCertificationCheck(true);
							} else {
								ch.setCertificationCheck(false);
							}

							if(args.containsKey("certificationCheckReport") && true == args.containsKey("certificationCheckReport")){
								ch.setCertificationCheckReport(true);
							} else {
								ch.setCertificationCheckReport(false);
							}

							if(args.containsKey("workFlowCaseCheck") && true == args.containsKey("workFlowCaseCheck")){
								ch.setWorkFlowCaseCheck(true);
							} else {
								ch.setWorkFlowCaseCheck(false);
							}

							if(args.containsKey("workFlowCaseCheckReport") && true == args.containsKey("workFlowCaseCheckReport")){
								ch.setWorkFlowCaseCheckReport(true);
							} else {
								ch.setWorkFlowCaseCheckReport(false);
							}

							//Decide what type it is
							if(DNChangeScanner.TYPE_ACCOUNT.equals(entryType)){
								ch.changeAllDNReferencesForAccount(identityID);
							} else if(DNChangeScanner.TYPE_GROUP.equals(entryType)){
								ch.changeAllDNReferencesForGroup();
							}

						} catch(Exception e){
							throw e;
						} finally {
							if(!rs.isClosed())
								rs.close();
							if(!stmt.isClosed())
								stmt.close();
						}


						return "Modified " +entryType +" : " +immutable +" from : " +oldDN +" to : " +dn;
					}

				}else {

					if(null != immutable){
						String insertSQL = "insert into custom_immutable (immutableid, dn, identity, type, application) values(?,?,?,?,?)";
						PreparedStatement preparedStatement = conn.prepareStatement(insertSQL);
						preparedStatement.setString(1, immutable);
						preparedStatement.setString(2, dn);
						preparedStatement.setString(3, identityID);
						preparedStatement.setString(4, entryType);
						preparedStatement.setString(5, application);
						// execute insert SQL stetement
						preparedStatement .executeUpdate();
					} else {
						throw new Exception("immutable cannot be null dn : " +dn +" entryType : " +entryType +" application : " +application +" try and manually define the immutable attribute in the task definition");
					}


					AuditWriter.auditDNChange(context, dn, null, application, "Insert into custom_immutable", "Insert", "Created entry in custom_immutable : " +immutable +" : " +dn +" : " +entryType +" : " +application, null, null);

					return "Insert " +entryType +" into custom_immutable ";
				}
			}catch (Exception e){
				throw e;
			} finally {
				if(!rs.isClosed())
					rs.close();
				if(!stmt.isClosed())
					stmt.close();
			}



		}catch(Exception e){
			logger.error(stack2string(e));
			return "Exception see log";
		}finally{
			SailPointFactory.releaseContext(context);
		}
	}
	private static String stack2string(Exception e) {
        try {
            StringWriter sw = new StringWriter();
            PrintWriter pw = new PrintWriter(sw);
            e.printStackTrace(pw);
            return "------\r\n" + sw.toString() + "------\r\n";
        }
        catch(Exception e2) {
            return "bad stack2string";
        }
    }
}
