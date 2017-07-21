package com.sailpoint.services.task;


import org.apache.log4j.Level;

import sailpoint.api.Provisioner;
import sailpoint.api.SailPointContext;
import sailpoint.api.SailPointFactory;
import sailpoint.object.ProvisioningPlan.AttributeRequest;
import sailpoint.object.Attributes;
import sailpoint.object.ProvisioningPlan;
import sailpoint.object.ProvisioningPlan.AccountRequest;
import sailpoint.object.TaskResult;
import sailpoint.object.TaskSchedule;
import sailpoint.spring.SpringStarter;
import sailpoint.tools.GeneralException;
import sailpoint.workflow.WorkflowContext;

public class TestRunner {
	
	final static boolean getContext = true;
	static SailPointContext context = null;
	WorkflowContext wfcontext = null;
	
	public static void main(String[] args) {


		WorkflowContext wfcontext = null;
		SpringStarter ss = null;
		
		try {


			if (getContext) {
				ss = new SpringStarter("iiqBeans.xml");

				final String[] services = {"Task", "Request"};

				ss.setSuppressedServices(services);

				ss.suppressSchedulers();


				ss.start();

				context = SailPointFactory.createContext();				
				
				
	
				
				Attributes attrib = new Attributes();
				
				attrib.put("applications", "4028808d59cafe180159cb381057001e");
				attrib.put("processAccounts", true);
				attrib.put("processGroups", true);
				attrib.put("certificationCheck", true);
				attrib.put("workFlowCaseCheck", true);
				attrib.put("uuidOverride", "My_LDAP;uid;cn&#xA;AD_Local;objectguid;objectguid");
				
				DNChangeScanner cs = new DNChangeScanner();
				cs.execute(context, new TaskSchedule(), new TaskResult(),attrib);

			}
			
		} catch(Exception e){
			System.out.println(e);
		} finally {
			if (null != ss) {
				ss.close();
			}
		}
		
	}

}
