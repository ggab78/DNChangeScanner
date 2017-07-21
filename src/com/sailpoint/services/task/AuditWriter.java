package com.sailpoint.services.task;

import sailpoint.api.SailPointContext;
import sailpoint.object.AuditEvent;
import sailpoint.server.Auditor;

public class AuditWriter {
	
	private static void auditAction(SailPointContext context,String action, String source, String target, String application, String instance, String value1, String value2, String value3, String value4) throws Exception{
		
		if(Auditor.isEnabled(action)){
			AuditEvent auditevent = new AuditEvent();
			auditevent.setAction(action);
			auditevent.setSource(source);
			auditevent.setTarget(target);
			auditevent.setApplication(application);
			auditevent.setInstance(instance);
			
			if(null != value1)
				if(value1.length() > 255)
					auditevent.setString1(value1.substring(0, 254));
				else
					auditevent.setString1(value1);
			if(null != value2)
				if(value2.length() > 255)
					auditevent.setString2(value2.substring(0, 254));
				else
					auditevent.setString2(value2);
			if(null != value3)
				if(value3.length() > 255)
					auditevent.setString3(value3.substring(0, 254));
				else
					auditevent.setString3(value3);
			if(null != value4)
				if(value4.length() > 255)
					auditevent.setString4(value4.substring(0, 254));
				else
					auditevent.setString4(value4);
			
			Auditor.log(auditevent);
			context.commitTransaction();
			
		}
		
		
	}
	
	static void auditDNChange(SailPointContext context, String source, String target, String application, String instance, String value1, String value2, String value3, String value4) throws Exception{
		
		auditAction(context,"DNChangeAudit",source,target,application,instance,value1,value2,value3,value4);
		
	}

}
