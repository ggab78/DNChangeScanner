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
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;

import sailpoint.api.ObjectUtil;
import sailpoint.api.PersistenceManager;
import sailpoint.api.SailPointContext;
import sailpoint.api.Terminator;
import sailpoint.object.Application;
import sailpoint.object.AttributeAssignment;
import sailpoint.object.Attributes;
import sailpoint.object.Bundle;
import sailpoint.object.Certification;
import sailpoint.object.EntitlementGroup;
import sailpoint.object.Filter;
import sailpoint.object.Identity;
import sailpoint.object.IdentityEntitlement;
import sailpoint.object.IdentityEntitlement.AggregationState;
import sailpoint.object.IdentityRequest;
import sailpoint.object.Link;
import sailpoint.object.ManagedAttribute;
import sailpoint.object.Profile;
import sailpoint.object.ProvisioningPlan;
import sailpoint.object.QueryOptions;
import sailpoint.object.RoleAssignment;
import sailpoint.object.RoleDetection;
import sailpoint.object.RoleTarget;
import sailpoint.object.WorkItem;
import sailpoint.object.Workflow;
import sailpoint.object.WorkflowCase;
import sailpoint.server.CacheService;
import sailpoint.server.Environment;
import sailpoint.tools.GeneralException;
import sailpoint.tools.Util;
import sailpoint.tools.xml.XMLObjectFactory;

public class DNChangeHandler {

	private Log logger = LogFactory.getLog(DNChangeHandler.class);

	private String entryType = null;
	private String applicationID = null;
	private String applicationName = null;
	private String identityID = null;
	private String newDN=null;
	private String oldDN=null;
	private boolean workFlowCaseCheck = true;
	private boolean certificationCheck = true;
	private boolean workFlowCaseCheckReport = false;
	private boolean certificationCheckReport = false;
	private boolean profileModified = false;
	private SailPointContext ctx = null;
	
	
	public void setWorkFlowCaseCheck(boolean workFlowCaseCheck) {
		this.workFlowCaseCheck = workFlowCaseCheck;
	}

	public void setCertificationCheck(boolean certificationCheck) {
		this.certificationCheck = certificationCheck;
	}
	
	public void setWorkFlowCaseCheckReport(boolean workFlowCaseCheckReport) {
		this.workFlowCaseCheckReport = workFlowCaseCheckReport;
	}

	public void setCertificationCheckReport(boolean certificationCheckReport) {
		this.certificationCheckReport = certificationCheckReport;
	}


	public String getApplicationId() {
		return applicationID;
	}

	private void setApplicationId(String applicationId) throws Exception {
		Application app = this.ctx.getObject(Application.class, applicationId);
		if(null != app){
			setApplicationName(app.getName());
			this.applicationID = app.getId();
			this.ctx.decache(app);
		} else {
			throw new Exception("Application " +applicationId +" is not a valid application id or name");
		}
	}
	
	public String getApplicationName() {
		return applicationName;
	}

	private void setApplicationName(String applicationName) {
		this.applicationName = applicationName;
	}

	public String getEntryType() {
		return entryType;
	}

	private void setEntryType(String entryType) throws Exception {
		
		if(!DNChangeScanner.TYPE_ACCOUNT.equals(entryType) && !DNChangeScanner.TYPE_GROUP.equals(entryType)){
			throw new Exception("entryType must be " +DNChangeScanner.TYPE_ACCOUNT +" or " +DNChangeScanner.TYPE_GROUP);
		}
		this.entryType = entryType;
	}

	public String getIdentityID() {
		return identityID;
	}

	private void setIdentityID(String identityID) {
		this.identityID = identityID;
	}

	public String getNewDN() {
		return newDN;
	}

	private void setNewDN(String newDN) {
		this.newDN = newDN;
	}

	public String getOldDN() {
		return oldDN;
	}

	public void setOldDN(String oldDN) {
		this.oldDN = oldDN;
	}

	public DNChangeHandler(SailPointContext context, String applicationId, String newDN,String oldDN) throws Exception {
		this.ctx = context;
		setNewDN(newDN);
		setOldDN(oldDN);
		setApplicationId(applicationId);
	}
	
	public DNChangeHandler(SailPointContext context, String applicationId, String newDN,String oldDN, boolean workFlowCaseCheck, boolean certificationCheck, boolean workFlowCaseCheckReport, boolean certificationCheckReport) throws Exception {
		this.ctx = context;
		setNewDN(newDN);
		setOldDN(oldDN);
		setApplicationId(applicationId);
		setWorkFlowCaseCheck(workFlowCaseCheck);
		setCertificationCheck(certificationCheck);
		setWorkFlowCaseCheckReport(workFlowCaseCheckReport);
		setCertificationCheckReport(certificationCheckReport);
	}

	public void changeAllDNReferencesForAccount(String identityID) throws Exception {
		
		setIdentityID(identityID);
		setEntryType(DNChangeScanner.TYPE_ACCOUNT);
		
		changeAllDNReferencesTypeAccount();
		
		updateCustomImmutable();
		
	}
	
	public void processDeletedObject(String identityID, String type) throws Exception{
		
		setEntryType(type);
		if(null != identityID && type.equals(DNChangeScanner.TYPE_ACCOUNT)){
			setIdentityID(identityID);
			updateIdentityFromDeleteLink();
		} else if(type.equals(DNChangeScanner.TYPE_GROUP)) {
			removeGroupFromProfile();
		}
	}


	private void removeGroupFromProfile() throws Exception {
		
		//lets try to avoid iterating through all profiles
		
		List<String> profiles = new ArrayList();
		
		Connection conn = this.ctx.getJdbcConnection();
		
		Statement stmt = conn.createStatement();
		
		ResultSet rs = stmt.executeQuery("select profile from spt_profile_constraints where elt like '%" +this.oldDN +"%'");
		
		while(rs.next()){
			profiles.add(rs.getString(1));
		}
		
		if(!rs.isClosed())
			rs.close();
		if(!stmt.isClosed())
			stmt.close();
		
		if(!profiles.isEmpty()){
			for(String profileId : profiles){
				
				Profile profile = this.ctx.getObject(Profile.class, profileId);
				
				Bundle bundle = profile.getBundle();
				
				bundle.remove(profile);
				
				this.ctx.saveObject(bundle);
				this.ctx.commitTransaction();

				AuditWriter.auditDNChange(this.ctx, this.oldDN, this.newDN, this.applicationName, "RemoveGroupFromRole", "Delete", "Group  :  " +this.oldDN +" was removed from Role : " +bundle.getName()  , null, null);
				
				this.ctx.decache(bundle);
				this.ctx.decache(profile);
			}
		}
		
	}

	private void updateIdentityFromDeleteLink() throws Exception {
		
		Identity identity = this.ctx.getObject(Identity.class, this.identityID);
		
		if(null != identity){
			
			deleteLinkFromPreferances(identity);
			deleteLinkFromExceptions(identity);
			
			if(this.workFlowCaseCheck){
				terminateWorkflowCase();
			}
			
			this.ctx.decache(identity);
		}
		
	}
	
	private void terminateWorkflowCase() throws Exception {

		if(logger.isDebugEnabled()) logger.debug("Starting terminateWorkflowCase");

		Iterator<Object[]> wfCases = this.ctx.search(WorkflowCase.class, null, "id");
		
		while(wfCases.hasNext()){
			
			String wfCaseId = (String)wfCases.next()[0];
			
			WorkflowCase wfCase = this.ctx.getObject(WorkflowCase.class, wfCaseId);
			
			if(null != wfCase){

				String targetName = wfCase.getTargetName();
				String launcher = wfCase.getLauncher();

				if(null != wfCase){
					Workflow workflow = wfCase.getWorkflow();

					if(null != workflow){

						ProvisioningPlan plan = (ProvisioningPlan) workflow.get("plan");
						String identityRequestId = (String) workflow.get("identityRequestId");

						if(null != plan){
							if(plan.toXml().contains(this.oldDN)){

								if(!workFlowCaseCheckReport){

									this.ctx.decache(wfCase);

									Terminator term = new Terminator(this.ctx);
									term.deleteObjects(WorkflowCase.class, new QueryOptions(Filter.eq("id", wfCaseId)));
									term.deleteObjects(IdentityRequest.class, new QueryOptions(Filter.eq("name", identityRequestId)));

									AuditWriter.auditDNChange(this.ctx, this.oldDN, this.newDN, this.applicationName, "WorkflowCase", "Delete", "WorkflowCase : " +wfCaseId +" and identityRequest : " +identityRequestId +" for : " +targetName +" raised by : " +launcher +" was identified as having entries for a deleted user : " +this.oldDN +" This WorkflowCase was Deleted", null, null);

								}else{
									AuditWriter.auditDNChange(this.ctx, this.oldDN, this.newDN, this.applicationName, "WorkflowCase", "Delete", "WorkflowCase : " +wfCaseId +" and identityRequest : " +identityRequestId +" for : " +targetName +" raised by : " +launcher +" was identified as having entries for a deleted user : " +this.oldDN, null, null);
								}

							}
						}
					}

					this.ctx.decache(wfCase);
				}
			}
		}
		
	}
	
	private void deleteLinkFromExceptions(Identity identity) throws Exception{
		
		boolean modified = false;
		
		if(logger.isDebugEnabled()) logger.debug("Starting deleteLinkFromExceptions");

		List<EntitlementGroup> exceptions = identity.getExceptions();

		if(null != exceptions){
			
			Iterator<EntitlementGroup> exceptionsIt = exceptions.iterator();
			
			while(exceptionsIt.hasNext()){
				EntitlementGroup entGrp = exceptionsIt.next();
				if(this.applicationID.equals(entGrp.getApplication().getId()) && this.oldDN.equals(entGrp.getNativeIdentity())){
					exceptionsIt.remove();
					modified=true;
				}
				Attributes<String, Object> attr = entGrp.getAttributes();
				
				Iterator<Entry<String, Object>> entries = attr.entrySet().iterator();
				
				while(entries.hasNext()){
					Map.Entry<String, Object> entry = entries.next();
					
					if(entry.getValue() instanceof String){
						if(entry.getValue().toString().contains(this.oldDN)){
							entries.remove();
							modified=true;
						}
					} else if(entry.getValue() instanceof List){
						List values = (List) entry.getValue();
						
						if(values.contains(this.oldDN)){
							entries.remove();
							modified=true;
						}
					}
				}
				
			}
			
			if(modified){
				identity.setExceptions(exceptions);
				this.ctx.saveObject(identity);
				this.ctx.commitTransaction();
			}
		}
		
		if(modified){
			AuditWriter.auditDNChange(this.ctx, this.oldDN, this.newDN, this.applicationName, "IdentityExceptions", "Delete", "IdentityExceptions  :  was Deleted for a renamed user : " +this.oldDN , null, null);
		}

	}
	
	private void deleteLinkFromPreferances(Identity identity) throws Exception {

		if(logger.isDebugEnabled()) logger.debug("Starting deleteLinkFromPreferances ");

		boolean modified = false;

		Map<String, Object> identityPref = identity.getPreferences();

		if(null != identityPref){

			if(DNChangeScanner.TYPE_ACCOUNT.equals(this.entryType)){

				List<RoleAssignment> roleAsignments = (List<RoleAssignment>) identityPref.get(Identity.PRF_ROLE_ASSIGNMENTS);

				if(null != roleAsignments){
					Iterator<RoleAssignment> roleAssignmentIt =  roleAsignments.iterator();
					while(roleAssignmentIt.hasNext()){
						RoleAssignment assignment = (RoleAssignment) roleAssignmentIt.next();
						List<RoleTarget> targets = assignment.getTargets();
						if(null != targets){
							Iterator<RoleTarget> targetIt =  targets.iterator();
							while(targetIt.hasNext()){
								RoleTarget target = targetIt.next();
								if(null != target.getNativeIdentity() && target.getNativeIdentity().equals(this.oldDN) && target.getApplicationId().equals(this.applicationID)){
									roleAssignmentIt.remove();
									
									Bundle bundle = this.ctx.getObject(Bundle.class, assignment.getRoleName());
									
									//Also remove it from identityEntitlements
									Terminator term = new Terminator(this.ctx);
									term.deleteObjects(IdentityEntitlement.class, new QueryOptions(Filter.and(Filter.eq("name", "assignedRoles"), Filter.eq("value",assignment.getRoleName() ))));
									
									identity.removeAssignedRole(bundle);
									
									AuditWriter.auditDNChange(this.ctx, this.oldDN, this.newDN, this.applicationName, "deleteLinkFromPreferances", "Delete", "RoleAssignment " +assignment.getRoleName() +" was Removed from Identity : " +identity.getName() +" because of Deleted " +entryType +" : " +this.oldDN , null, null);
									
									modified = true;
								}
							}
						}
					}
				}

				List<RoleDetection> roleDetections = (List<RoleDetection>) identityPref.get(Identity.PRF_ROLE_DETECTIONS);

				if(null != roleDetections){
					Iterator<RoleDetection> roleDetectionsIt =  roleDetections.iterator();
					while(roleDetectionsIt.hasNext()){
						RoleDetection detection = roleDetectionsIt.next();
						List<RoleTarget> targets = detection.getTargets();
						if(null != targets){
							Iterator<RoleTarget> targetIt =  targets.iterator();
							while(targetIt.hasNext()){
								RoleTarget target = targetIt.next();
								if(null != target.getNativeIdentity() && target.getNativeIdentity().equals(this.oldDN) && target.getApplicationId().equals(this.applicationID)){
									roleDetectionsIt.remove();
									
									Bundle bundle = this.ctx.getObject(Bundle.class, target.getRoleName());
									
									//Also remove it from identityEntitlements
									//Should not need this but still
									Terminator term = new Terminator(this.ctx);
									term.deleteObjects(IdentityEntitlement.class, new QueryOptions(Filter.and(Filter.eq("name", "detectedRoles"), Filter.eq("value",detection.getRoleName() ))));
									
									identity.removeDetectedRole(bundle);
									
									modified = true;
								}
							}
						}
					}
				}
			}


			List<AttributeAssignment> attributeAssignments = (List<AttributeAssignment>) identityPref.get(Identity.PRF_ATTRIBUTE_ASSIGNMENTS);

			if(null != attributeAssignments){
				Iterator<AttributeAssignment> attributeAssignmentsIt =  attributeAssignments.iterator();
				while(attributeAssignmentsIt.hasNext()){
					AttributeAssignment attributeAssignment = attributeAssignmentsIt.next();

					if(DNChangeScanner.TYPE_ACCOUNT.equals(this.entryType)){
						if(null != attributeAssignment.getNativeIdentity() && attributeAssignment.getNativeIdentity().equals(this.oldDN) && attributeAssignment.getApplicationId().equals(this.applicationID)){
							attributeAssignmentsIt.remove();
							modified = true;
						}
					} 
				}
				if(modified){
					AuditWriter.auditDNChange(this.ctx, this.oldDN, this.newDN, this.applicationName, "deleteLinkFromPreferances", "Delete", "IdentityPreferences  :  was modified on Identity : " +identity.getName() +" for a Deleted " +entryType +" : " +this.oldDN , null, null);
				}
			}
		}

	}

	public void changeAllDNReferencesForGroup() throws Exception {
		
		setEntryType(DNChangeScanner.TYPE_GROUP);
		
		changeAllDNReferencesTypeGroup();
		
		if(this.profileModified){
			Environment env = Environment.getEnvironment();
	        CacheService caches = env.getCacheService();
	        if (caches != null)
	            caches.forceRefresh(this.ctx);
		}
		
		updateCustomImmutable();

	}
	
	private void updateCustomImmutable() throws Exception {
		
		Connection conn = this.ctx.getJdbcConnection();
		
		DatabaseMetaData dbm = conn.getMetaData();
		
		ResultSet rs = dbm.getTables(null, null, "CUSTOM_IMMUTABLE", null);
		
		if (rs.next()) {

			String updateSQL = "update custom_immutable set dn = ? where dn= ? and type = ? and application = ?";
			PreparedStatement preparedStatement = conn.prepareStatement(updateSQL);
			preparedStatement.setString(1, this.newDN);
			preparedStatement.setString(2, this.oldDN);
			preparedStatement.setString(3, this.entryType);
			preparedStatement.setString(4, this.applicationID);
			// execute Update SQL stetement
			preparedStatement.executeUpdate();
		}
		
	}
	

	private void changeAllDNReferencesTypeGroup() throws Exception {
		
		modifyManagedAttributeObject();
		
		//get a list of Accounts connected to the group
		
		List<String> groupMembers = getGroupMembers();
		
		for(String member : groupMembers){
			modifyIdentityReferences(member);
		}
		
		modifyProfileConstraints();
		
		if(this.workFlowCaseCheck){
			modifyWorkflowCases();
		}
		
		if(this.certificationCheck){
			modifyCertifications();
		}
		
	}


	private void modifyManagedAttributeObject() throws Exception {
		if(logger.isDebugEnabled()) logger.debug("Starting modifyManagedAttributeObject application : " +this.applicationID +" old nativeIdentity : " +this.oldDN +" new nativeIdentity : " +this.newDN);


		if(null == this.applicationID || null == this.oldDN || null == this.newDN)
			throw new Exception("Requires applicationId and old DN");

		QueryOptions oldQO = new QueryOptions();
		oldQO.addFilter(Filter.eq("application.id", this.applicationID));
		oldQO.addFilter(Filter.eq("value", this.oldDN));

		QueryOptions newQO = new QueryOptions();
		newQO.addFilter(Filter.eq("application.id", this.applicationID));
		newQO.addFilter(Filter.eq("value", this.newDN));

		ManagedAttribute oldMA = null;
		ManagedAttribute newMA = null;

		List<ManagedAttribute> mas = this.ctx.getObjects(ManagedAttribute.class, oldQO);
		if(null != mas && mas.size() > 0){
			oldMA = mas.get(0);
		}

		mas = this.ctx.getObjects(ManagedAttribute.class, newQO);
		if(null != mas && mas.size() > 0){
			newMA = mas.get(0);
		}

		if(null != newMA && null != oldMA){
			Terminator term = new Terminator(this.ctx);
			term.deleteObjects(Link.class, oldQO);
		} else if(null != oldMA && null == newMA){
			oldMA.setValue(this.newDN);
			this.ctx.saveObject(oldMA);
			this.ctx.commitTransaction();
		} 

	}

	private List<String> getGroupMembers() throws Exception {
		
		List<String> returnList = new ArrayList<String>();
		
		QueryOptions qo = new QueryOptions();
		
		qo.addFilter(Filter.eq("value", this.newDN));
		qo.addFilter(Filter.eq("application.id", this.applicationID));
		
		Iterator<Object[]> idenEntIt = this.ctx.search(IdentityEntitlement.class, qo, "identity.id");
		
		while(idenEntIt.hasNext()){
			returnList.add((String) idenEntIt.next()[0]);
		}
		
		return returnList;
		
	}

	private void modifyProfileConstraints() throws Exception {
		
		boolean modified = false;
		
		//lets try to avoid iterating through all profiles
		
		List<String> profiles = new ArrayList();
		
		Connection conn = this.ctx.getJdbcConnection();
		
		Statement stmt = conn.createStatement();
		
		ResultSet rs = stmt.executeQuery("select profile from spt_profile_constraints where elt like '%" +this.oldDN +"%'");
		
		while(rs.next()){
			profiles.add(rs.getString(1));
		}
		
		if(!rs.isClosed())
			rs.close();
		if(!stmt.isClosed())
			stmt.close();
		
		if(!profiles.isEmpty()){
			for(String profileId : profiles){
				
				Profile profile = this.ctx.getObject(Profile.class, profileId);
				
				List<Filter> constraints = profile.getConstraints();

				List<Filter> newFilters = new ArrayList<Filter>();

				for(Filter filter : constraints){
					String filterXml = filter.toXml();
					if(filterXml.contains(this.oldDN)){
						if(logger.isTraceEnabled()) logger.trace("Filter xml for profile " +profileId +" before change " +filterXml);

						filterXml = filterXml.replace(this.oldDN, this.newDN);


						Filter newfilter = (Filter) XMLObjectFactory.getInstance().parseXml(this.ctx, filterXml, false);

						if(logger.isTraceEnabled()) logger.trace("Filter xml for profile " +profileId +" after change " +newfilter.toXml());

						newFilters.add(newfilter);
						
						modified = true;

					} else{
						newFilters.add(filter);
					}
				}
				
				if(modified){
					
					profile.setConstraints(newFilters);
					this.ctx.saveObject(profile);

					AuditWriter.auditDNChange(this.ctx, this.oldDN, this.newDN, this.applicationName, "ProfileConstraints", "Update", "ProfileConstraints  :  was modified for a renamed " +entryType +" : " +this.oldDN , null, null);
				}
			}
			this.ctx.commitTransaction();
		}
		
	}

	private void changeAllDNReferencesTypeAccount() throws Exception {

		modifyLinkObject();

		modifyIdentityReferences();
		
		if(this.workFlowCaseCheck){
			modifyWorkflowCases();
		}
		
		if(this.certificationCheck){
			modifyCertifications();
		}
		
	}
	

	private void modifyCertifications() throws Exception {
		
		if(logger.isDebugEnabled()) logger.debug("Starting modifyCertifications");
		
		QueryOptions qo = new QueryOptions(Filter.eq("complete", false));
		
		Iterator<Object[]> certIt = this.ctx.search(Certification.class, qo, "id");
		
		while(certIt.hasNext()){
			
			Certification cert = this.ctx.getObject(Certification.class, (String)certIt.next()[0]);
			
			if(cert.toXml().contains(this.oldDN)){
				
				if(!certificationCheckReport){
					
					cert = ObjectUtil.lockCert(this.ctx, cert, ObjectUtil.DEFAULT_LOCK_TIMEOUT);
					
					try{

						String certString = cert.toXml();
						certString = certString.replace(this.oldDN, this.newDN);

						Certification modifiedCert = (Certification) XMLObjectFactory.getInstance().parseXml(this.ctx, certString, false);

						if(logger.isTraceEnabled())logger.trace("CertString modified : " +modifiedCert.toXml());

						this.ctx.saveObject(modifiedCert);

						this.ctx.commitTransaction();

					} catch(Exception e){
						throw e;
					} finally{
						ObjectUtil.unlockCert(this.ctx, cert);
					}

					AuditWriter.auditDNChange(this.ctx, this.oldDN, this.newDN, this.applicationName, "Certification", "Update", "Certification : " +cert.getId() +" : " +cert.getName() +" was identified as having entries for a renamed user : " +this.oldDN +" This Certification was Updated", null, null);
					
				} else {
					AuditWriter.auditDNChange(this.ctx, this.oldDN, this.newDN, this.applicationName, "Certification", "Report Only", "Certification : " +cert.getId() +" : " +cert.getName() +" was identified as having entries for a renamed user : " +this.oldDN, null, null);
				}
				
			}
			this.ctx.decache(cert);
			
		}
		
	}

	private void modifyWorkflowCases() throws Exception {

		if(logger.isDebugEnabled()) logger.debug("Starting modifyWorkflowCases");

		Iterator<Object[]> wfCases = this.ctx.search(WorkflowCase.class, null, "id");

		while(wfCases.hasNext()){
			WorkflowCase wfCase = this.ctx.getObject(WorkflowCase.class, (String)wfCases.next()[0]);

			boolean requiresModification = false;

			if(null != wfCase){
				Workflow workflow = wfCase.getWorkflow();

				if(null != workflow){

					ProvisioningPlan plan = (ProvisioningPlan) workflow.get("plan");
					String identityRequestId = (String) workflow.get("identityRequestId");

					if(null != plan){

						if(plan.toXml().contains(this.oldDN)){

							if(!workFlowCaseCheckReport){
								wfCase = ObjectUtil.lockObject(this.ctx, WorkflowCase.class, wfCase.getId(), null, PersistenceManager.LOCK_TYPE_TRANSACTION, Util.uuid(), ObjectUtil.DEFAULT_LOCK_TIMEOUT);

								try{

									String workflowCaseString = wfCase.toXml();

									workflowCaseString = workflowCaseString.replace(this.oldDN, this.newDN);

									if(null != identityRequestId){
										IdentityRequest idRequest = this.ctx.getObjectByName(IdentityRequest.class, identityRequestId);

										String identityRequestString = idRequest.toXml();

										identityRequestString= identityRequestString.replace(this.oldDN, this.newDN);

										IdentityRequest identityRequest = (IdentityRequest) XMLObjectFactory.getInstance().parseXml(this.ctx, identityRequestString, false);

										this.ctx.saveObject(identityRequest);

									}


									WorkflowCase wflowCase = (WorkflowCase) XMLObjectFactory.getInstance().parseXml(this.ctx, workflowCaseString, false);

									if(logger.isTraceEnabled())logger.trace("workflowcase modified : " +wflowCase.toXml());

									this.ctx.saveObject(wflowCase);

									//Now deal with Approvals

									QueryOptions qo = new QueryOptions(Filter.eq("workflowCase.id", wfCase.getId()));

									Iterator<Object[]> workItemsIt = this.ctx.search(WorkItem.class, qo, "id");

									if(null != workItemsIt){
										while(workItemsIt.hasNext()){

											WorkItem wItem = this.ctx.getObject(WorkItem.class, (String)workItemsIt.next()[0]);

											String wItemString = wItem.toXml();

											if(wItemString.contains(this.oldDN)){

												wItemString = wItemString.replace(this.oldDN, this.newDN);

												WorkItem workItem = (WorkItem) XMLObjectFactory.getInstance().parseXml(this.ctx, wItemString, false);

												if(logger.isTraceEnabled())logger.trace("WorkItem : " +wItem.toXml());

												this.ctx.saveObject(workItem);

											}

										}
									}

									this.ctx.commitTransaction();

								} catch(Exception e){
									throw e;
								}finally{
									ObjectUtil.unlockObject(this.ctx, wfCase, PersistenceManager.LOCK_TYPE_PERSISTENT);
								}

								AuditWriter.auditDNChange(this.ctx, this.oldDN, this.newDN, this.applicationName, "WorkflowCase", "Update", "WorkflowCase : " +wfCase.getId() +" : " +wfCase.getName() +" was identified as having entries for a renamed user : " +this.oldDN +" This WorkflowCase was Updated", null, null);

							}else{
								AuditWriter.auditDNChange(this.ctx, this.oldDN, this.newDN, this.applicationName, "WorkflowCase", "Report Only", "WorkflowCase : " +wfCase.getId() +" : " +wfCase.getName() +" was identified as having entries for a renamed user : " +this.oldDN, null, null);
							}

						}
					}
				}

				this.ctx.decache(wfCase);
			}
		}

	}


	
	private void modifyIdentityReferences(String identityID) throws Exception {
		this.setIdentityID(identityID);
		modifyIdentityReferences();
	}

	private void modifyIdentityReferences() throws Exception {

		if(logger.isDebugEnabled()) logger.debug("Starting modifyIdentityReferences application : " +this.applicationID +" oldDN : " +this.oldDN +" newDN : " +this.newDN);

		if(null == this.applicationID || null == this.oldDN || null == this.newDN || null == this.identityID)
			throw new Exception("Requires applicationId, identityID, OldDN and NewDN");

		
		Identity identity = ObjectUtil.lockIdentity(this.ctx, this.identityID);

		if(null != identity){
			
			if(logger.isTraceEnabled())logger.trace("identity read : " +identity.toXml());
			
			try{
				
				modifyPreferances(identity);
				
				if(DNChangeScanner.TYPE_ACCOUNT.equals(this.entryType)){
					modifyIdentityEntitlementsAccountChange();
				} else if(DNChangeScanner.TYPE_GROUP.equals(this.entryType)){
					modifyIdentityEntitlementsGroupChange();
				}
				
				modifyIdentityExceptions(identity);
				
				if(logger.isTraceEnabled())logger.trace("identity after : " +identity.toXml());
				
			}catch(Exception e){
				logger.error(stack2string(e));
				throw e;
			} finally{
				ObjectUtil.unlockIdentity(this.ctx, identity);
				this.ctx.decache(identity);
			}

		} else {
			throw new Exception("Could not find Identity " +this.identityID);
		}

	}

	private void modifyIdentityExceptions(Identity identity) throws Exception{
		
		boolean modified = false;
		
		if(logger.isDebugEnabled()) logger.debug("Starting modifyIdentityExceptions");

		List<EntitlementGroup> exceptions = identity.getExceptions();

		if(null != exceptions){

			for(EntitlementGroup entGrp : exceptions){
				if(this.applicationID.equals(entGrp.getApplication().getId()) && this.oldDN.equals(entGrp.getNativeIdentity())){
					entGrp.setNativeIdentity(this.newDN);
					modified=true;
				}
				Attributes<String, Object> attr = entGrp.getAttributes();
				
				Iterator<Entry<String, Object>> entries = attr.entrySet().iterator();
				
				while(entries.hasNext()){
					Map.Entry<String, Object> entry = entries.next();
					
					if(entry.getValue() instanceof String){
						if(entry.getValue().toString().contains(this.oldDN)){
							entry.setValue(this.newDN);
							modified=true;
						}
					} else if(entry.getValue() instanceof List){
						List values = (List) entry.getValue();
						
						if(values.contains(this.oldDN)){
							values.remove(this.oldDN);
							values.add(this.newDN);
							modified=true;
						}
					}
				}
				
			}
			
			if(modified){
				identity.setExceptions(exceptions);
				this.ctx.saveObject(identity);
				this.ctx.commitTransaction();
			}
		}
		
		if(modified){
			AuditWriter.auditDNChange(this.ctx, this.oldDN, this.newDN, this.applicationName, "IdentityExceptions", "Update", "IdentityExceptions  :  was modified for a renamed user : " +this.oldDN , null, null);
		}

	}
	
	private void modifyIdentityEntitlementsGroupChange() throws Exception {
		
		if(logger.isDebugEnabled()) logger.debug("Starting modifyIdentityEntitlementsGroupChange ");
		
		boolean modified = false;
		
		String delimiter = "@~#";
		
		//Check if the aggregation brought in any IdentityEntitlements that was already there from the old DN 
		//and therefore should not really be here
		QueryOptions qo = new QueryOptions();
		
		qo.addFilter(Filter.eq("value", this.newDN));
		qo.addFilter(Filter.eq("application.id", this.applicationID));
		
		List attributes = new ArrayList();
		
		attributes.add("id");
		attributes.add("name");
		attributes.add("nativeIdentity");
		attributes.add("application");

		Iterator<Object[]> newIdnEnts = this.ctx.search(IdentityEntitlement.class, qo, attributes);
		
		Map existingIdentityEntitlement = new HashMap();
		
		if(null != newIdnEnts){
			while(newIdnEnts.hasNext()){
				Object[] nextRow = newIdnEnts.next();

				existingIdentityEntitlement.put(this.newDN + delimiter +nextRow[1] +delimiter +nextRow[2] +delimiter +nextRow[3] , nextRow[0]);
			}
		}
		
		qo = new QueryOptions();

		qo.addFilter(Filter.eq("value", this.oldDN));
		qo.addFilter(Filter.eq("application.id", this.applicationID));

		Iterator<Object[]> oldIdnEnts = this.ctx.search(IdentityEntitlement.class, qo, attributes);
		
		if(null != oldIdnEnts){

			Terminator term = new Terminator(this.ctx);

			while(oldIdnEnts.hasNext()){

				Object[] nextRow = oldIdnEnts.next();

				IdentityEntitlement idnEnt = this.ctx.getObjectById(IdentityEntitlement.class, (String)(nextRow)[0]);
				if(null != idnEnt){
					if(null != idnEnt.getValue() && idnEnt.getValue().equals(this.oldDN)){
						idnEnt.setValue(this.newDN);
						idnEnt.setAggregationState(AggregationState.Connected);
						
						modified=true;

						//New check if we need to delete anything

						String deletId = (String) existingIdentityEntitlement.get(this.newDN + delimiter +nextRow[1] +delimiter +nextRow[2] +delimiter +nextRow[3]);

						if(null != deletId){
							if(logger.isDebugEnabled()) logger.debug("Deleting IdentityEntitlement : " +this.newDN + delimiter +nextRow[1] +delimiter +nextRow[2] +delimiter +nextRow[3]);
							QueryOptions deleteOption = new QueryOptions(Filter.eq("id", deletId));
							term.deleteObjects(IdentityEntitlement.class, deleteOption);
						}

						this.ctx.saveObject(idnEnt);
						this.ctx.commitTransaction();
					}
				}
			}
		}
		
		if(modified){
			AuditWriter.auditDNChange(this.ctx, this.oldDN, this.newDN, this.applicationName, "IdentityEntitlements-Group", "Update", "IdentityEntitlements  :  was modified for a renamed group : " +this.oldDN , null, null);
		}

	}

	private void modifyIdentityEntitlementsAccountChange() throws Exception {
		
		if(logger.isDebugEnabled()) logger.debug("Starting modifyIdentityEntitlementsAccountChange ");
		
		boolean modified = false;
		
		String delimiter = "¬:";
		
		//Check if the aggregation brought in any IdentityEntitlements that was already there from the old DN 
		//and therefore should not really be here
		QueryOptions qo = new QueryOptions();
		
		qo.addFilter(Filter.eq("nativeIdentity", this.newDN));
		qo.addFilter(Filter.eq("application.id", this.applicationID));
		
		List attributes = new ArrayList();
		
		attributes.add("id");
		attributes.add("name");
		attributes.add("value");
		attributes.add("application");

		Iterator<Object[]> newIdnEnts = this.ctx.search(IdentityEntitlement.class, qo, attributes);
		
		Map existingIdentityEntitlement = new HashMap();
		
		if(null != newIdnEnts){
			while(newIdnEnts.hasNext()){
				Object[] nextRow = newIdnEnts.next();

				existingIdentityEntitlement.put(this.newDN + delimiter +nextRow[1] +delimiter +nextRow[2] +delimiter +nextRow[3] , nextRow[0]);
			}
		}
		
		qo = new QueryOptions();

		qo.addFilter(Filter.eq("nativeIdentity", this.oldDN));
		qo.addFilter(Filter.eq("application.id", this.applicationID));

		Iterator<Object[]> oldIdnEnts = this.ctx.search(IdentityEntitlement.class, qo, attributes);
		
		if(null != oldIdnEnts){

			Terminator term = new Terminator(this.ctx);

			while(oldIdnEnts.hasNext()){

				Object[] nextRow = oldIdnEnts.next();

				IdentityEntitlement idnEnt = this.ctx.getObjectById(IdentityEntitlement.class, (String)(nextRow)[0]);
				if(null != idnEnt){
					if(null != idnEnt.getNativeIdentity() && idnEnt.getNativeIdentity().equals(this.oldDN)){
						idnEnt.setNativeIdentity(this.newDN);
						
						modified=true;

						//New check if we need to delete anything

						String deletId = (String) existingIdentityEntitlement.get(this.newDN + delimiter +nextRow[1] +delimiter +nextRow[2] +delimiter +nextRow[3]);

						if(null != deletId){
							if(logger.isDebugEnabled()) logger.debug("Deleting IdentityEntitlement : " +this.newDN + delimiter +nextRow[1] +delimiter +nextRow[2] +delimiter +nextRow[3]);
							QueryOptions deleteOption = new QueryOptions(Filter.eq("id", deletId));
							term.deleteObjects(IdentityEntitlement.class, deleteOption);
						}

						this.ctx.saveObject(idnEnt);
						this.ctx.commitTransaction();
					}
				}
			}
		}
		
		if(modified){
			AuditWriter.auditDNChange(this.ctx, this.oldDN, this.newDN, this.applicationName, "IdentityEntitlements-Account", "Update", "IdentityEntitlements  :  was modified for a renamed user : " +this.oldDN , null, null);
		}

	}

	private void modifyPreferances(Identity identity) throws Exception {

		if(logger.isDebugEnabled()) logger.debug("Starting modifyPreferances ");

		boolean modified = false;


		Map<String, Object> identityPref = identity.getPreferences();

		if(null != identityPref){

			if(DNChangeScanner.TYPE_ACCOUNT.equals(this.entryType)){

				List<RoleAssignment> roleAsignments = (List<RoleAssignment>) identityPref.get(Identity.PRF_ROLE_ASSIGNMENTS);

				if(null != roleAsignments){
					for(int i=0; i<roleAsignments.size(); i++){
						RoleAssignment assignment = (RoleAssignment) roleAsignments.get(i);
						List<RoleTarget> targets = assignment.getTargets();
						if(null != targets){
							for(RoleTarget target : targets){
								if(null != target.getNativeIdentity() && target.getNativeIdentity().equals(this.oldDN) && target.getApplicationId().equals(this.applicationID)){
									target.setNativeIdentity(this.newDN);
									modified = true;
								}
							}
						}
					}
				}

				List<RoleDetection> roleDetections = (List<RoleDetection>) identityPref.get(Identity.PRF_ROLE_DETECTIONS);

				if(null != roleDetections){
					for(int i=0; i<roleDetections.size(); i++){
						RoleDetection detection = (RoleDetection) roleDetections.get(i);
						List<RoleTarget> targets = detection.getTargets();
						if(null != targets){
							for(RoleTarget target : targets){
								if(null != target.getNativeIdentity() && target.getNativeIdentity().equals(this.oldDN) && target.getApplicationId().equals(this.applicationID)){
									target.setNativeIdentity(this.newDN);
									modified = true;
								}
							}
						}
					}
				}
			}


			List<AttributeAssignment> attributeAssignments = (List<AttributeAssignment>) identityPref.get(Identity.PRF_ATTRIBUTE_ASSIGNMENTS);

			if(null != attributeAssignments){
				for(int i=0; i<attributeAssignments.size(); i++){
					AttributeAssignment attributeAssignment = (AttributeAssignment) attributeAssignments.get(i);

					if(DNChangeScanner.TYPE_ACCOUNT.equals(this.entryType)){
						if(null != attributeAssignment.getNativeIdentity() && attributeAssignment.getNativeIdentity().equals(this.oldDN) && attributeAssignment.getApplicationId().equals(this.applicationID)){
							attributeAssignment.setNativeIdentity(this.newDN);
							modified = true;
						}
					} else if(DNChangeScanner.TYPE_GROUP.equals(this.entryType)){
						if(null != attributeAssignment.getValue() && attributeAssignment.getValue().equals(this.oldDN) && attributeAssignment.getApplicationId().equals(this.applicationID)){
							attributeAssignment.setValue(this.newDN);
							modified = true;
						}
					}
				}
				if(modified){
					AuditWriter.auditDNChange(this.ctx, this.oldDN, this.newDN, this.applicationName, "IdentityPreferences", "Update", "IdentityPreferences  :  was modified on Identity : " +identity.getName() +" for a renamed " +entryType +" : " +this.oldDN , null, null);
				}
			}
		}

	}

	private void modifyLinkObject() throws Exception {

		if(logger.isDebugEnabled()) logger.debug("Starting modifyLinkObject application : " +this.applicationID +" old nativeIdentity : " +this.oldDN +" new nativeIdentity : " +this.newDN);
		
		
		if(null == this.applicationID || null == this.oldDN || null == this.newDN)
			throw new Exception("Requires applicationId and old DN");

		QueryOptions oldQO = new QueryOptions();
		oldQO.addFilter(Filter.eq("application.id", this.applicationID));
		oldQO.addFilter(Filter.eq("nativeIdentity", this.oldDN));
		
		QueryOptions newQO = new QueryOptions();
		newQO.addFilter(Filter.eq("application.id", this.applicationID));
		newQO.addFilter(Filter.eq("nativeIdentity", this.newDN));
		
		Link oldLink = null;
		Link newLink = null;
		
		List<Link> links = this.ctx.getObjects(Link.class, oldQO);
		if(null != links && links.size()>0){
			oldLink = links.get(0);
		}
		
		links = this.ctx.getObjects(Link.class, newQO);
		if(null != links  && links.size()>0){
			newLink = links.get(0);
		}
		
		if(null != newLink && null != oldLink){
			Terminator term = new Terminator(this.ctx);
			term.deleteObjects(Link.class, oldQO);
		} else if(null != oldLink && null == newLink){
			oldLink.setNativeIdentity(this.newDN);
			
			if(null != oldLink.getAttribute("distinguishedName")){
				oldLink.setAttribute("distinguishedName", this.newDN);
			}
			
			this.ctx.saveObject(oldLink);
			this.ctx.commitTransaction();
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
