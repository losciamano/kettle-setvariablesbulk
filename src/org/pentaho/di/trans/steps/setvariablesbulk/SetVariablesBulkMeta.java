 /* Copyright (c) 2007 Pentaho Corporation.  All rights reserved. 
 * This software was developed by Pentaho Corporation and is provided under the terms 
 * of the GNU Lesser General Public License, Version 2.1. You may not use 
 * this file except in compliance with the license. If you need a copy of the license, 
 * please go to http://www.gnu.org/licenses/lgpl-2.1.txt. The Original Code is Pentaho 
 * Data Integration.  The Initial Developer is Pentaho Corporation.
 *
 * Software distributed under the GNU Lesser Public License is distributed on an "AS IS" 
 * basis, WITHOUT WARRANTY OF ANY KIND, either express or  implied. Please refer to 
 * the license for the specific language governing your rights and limitations.*/

package org.pentaho.di.trans.steps.setvariablesbulk;

import java.util.List;
import java.util.Map;

import org.pentaho.di.core.CheckResult;
import org.pentaho.di.core.CheckResultInterface;
import org.pentaho.di.core.Const;
import org.pentaho.di.core.Counter;
import org.pentaho.di.core.database.DatabaseMeta;
import org.pentaho.di.core.exception.KettleException;
import org.pentaho.di.core.exception.KettleXMLException;
import org.pentaho.di.core.row.RowMetaInterface;
import org.pentaho.di.core.xml.XMLHandler;
import org.pentaho.di.i18n.BaseMessages;
import org.pentaho.di.repository.Repository;
import org.pentaho.di.repository.ObjectId;
import org.pentaho.di.trans.Trans;
import org.pentaho.di.trans.TransMeta;
import org.pentaho.di.trans.step.BaseStepMeta;
import org.pentaho.di.trans.step.StepDataInterface;
import org.pentaho.di.trans.step.StepInterface;
import org.pentaho.di.trans.step.StepMeta;
import org.pentaho.di.trans.step.StepMetaInterface;
import org.w3c.dom.Node;

/**
 * Sets environment variables based on content in certain fields of a single input row.
 * 
 * Created on 27-apr-2006
 */
public class SetVariablesBulkMeta extends BaseStepMeta implements StepMetaInterface
{
	private static Class<?> PKG = SetVariablesBulkMeta.class; // for i18n purposes, needed by Translator2!!   $NON-NLS-1$

    public static final int VARIABLE_TYPE_JVM              = 0;
    public static final int VARIABLE_TYPE_PARENT_JOB       = 1;
    public static final int VARIABLE_TYPE_GRAND_PARENT_JOB = 2;
    public static final int VARIABLE_TYPE_ROOT_JOB         = 3;
    
    private static final String variableTypeCode[] = { "JVM", "PARENT_JOB", "GP_JOB", "ROOT_JOB" };
    private static final String variableTypeDesc[] = 
        { 
            "Valid in the Java Virtual Machine", 
            "Valid in the parent job", 
            "Valid in the grand-parent job", 
            "Valid in the root job" 
        };
    
    private String variableValueField;
    private String variableNameField;
    private int variableType;
    private boolean usingFormatting;
	
	public SetVariablesBulkMeta()
	{
		super(); // allocate BaseStepMeta
	}
	
	/**
     * @return Returns the fieldName.
     */
    public String getVariableValueField()
    {
        return variableValueField;
    }
    
    /**
     * @param fieldName The fieldName to set.
     */
    public void setVariableValueField(String fieldName)
    {
        this.variableValueField = fieldName;
    }
 
    /**
     * @param fieldValue The fieldValue to set.
     */
    public void setVariableNameField(String fieldValue)
    {
        this.variableNameField = fieldValue;
    }
    
    /**
     * @return Returns the fieldValue.
     */
    public String getVariableNameField()
    {
        return variableNameField;
    }
    
    /**
     * @return Returns the local variable flag: true if this variable is only valid in the parents job.
     */
    public int getVariableType()
    {
        return variableType;
    }
    
    /**
     * @param variableType The variable type, see also VARIABLE_TYPE_...
     * @return the variable type code for this variable type
     */
    public static final String getVariableTypeCode(int variableType)
    {
        return variableTypeCode[variableType];
    }
    
    /**
     * @param variableType The variable type, see also VARIABLE_TYPE_...
     * @return the variable type description for this variable type
     */
    public static final String getVariableTypeDescription(int variableType)
    {
        return variableTypeDesc[variableType];
    }

    /**
     * @param variableType The code or description of the variable type 
     * @return The variable type
     */
    public static final int getVariableType(String variableType)
    {
        for (int i=0;i<variableTypeCode.length;i++)
        {
            if (variableTypeCode[i].equalsIgnoreCase(variableType)) return i;
        }
        for (int i=0;i<variableTypeDesc.length;i++)
        {
            if (variableTypeDesc[i].equalsIgnoreCase(variableType)) return i;
        }
        return VARIABLE_TYPE_JVM;
    }

    /**
     * @param localVariable The localVariable to set.
     */
    public void setVariableType(int localVariable)
    {
        this.variableType = localVariable;
    }
    
    public static final String[] getVariableTypeDescriptions()
    {
        return variableTypeDesc;
    }
    

 	public void loadXML(Node stepnode, List<DatabaseMeta> databases, Map<String, Counter> counters)
		throws KettleXMLException
	{
		readData(stepnode);
	}

	public Object clone()
	{
		SetVariablesBulkMeta retval = (SetVariablesBulkMeta)super.clone();
		retval.variableValueField  = variableValueField;
		retval.variableNameField = variableNameField;
	        retval.variableType = variableType;
		return retval;
	}
	
	private void readData(Node stepnode)
		throws KettleXMLException
	{
		try
		{
			variableNameField = XMLHandler.getTagValue(stepnode,"variable_name_field");
			variableValueField = XMLHandler.getTagValue(stepnode,"variable_value_field");
			variableType = getVariableType(XMLHandler.getTagValue(stepnode,"variable_type"));
			usingFormatting  = "Y".equalsIgnoreCase(XMLHandler.getTagValue(stepnode, "use_formatting")); //$NON-NLS-1$
		}
		catch(Exception e)
		{
			throw new KettleXMLException(BaseMessages.getString(PKG, "SetVariablesBulkMeta.RuntimeError.UnableToReadXML.SETVARIABLE0004"), e); //$NON-NLS-1$
		}
	}
	
	public void setDefault()
	{
		variableNameField = "name";
		variableValueField = "value";
		variableType = VARIABLE_TYPE_JVM;
		usingFormatting = true;
	}

	public String getXML()
	{
	        StringBuffer retval = new StringBuffer(150);
		retval.append("    ").append(XMLHandler.addTagValue("variable_name_field", variableNameField));
		retval.append("    ").append(XMLHandler.addTagValue("variable_value_field", variableValueField));
		retval.append("    ").append(XMLHandler.addTagValue("variable_type", getVariableTypeCode(variableType)));
		retval.append("    ").append(XMLHandler.addTagValue("use_formatting", usingFormatting));
		return retval.toString();
	}
	
	public void readRep(Repository rep, ObjectId id_step, List<DatabaseMeta> databases, Map<String, Counter> counters)
		throws KettleException
	{
		try
		{
			variableNameField = rep.getStepAttributeString(id_step,"variable_name_field"); //$NON-NLS-1$
			variableValueField = rep.getStepAttributeString(id_step,"variable_value_field"); //$NON-NLS-1$
                	variableType = getVariableType(rep.getStepAttributeString(id_step, 0, "variable_type")); //$NON-NLS-1$
			usingFormatting = rep.getStepAttributeBoolean(id_step, 0, "use_formatting", false); //$NON-NLS-1$
		}
		catch(Exception e)
		{
			throw new KettleException(BaseMessages.getString(PKG, "SetVariablesBulkMeta.RuntimeError.UnableToReadRepository.SETVARIABLE0005"), e); //$NON-NLS-1$
		}
	}


    public void saveRep(Repository rep, ObjectId id_transformation, ObjectId id_step)
		throws KettleException
	{
		try
		{
                	rep.saveStepAttribute(id_transformation, id_step, 0, "variable_name_field",   variableNameField); //$NON-NLS-1$
                	rep.saveStepAttribute(id_transformation, id_step, 0, "variable_value_field",   variableValueField); //$NON-NLS-1$
                	rep.saveStepAttribute(id_transformation, id_step, 0, "variable_type",   getVariableTypeCode(variableType)); //$NON-NLS-1$
            		rep.saveStepAttribute(id_transformation, id_step, 0, "use_formatting", usingFormatting);
		}
		catch(Exception e)
		{
			throw new KettleException(BaseMessages.getString(PKG, "SetVariablesBulkMeta.RuntimeError.UnableToSaveRepository.SETVARIABLE0006", ""+id_step), e); //$NON-NLS-1$
		}

	}

    public void check(List<CheckResultInterface> remarks, TransMeta transMeta, StepMeta stepMeta, RowMetaInterface prev, String input[], String output[], RowMetaInterface info)
	{
		CheckResult cr;
		if (prev==null || prev.size()==0)
		{
			cr = new CheckResult(CheckResultInterface.TYPE_RESULT_WARNING, BaseMessages.getString(PKG, "SetVariablesBulkMeta.CheckResult.NotReceivingFieldsFromPreviousSteps"), stepMeta); //$NON-NLS-1$
			remarks.add(cr);
		}
		else
		{
			cr = new CheckResult(CheckResultInterface.TYPE_RESULT_OK, BaseMessages.getString(PKG, "SetVariablesBulkMeta.CheckResult.ReceivingFieldsFromPreviousSteps", ""+prev.size()), stepMeta); //$NON-NLS-1$ //$NON-NLS-2$
			remarks.add(cr);
		}
		
		// See if we have input streams leading to this step!
		if (input.length>0)
		{
			cr = new CheckResult(CheckResultInterface.TYPE_RESULT_OK, BaseMessages.getString(PKG, "SetVariablesBulkMeta.CheckResult.ReceivingInfoFromOtherSteps"), stepMeta); //$NON-NLS-1$
			remarks.add(cr);
		}
		else
		{
			cr = new CheckResult(CheckResultInterface.TYPE_RESULT_ERROR, BaseMessages.getString(PKG, "SetVariablesBulkMeta.CheckResult.NotReceivingInfoFromOtherSteps"), stepMeta); //$NON-NLS-1$
			remarks.add(cr);
		}
	}

	public StepInterface getStep(StepMeta stepMeta, StepDataInterface stepDataInterface, int cnr, TransMeta transMeta, Trans trans)
	{
		return new SetVariablesBulk(stepMeta, stepDataInterface, cnr, transMeta, trans);
	}

	public StepDataInterface getStepData()
	{
		return new SetVariablesBulkData();
	}

	/**
	 * @return the usingFormatting
	 */
	public boolean isUsingFormatting() {
		return usingFormatting;
	}

	/**
	 * @param usingFormatting the usingFormatting to set
	 */
	public void setUsingFormatting(boolean usingFormatting) {
		this.usingFormatting = usingFormatting;
	}
}
