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

import org.pentaho.di.core.Const;
import org.pentaho.di.core.exception.KettleException;
import org.pentaho.di.core.exception.KettleStepException;
import org.pentaho.di.core.row.ValueMetaInterface;
import org.pentaho.di.core.variables.VariableSpace;
import org.pentaho.di.i18n.BaseMessages;
import org.pentaho.di.job.Job;
import org.pentaho.di.trans.Trans;
import org.pentaho.di.trans.TransMeta;
import org.pentaho.di.trans.step.BaseStep;
import org.pentaho.di.trans.step.StepDataInterface;
import org.pentaho.di.trans.step.StepInterface;
import org.pentaho.di.trans.step.StepMeta;
import org.pentaho.di.trans.step.StepMetaInterface;


/**
 * Convert Values in a certain fields to other values
 * 
 * @author Matt 
 * @since 27-apr-2006
 */
public class SetVariablesBulk extends BaseStep implements StepInterface
{
	private static Class<?> PKG = SetVariablesBulkMeta.class; // for i18n purposes, needed by Translator2!!   $NON-NLS-1$

	private SetVariablesBulkMeta meta;
	private SetVariablesBulkData data;
	
	public SetVariablesBulk(StepMeta stepMeta, StepDataInterface stepDataInterface, int copyNr, TransMeta transMeta, Trans trans)
	{
		super(stepMeta, stepDataInterface, copyNr, transMeta, trans);
	}
	
	public boolean processRow(StepMetaInterface smi, StepDataInterface sdi) throws KettleException
	{
		meta=(SetVariablesBulkMeta)smi;
		data=(SetVariablesBulkData)sdi;
		
		// Get one row from one of the rowsets...
        	//
		Object[] rowData = getRow();
		if (rowData==null)  // means: no more input to be expected...
		{
			/*if (first)
			{
				// We do not received any row !!
				logBasic(BaseMessages.getString(PKG, "SetVariablesBulk.Log.NoInputRowSetDefault"));
				for (int i=0;i<meta.getFieldName().length;i++)
			        {
					if(!Const.isEmpty(meta.getDefaultValue()[i])) setValue(rowData,i,true); 
		        	}
			}*/
		
            		logBasic("Finished after "+getLinesWritten()+" rows.");
			setOutputDone();
			return false;
		}		
		if (first)
		{
		    first=false;		    
		    data.outputMeta = getInputRowMeta().clone();            
	            logBasic(BaseMessages.getString(PKG, "SetVariablesBulk.Log.SettingVar"));
         	}
	        setValue(rowData);  
	        putRow(data.outputMeta, rowData);
	        return true;		
	}
	
	private void setValue(Object[] rowData) throws KettleException
	{
    		// Set the appropriate environment variable
	    	//
		String value = null;
		int vlIndex = data.outputMeta.indexOfValue(meta.getVariableValueField());
	 	if (vlIndex<0)
	    	{
	    		throw new KettleException("Unable to find field ["+meta.getVariableValueField()+"] in input row");
	    	}
	    	ValueMetaInterface valueMeta = data.outputMeta.getValueMeta(vlIndex);
	    	Object valueData = rowData[vlIndex];
	   	
		// Get variable value
	    	//
	    	if (meta.isUsingFormatting()) {
	    		value=valueMeta.getString(valueData);
	    	} else {
	    		value=valueMeta.getCompatibleString(valueData);
	    	}       
		if (value==null) value="";
		
    		// Get variable name
	        String varname = null;
	        int nameIndex = data.outputMeta.indexOfValue(meta.getVariableNameField());
	 	if (nameIndex<0)
	    	{
    			throw new KettleException("Unable to find field ["+meta.getVariableNameField()+"] in input row");
    		}
	    	ValueMetaInterface nameValueMeta = data.outputMeta.getValueMeta(nameIndex);
	    	Object nameValueData = rowData[nameIndex];
   	
		// Get variable value
	    	//
	    	if (meta.isUsingFormatting()) {
	    		varname=nameValueMeta.getString(nameValueData);
	    	} else {
	    		varname=nameValueMeta.getCompatibleString(nameValueData);
	    	}       
	
        	if (Const.isEmpty(varname))
	        {
	            if (Const.isEmpty(value))
	            {
	                throw new KettleException("Variable value Field was not specifiedi");
	            }
	       	    else
        	    {
	                throw new KettleException("There was no variable name field specified.");
        	    }
	        }
        
        	Job parentJob = null;
        
	        // We always set the variable in this step and in the parent transformation...
        	//
        	setVariable(varname, value);

	        // Set variable in the transformation
	        //
	        Trans trans = getTrans();
	        trans.setVariable(varname, value);

	        // Make a link between the transformation and the parent transformation (in a sub-transformation)
	        //
	        while (trans.getParentTrans()!=null) {
	        	trans = trans.getParentTrans();
        		trans.setVariable(varname, value);
		}
	        
	        // The trans object we have now is the trans being executed by a job.
	        // It has one or more parent jobs.
	        // Below we see where we need to this value as well...  
	        //
	        switch(meta.getVariableType())
	        {
        		case SetVariablesBulkMeta.VARIABLE_TYPE_JVM: 
		            System.setProperty(varname, value);
		            parentJob = trans.getParentJob();
		            while (parentJob!=null)
		            {                           
		                parentJob.setVariable(varname, value);
		                parentJob = parentJob.getParentJob();
		            }
	            
		            break;
		        case SetVariablesBulkMeta.VARIABLE_TYPE_ROOT_JOB:
		       	{
		                // Comments by SB
		                // VariableSpace rootJob = null;
		                parentJob = trans.getParentJob();
		                while (parentJob!=null)
		                {                           
		                    parentJob.setVariable(varname, value);
		                    //rootJob = parentJob;
		                    parentJob = parentJob.getParentJob();
		                }
		                // OK, we have the rootjob, set the variable on it...
		                //if (rootJob==null)
		                //{
		                //   throw new KettleStepException("Can't set variable ["+varname+"] on root job: the root job is not available (meaning: not even the parent job)");
		                //}
		                //  Comment: why throw an exception on this?
			       }
	        		break;
		        case SetVariablesBulkMeta.VARIABLE_TYPE_GRAND_PARENT_JOB:
			{
		                // Set the variable in the parent job 
		                //
		                parentJob = trans.getParentJob();
		                if (parentJob!=null)
	        	        {
		                	parentJob.setVariable(varname, value);
		                }
		                else
	        	        {
		                	throw new KettleStepException("Can't set variable ["+varname+"] on parent job: the parent job is not available");
                		}
	
		                // Set the variable on the grand-parent job
		                //
		                VariableSpace gpJob = trans.getParentJob().getParentJob();
		                if (gpJob!=null)
        		        {
		                    gpJob.setVariable(varname, value);
                		}
		                else
                		{
		                    throw new KettleStepException("Can't set variable ["+varname+"] on grand parent job: the grand parent job is not available");
                		}
                
		        }
		        break;
			case SetVariablesBulkMeta.VARIABLE_TYPE_PARENT_JOB:
		        {                        
		                // Set the variable in the parent job 
	        	        //
		                parentJob = trans.getParentJob();
	                	if (parentJob!=null)
		                {
		                	parentJob.setVariable(varname, value);
		                }
		                else
		                {
		                	throw new KettleStepException("Can't set variable ["+varname+"] on parent job: the parent job is not available");
		                }
		         }
	    }               
            logBasic(BaseMessages.getString(PKG, "SetVariablesBulk.Log.SetVariablesBulkToValue",varname,value));
	}
	
	public void dispose(StepMetaInterface smi, StepDataInterface sdi)
	{
		meta=(SetVariablesBulkMeta)smi;
		data=(SetVariablesBulkData)sdi;

		super.dispose(smi, sdi);
	}
	
	public boolean init(StepMetaInterface smi, StepDataInterface sdi)
	{
		meta=(SetVariablesBulkMeta)smi;
		data=(SetVariablesBulkData)sdi;
		
		if (super.init(smi, sdi))
		{
            return true;
		}
		return false;
	}

}
