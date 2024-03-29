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

 
/*
 * Created on 5-aug-2003
 *
 */

package org.pentaho.di.ui.trans.steps.setvariablesbulk;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;

import org.eclipse.jface.dialogs.MessageDialog;
import org.eclipse.jface.dialogs.MessageDialogWithToggle;
import org.eclipse.swt.SWT;
import org.eclipse.swt.events.ModifyEvent;
import org.eclipse.swt.events.ModifyListener;
import org.eclipse.swt.events.SelectionAdapter;
import org.eclipse.swt.events.SelectionEvent;
import org.eclipse.swt.events.ShellAdapter;
import org.eclipse.swt.events.ShellEvent;
import org.eclipse.swt.layout.FormAttachment;
import org.eclipse.swt.layout.FormData;
import org.eclipse.swt.layout.FormLayout;
import org.eclipse.swt.widgets.Button;
import org.eclipse.swt.widgets.Display;
import org.eclipse.swt.widgets.Event;
import org.eclipse.swt.widgets.Label;
import org.eclipse.swt.widgets.Listener;
import org.eclipse.swt.widgets.Shell;
import org.eclipse.swt.widgets.TableItem;
import org.eclipse.swt.widgets.Text;
import org.eclipse.swt.custom.CCombo;
import org.pentaho.di.core.Const;
import org.pentaho.di.core.exception.KettleException;
import org.pentaho.di.core.row.RowMetaInterface;
import org.pentaho.di.core.row.ValueMetaInterface;
import org.pentaho.di.i18n.BaseMessages;
import org.pentaho.di.trans.TransMeta;
import org.pentaho.di.trans.step.BaseStepMeta;
import org.pentaho.di.trans.step.StepDialogInterface;
import org.pentaho.di.trans.step.StepMeta;
import org.pentaho.di.trans.steps.setvariablesbulk.SetVariablesBulkMeta;
import org.pentaho.di.ui.core.dialog.ErrorDialog;
import org.pentaho.di.ui.core.gui.GUIResource;
import org.pentaho.di.ui.core.widget.ColumnInfo;
import org.pentaho.di.ui.core.widget.TableView;
import org.pentaho.di.ui.trans.step.BaseStepDialog;
import org.pentaho.di.ui.trans.step.TableItemInsertListener;


public class SetVariablesBulkDialog extends BaseStepDialog implements StepDialogInterface
{
	private static Class<?> PKG = SetVariablesBulkMeta.class; // for i18n purposes, needed by Translator2!!   $NON-NLS-1$

	public static final String STRING_USAGE_WARNING_PARAMETER = "SetVariablesBulkUsageWarning"; //$NON-NLS-1$
    
	private Label        wlStepname;
	private Text         wStepname;
    	private FormData     fdlStepname, fdStepname;

	private Label        wlFormat;
	private Button       wFormat;
	private FormData     fdlFormat, fdFormat;

	private Label        wlVariableType;
	private CCombo       wVariableType;
	private FormData     fdlVariableType, fdVariableType;
	
	private Label        wlVariableNameField;
	private CCombo       wVariableNameField;
	private FormData     fdlVariableNameField, fdVariableNameField;

	private Label        wlVariableValueField;
	private CCombo       wVariableValueField;
	private FormData     fdlVariableValueField, fdVariableValueField;
	private SetVariablesBulkMeta input;

  	private Map<String, Integer> inputFields;
	private String[] fieldComboItems;
    
	
	public SetVariablesBulkDialog(Shell parent, Object in, TransMeta transMeta, String sname)
	{
		super(parent, (BaseStepMeta)in, transMeta, sname);
		input=(SetVariablesBulkMeta)in;
    	        inputFields =new HashMap<String, Integer>();
		fieldComboItems = new String[0];
	}

	public String open()
	{
		Shell parent = getParent();
		Display display = parent.getDisplay();

		shell = new Shell(parent, SWT.DIALOG_TRIM | SWT.RESIZE | SWT.MAX | SWT.MIN);
 		props.setLook(shell);
	        setShellImage(shell, input);

		ModifyListener lsMod = new ModifyListener() 
		{
			public void modifyText(ModifyEvent e) 
			{
				input.setChanged();
			}
		};
		changed = input.hasChanged();

		FormLayout formLayout = new FormLayout ();
		formLayout.marginWidth  = Const.FORM_MARGIN;
		formLayout.marginHeight = Const.FORM_MARGIN;

		shell.setLayout(formLayout);
		shell.setText(BaseMessages.getString(PKG, "SetVariablesBulkDialog.DialogTitle")); 
		
		int middle = props.getMiddlePct();
		int margin = Const.MARGIN;

		// Stepname line
		wlStepname=new Label(shell, SWT.RIGHT);
		wlStepname.setText(BaseMessages.getString(PKG, "SetVariablesBulkDialog.Stepname.Label"));
 		props.setLook(wlStepname);
		fdlStepname=new FormData();
		fdlStepname.left = new FormAttachment(0, 0);
		fdlStepname.right= new FormAttachment(middle, -margin);
		fdlStepname.top  = new FormAttachment(0, margin);
		wlStepname.setLayoutData(fdlStepname);
		wStepname=new Text(shell, SWT.SINGLE | SWT.LEFT | SWT.BORDER);
 		props.setLook(wStepname);
		wStepname.addModifyListener(lsMod);
		fdStepname=new FormData();
		fdStepname.left = new FormAttachment(middle, 0);
		fdStepname.top  = new FormAttachment(0, margin);
		fdStepname.right= new FormAttachment(100, 0);
		wStepname.setLayoutData(fdStepname);

		wlFormat=new Label(shell, SWT.RIGHT);
		wlFormat.setText(BaseMessages.getString(PKG, "SetVariablesBulkDialog.Format.Label"));
		wlFormat.setToolTipText(BaseMessages.getString(PKG, "SetVariablesBulkDialog.Format.Tooltip")); //$NON-NLS-1$
 		props.setLook(wlFormat);
		fdlFormat=new FormData();
		fdlFormat.left = new FormAttachment(0, 0);
		fdlFormat.right= new FormAttachment(middle, -margin);
		fdlFormat.top  = new FormAttachment(wStepname, margin);
		wlFormat.setLayoutData(fdlFormat);
		wFormat=new Button(shell, SWT.CHECK);
		wFormat.setToolTipText(BaseMessages.getString(PKG, "SetVariablesBulkDialog.Format.Tooltip")); //$NON-NLS-1$
 		props.setLook(wFormat);
		fdFormat=new FormData();
		fdFormat.left = new FormAttachment(middle, 0);
		fdFormat.top  = new FormAttachment(wStepname, margin);
		wFormat.setLayoutData(fdFormat);

		wlVariableType = new Label(shell, SWT.RIGHT);
		wlVariableType.setText(BaseMessages.getString(PKG, "SetVariablesBulkDialog.VariableType.Label"));
		props.setLook(wlVariableType);
		fdlVariableType = new FormData();
		fdlVariableType.left = new FormAttachment(0,0);
		fdlVariableType.right = new FormAttachment(middle,-margin);
		fdlVariableType.top = new FormAttachment(wFormat,margin);
		wlVariableType.setLayoutData(fdlVariableType);
		wVariableType = new CCombo(shell,SWT.SINGLE | SWT.READ_ONLY | SWT.BORDER);
		wVariableType.setToolTipText(BaseMessages.getString(PKG,"SetVariablesBulkDialog.VariableType.Tooltip"));
		wVariableType.setItems(SetVariablesBulkMeta.getVariableTypeDescriptions());
		props.setLook(wVariableType);
		fdVariableType=new FormData();
		fdVariableType.left = new FormAttachment(middle,0);
		fdVariableType.top = new FormAttachment(wFormat,margin);
		fdVariableType.right = new FormAttachment(100,0);
		wVariableType.setLayoutData(fdVariableType);

		wlVariableNameField = new Label(shell, SWT.RIGHT);
		wlVariableNameField.setText(BaseMessages.getString(PKG, "SetVariablesBulkDialog.VariableNameField.Label"));
		props.setLook(wlVariableNameField);
		fdlVariableNameField = new FormData();
		fdlVariableNameField.left = new FormAttachment(0,0);
		fdlVariableNameField.right = new FormAttachment(middle,-margin);
		fdlVariableNameField.top = new FormAttachment(wVariableType,margin);
		wlVariableNameField.setLayoutData(fdlVariableNameField);
		wVariableNameField = new CCombo(shell,SWT.BORDER | SWT.READ_ONLY);
		wVariableNameField.setToolTipText(BaseMessages.getString(PKG,"SetVariablesBulkDialog.VariableNameField.Tooltip"));
		wVariableNameField.setEditable(true);
		wVariableNameField.addModifyListener(lsMod);
		props.setLook(wVariableNameField);
		wVariableNameField.setItems(fieldComboItems);
		fdVariableNameField=new FormData();
		fdVariableNameField.left = new FormAttachment(middle,0);
		fdVariableNameField.top = new FormAttachment(wVariableType,margin);
		fdVariableNameField.right = new FormAttachment(100,0);
		wVariableNameField.setLayoutData(fdVariableNameField);

		wlVariableValueField = new Label(shell, SWT.RIGHT);
		wlVariableValueField.setText(BaseMessages.getString(PKG, "SetVariablesBulkDialog.VariableValueField.Label"));
		props.setLook(wlVariableValueField);
		fdlVariableValueField = new FormData();
		fdlVariableValueField.left = new FormAttachment(0,0);
		fdlVariableValueField.right = new FormAttachment(middle,-margin);
		fdlVariableValueField.top = new FormAttachment(wVariableNameField,margin);
		wlVariableValueField.setLayoutData(fdlVariableValueField);
		wVariableValueField = new CCombo(shell,SWT.BORDER | SWT.READ_ONLY);
		wVariableValueField.setToolTipText(BaseMessages.getString(PKG,"SetVariablesBulkDialog.VariableValueField.Tooltip"));
		wVariableValueField.setEditable(true);
		wVariableValueField.setItems(fieldComboItems);
		wVariableValueField.addModifyListener(lsMod);
		props.setLook(wVariableValueField);
		fdVariableValueField=new FormData();
		fdVariableValueField.left = new FormAttachment(middle,0);
		fdVariableValueField.top = new FormAttachment(wVariableNameField,margin);
		fdVariableValueField.right = new FormAttachment(100,0);
		wVariableValueField.setLayoutData(fdVariableValueField);


/*
		wlFields=new Label(shell, SWT.NONE);
		wlFields.setText(BaseMessages.getString(PKG, "SetVariablesBulkDialog.Fields.Label")); //$NON-NLS-1$
 		props.setLook(wlFields);
		fdlFields=new FormData();
		fdlFields.left = new FormAttachment(0, 0);
		fdlFields.top  = new FormAttachment(wFormat, margin);
		wlFields.setLayoutData(fdlFields);
		
		final int FieldsRows=input.getFieldName().length;
		colinf=new ColumnInfo[4];
		colinf[0]=  new ColumnInfo(BaseMessages.getString(PKG, "SetVariablesBulkDialog.Fields.Column.FieldName"), ColumnInfo.COLUMN_TYPE_CCOMBO, new String[] { "" }, false);//$NON-NLS-1$
		colinf[1]=  new ColumnInfo(BaseMessages.getString(PKG, "SetVariablesBulkDialog.Fields.Column.VariableName"), ColumnInfo.COLUMN_TYPE_TEXT, false); //$NON-NLS-1$
		colinf[2]=  new ColumnInfo(BaseMessages.getString(PKG, "SetVariablesBulkDialog.Fields.Column.VariableType"), ColumnInfo.COLUMN_TYPE_CCOMBO, SetVariablesBulkMeta.getVariableTypeDescriptions(), false); //$NON-NLS-1$
		colinf[3]=  new ColumnInfo(BaseMessages.getString(PKG, "SetVariablesBulkDialog.Fields.Column.DefaultValue"), ColumnInfo.COLUMN_TYPE_TEXT, false);
		colinf[3].setUsingVariables(true);
		colinf[3].setToolTip(BaseMessages.getString(PKG, "SetVariablesBulkDialog.Fields.Column.DefaultValue.Tooltip"));
		
		wFields=new TableView(transMeta, shell, 
							  SWT.BORDER | SWT.FULL_SELECTION | SWT.MULTI, 
							  colinf, 
							  FieldsRows,  
							  lsMod,
							  props
							  );

		fdFields=new FormData();
		fdFields.left  = new FormAttachment(0, 0);
		fdFields.top   = new FormAttachment(wlFields, margin);
		fdFields.right = new FormAttachment(100, 0);
		fdFields.bottom= new FormAttachment(100, -50);
		wFields.setLayoutData(fdFields);
*/
		
		  // 
		Runnable runnable = new Runnable()
		{
			public void run()
			{
				try
				{
					RowMetaInterface inputfields = transMeta.getPrevStepFields(stepname);
                			if (inputfields!=null)
			                {
    						for (int i=0;i<inputfields.size();i++)
	    					{
	    						wVariableNameField.add(inputfields.getValueMeta(i).getName() );
	    						wVariableValueField.add(inputfields.getValueMeta(i).getName() );
	    					}
         			        }
				}
				catch(Exception ke)
				{
					new ErrorDialog(shell, BaseMessages.getString(PKG, "SetVariablesBulkDialog.Dialog.FailedToGetFields.DialogTitle"), BaseMessages.getString(PKG, "SetVariablesBulkDialog.FailedToGetFields.DialogMessage"), ke); //$NON-NLS-1$ //$NON-NLS-2$
				}
			}
		};
		display.asyncExec(runnable);
/*	
		final Runnable runnable = new Runnable()
		{
		    public void run()
		    {
			StepMeta stepMeta = transMeta.findStep(stepname);
			if (stepMeta!=null)
			{
			    try
			    {
				RowMetaInterface row = transMeta.getPrevStepFields(stepMeta);
			       
				// Remember these fields...
				for (int i=0;i<row.size();i++)
				{
				    inputFields.put(row.getValueMeta(i).getName(), Integer.valueOf(i));
				}
				setComboBoxes();
			    }
			    catch(KettleException e)
			    {
				logError(BaseMessages.getString(PKG, "System.Dialog.GetFieldsFailed.Message"));
			    }
			}
		    }
		};
		new Thread(runnable).start();
			*/		
			// Some buttons
		wOK=new Button(shell, SWT.PUSH);
		wOK.setText(BaseMessages.getString(PKG, "System.Button.OK")); //$NON-NLS-1$
		wCancel=new Button(shell, SWT.PUSH);
		wCancel.setText(BaseMessages.getString(PKG, "System.Button.Cancel")); //$NON-NLS-1$

		setButtonPositions(new Button[] { wOK, wCancel }, margin,wVariableValueField);

			// Add listeners
		lsCancel = new Listener() { public void handleEvent(Event e) { cancel(); } };
		lsOK     = new Listener() { public void handleEvent(Event e) { ok();       } };
		wCancel.addListener(SWT.Selection, lsCancel);
		wOK.addListener    (SWT.Selection, lsOK    );
		lsDef=new SelectionAdapter() { public void widgetDefaultSelected(SelectionEvent e) { ok(); } };
		wStepname.addSelectionListener( lsDef );
		// Detect X or ALT-F4 or something that kills this window...
		shell.addShellListener(	new ShellAdapter() { public void shellClosed(ShellEvent e) { cancel(); } } );
		// Set the shell size, based upon previous time...
		setSize();
		
		getData();
		input.setChanged(changed);
	
		shell.open();
		while (!shell.isDisposed())
		{
			if (!display.readAndDispatch()) display.sleep();
		}
		return stepname;
	}
	protected void setComboBoxes()
	{
       		 // Something was changed in the row.
	        //
	        final Map<String, Integer> fields = new HashMap<String, Integer>();
	        
	        // Add the currentMeta fields...
	        fields.putAll(inputFields);
	        
        	Set<String> keySet = fields.keySet();
	        List<String> entries = new ArrayList<String>(keySet);
		
	        String fieldNames[] = (String[]) entries.toArray(new String[entries.size()]);
	
	        Const.sortStrings(fieldNames);
		fieldComboItems = fieldNames;
	}
	/**
	 * Copy information from the meta-data input to the dialog fields.
	 */ 
	public void getData()
	{
		wStepname.setText(stepname);
		String nameField = input.getVariableNameField();
		String valueField = input.getVariableValueField();
		String varType = SetVariablesBulkMeta.getVariableTypeDescription(input.getVariableType());
		if (nameField!=null) wVariableNameField.setText(nameField);
		if (valueField!=null) wVariableValueField.setText(valueField);
		if (varType!=null) wVariableType.setText(varType);
        	wFormat.setSelection(input.isUsingFormatting());
		wStepname.selectAll();
	}
	
	private void cancel()
	{
		stepname=null;
		input.setChanged(changed);
		dispose();
	}
	
	private void ok()
	{
		if (Const.isEmpty(wStepname.getText())) return;

		stepname = wStepname.getText(); // return value

		input.setVariableType(SetVariablesBulkMeta.getVariableType(wVariableType.getText()));
		input.setVariableNameField(wVariableNameField.getText());
		input.setVariableValueField(wVariableValueField.getText());
		input.setUsingFormatting(wFormat.getSelection());
		
	        // Show a warning (optional)
	        //
	        if ( "Y".equalsIgnoreCase( props.getCustomParameter(STRING_USAGE_WARNING_PARAMETER, "Y") )) //$NON-NLS-1$ //$NON-NLS-2$
	        {
	            MessageDialogWithToggle md = new MessageDialogWithToggle(shell, 
						BaseMessages.getString(PKG, "SetVariablesBulkDialog.UsageWarning.DialogTitle"),  //$NON-NLS-1$
				                null,
						BaseMessages.getString(PKG, "SetVariablesBulkDialog.UsageWarning.DialogMessage", Const.CR )+Const.CR, //$NON-NLS-1$ //$NON-NLS-2$
						MessageDialog.WARNING,
				                new String[] { BaseMessages.getString(PKG, "SetVariablesBulkDialog.UsageWarning.Option1") }, //$NON-NLS-1$
				                0,
				                BaseMessages.getString(PKG, "SetVariablesBulkDialog.UsageWarning.Option2"), //$NON-NLS-1$
				                "N".equalsIgnoreCase( props.getCustomParameter(STRING_USAGE_WARNING_PARAMETER, "Y") ) //$NON-NLS-1$ //$NON-NLS-2$
	            );
	            MessageDialogWithToggle.setDefaultImage(GUIResource.getInstance().getImageSpoon());
        	    md.open();
	            props.setCustomParameter(STRING_USAGE_WARNING_PARAMETER, md.getToggleState()?"N":"Y"); //$NON-NLS-1$ //$NON-NLS-2$
	            props.saveProps();
        	}
		dispose();
	}
    
}
